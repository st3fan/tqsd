//
// This file is part of Tiny Queue Service.
//
// Tiny Queue Service is free software: you can redistribute it and/or
// modify it under the terms of the GNU General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// Tiny Queue Service is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
//

package tqs

import (
	"time"

	"github.com/boltdb/bolt"
)

// QueueSetting needs a comment TODO
type QueueSetting func(*QueueSettings) error

// LeaseDuration needs a comment TODO
func LeaseDuration(leaseDuration int) func(*QueueSettings) error {
	return func(qs *QueueSettings) error {
		return qs.setLeaseDuration(leaseDuration)
	}
}

// MessageRetention needs a comment TODO
func MessageRetentionPeriod(messageRetentionPeriod int) func(*QueueSettings) error {
	return func(qs *QueueSettings) error {
		return qs.setMessageRetentionPeriod(messageRetentionPeriod)
	}
}

// DelaySeconds needs a comment TODO
func DelaySeconds(delaySeconds int) func(*QueueSettings) error {
	return func(qs *QueueSettings) error {
		return qs.setDelaySeconds(delaySeconds)
	}
}

func defaultQueueSettings() QueueSettings {
	return QueueSettings{
		LeaseDuration:          DefaultLeaseDuration,
		MessageRetentionPeriod: DefaultMessageRetentionPeriod,
		DelaySeconds:           DefaultDelaySeconds,
	}
}

//

// CreateQueue needs a comment TODO
func (s *Store) CreateQueue(name string, overriddenSettings ...QueueSetting) (QueueMeta, QueueSettings, error) {
	if !isValidQueueName(name) {
		return QueueMeta{}, QueueSettings{}, ErrInvalidQueueName
	}

	meta := QueueMeta{Name: name, Created: time.Now()}
	settings := defaultQueueSettings()

	for _, setting := range overriddenSettings {
		if err := setting(&settings); err != nil {
			return QueueMeta{}, QueueSettings{}, err
		}
	}

	return meta, settings, s.db.Update(func(tx *bolt.Tx) error {
		queues := tx.Bucket([]byte("Queues"))

		bucket, err := queues.CreateBucket([]byte(name))
		if err != nil {
			if err == bolt.ErrBucketExists {
				return ErrQueueExists
			}
			return err
		}

		// Meta

		metaBucket, err := bucket.CreateBucketIfNotExists([]byte("Meta"))
		if err != nil {
			return err
		}

		if err = metaBucket.Put([]byte("Name"), []byte(name)); err != nil {
			return err
		}

		if err = metaBucket.Put([]byte("Created"), encodeTime(meta.Created)); err != nil {
			return err
		}

		// Settings

		settingsBucket, err := bucket.CreateBucketIfNotExists([]byte("Settings"))
		if err != nil {
			return err
		}

		if err = settingsBucket.Put([]byte("LeaseDuration"), encodeInt(settings.LeaseDuration)); err != nil {
			return err
		}
		if err = settingsBucket.Put([]byte("MessageRetentionPeriod"), encodeInt(settings.MessageRetentionPeriod)); err != nil {
			return err
		}
		if err = settingsBucket.Put([]byte("DelaySeconds"), encodeInt(settings.DelaySeconds)); err != nil {
			return err
		}

		// Message Buckets

		messages, err := bucket.CreateBucketIfNotExists([]byte("Messages"))
		if err != nil {
			return err
		}

		if _, err := messages.CreateBucketIfNotExists([]byte("Visible")); err != nil {
			return err
		}

		if _, err := messages.CreateBucketIfNotExists([]byte("Leased")); err != nil {
			return err
		}

		if _, err := messages.CreateBucketIfNotExists([]byte("Delayed")); err != nil {
			return err
		}

		return nil
	})
}
