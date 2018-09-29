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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

var (
	ErrQueueNotFound = errors.New("queue not found")
	ErrQueueExists   = errors.New("queue already exists")
	ErrLeaseNotFound = errors.New("lease not found")

	ErrInvalidQueueName              = errors.New("invalid queue name")
	ErrInvalidLeaseDuration          = errors.New("invalid lease duration")
	ErrInvalidMessageRetentionPeriod = errors.New("invalid message retention period")
	ErrInvalidDelaySeconds           = errors.New("invalid delay")
)

const (
	MinMaxNumberOfMessages     = 1
	MaxMaxNumberOfMessages     = 25
	DefaultMaxNumberOfMessages = 1

	MinLeaseDuration     = 5     // 5 seconds
	MaxLeaseDuration     = 43200 // 12 hours
	DefaultLeaseDuration = 30    // 30 seconds

	MinMessageRetentionPeriod     = 60      // 1 minute
	MaxMessageRetentionPeriod     = 1209600 // 14 days
	DefaultMessageRetentionPeriod = 345600  // 1 hour

	MinDelaySeconds     = 0   // Immediately
	MaxDelaySeconds     = 900 // 15 minutes
	DefaultDelaySeconds = 0   // Immediately

	MaxBodyLength = 32 * 1024

	MinPriority     = 1
	DefaultPriority = 127
	MaxPriority     = 255
)

type LeasedMessage struct {
	Expiration time.Time
	Message    []byte
}

type QueueSettings struct {
	LeaseDuration          int
	MessageRetentionPeriod int
	DelaySeconds           int
}

type MessageID [9]byte // TODO Can this become a struct with Priority/ID and a custom Marshal/Encode method?

func (id *MessageID) MarshalJSON() ([]byte, error) {
	return []byte("\"" + hex.EncodeToString([]byte(id[:])) + "\""), nil
}

func generateMessageID(priority uint8) MessageID {
	now := uint64(time.Now().UnixNano())
	var buf [9]byte
	buf[0] = priority
	binary.BigEndian.PutUint64(buf[1:], now)
	return MessageID(buf)
}

func messageIDFromLeaseID(leaseID LeaseID) MessageID {
	var messageID MessageID
	for i := 0; i < 8; i++ {
		messageID[i] = leaseID[i]
	}
	return messageID
}

// MessageSettings needs a comment TODO
type MessageSettings struct {
	Priority               int
	LeaseDuration          int
	MessageRetentionPeriod int
	DelaySeconds           int
}

// Message needs a comment TODO
type Message struct {
	Body     string
	Settings MessageSettings
}

func (qs *QueueSettings) setLeaseDuration(leaseDuration int) error {
	if !isInRange(leaseDuration, MinLeaseDuration, MaxLeaseDuration) {
		return ErrInvalidLeaseDuration
	}
	qs.LeaseDuration = leaseDuration
	return nil
}

func (qs *QueueSettings) setMessageRetentionPeriod(messageRetentionPeriod int) error {
	if !isInRange(messageRetentionPeriod, MinMessageRetentionPeriod, MaxMessageRetentionPeriod) {
		return ErrInvalidMessageRetentionPeriod
	}
	qs.MessageRetentionPeriod = messageRetentionPeriod
	return nil
}

func (qs *QueueSettings) setDelaySeconds(delaySeconds int) error {
	if !isInRange(delaySeconds, MinDelaySeconds, MaxDelaySeconds) {
		return ErrInvalidDelaySeconds
	}
	qs.DelaySeconds = delaySeconds
	return nil
}

// Store needs a comment TODO
type Store struct {
	path  string
	db    *bolt.DB
	debug bool
}

// NewStore needs a comment TODO
func NewStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err = tx.CreateBucketIfNotExists([]byte("Queues")); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	store := &Store{
		path: path,
		db:   db,
	}

	return store, nil
}

// Close should have a comment TODO
func (s *Store) Close() error {
	return s.db.Close()
}

// DeleteQueue should have a comment TODO
func (s *Store) DeleteQueue(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := s.queues(tx)
		err := bucket.DeleteBucket([]byte(name))
		if err == bolt.ErrBucketNotFound {
			return ErrQueueNotFound
		}
		return err
	})
}

// DeleteLeasedMessage needs a comment TODO
func (s *Store) DeleteLeasedMessage(queueName string, leaseID LeaseID) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		leased := s.leased(tx, queueName)
		if leased == nil {
			return fmt.Errorf("Could not get %s/Messages/Leased bucket", queueName)
		}
		if err := leased.Delete([]byte(leaseID[:])); err != nil {
			return fmt.Errorf("Could not delete lease: %s", err)
		}
		return nil
	})
}

// GetQueueNames needs a comment TODO
func (s *Store) GetQueueNames() ([]string, error) {
	var names []string
	return names, s.db.View(func(tx *bolt.Tx) error {
		return s.queues(tx).ForEach(func(key, value []byte) error {
			names = append(names, string(key))
			return nil
		})
	})
}

// QueueMeta needs a comment TODO
type QueueMeta struct {
	Name    string
	Created time.Time
}

// GetQueueMeta needs a comment TODO
func (s *Store) GetQueueMeta(name string) (QueueMeta, error) {
	var meta QueueMeta
	return meta, s.db.View(func(tx *bolt.Tx) error {
		metaBucket := s.meta(tx, name)
		if metaBucket == nil {
			return ErrQueueNotFound
		}

		created, err := decodeTime(metaBucket.Get([]byte("Created")))
		if err != nil {
			return fmt.Errorf("Unable to retrieve/decode meta data (Created)")
		}
		meta.Created = created

		meta.Name = name

		return nil
	})
}

//

func (s *Store) getQueueSettings(tx *bolt.Tx, name string) (QueueSettings, error) {
	var settings QueueSettings

	settingsBucket := s.settings(tx, name)
	if settingsBucket == nil {
		return QueueSettings{}, ErrQueueNotFound
	}

	leaseDuration, err := decodeInt(settingsBucket.Get([]byte("LeaseDuration")))
	if err != nil {
		return QueueSettings{}, fmt.Errorf("Unable to retrieve/decode setting (LeaseDuration): %s", err)
	}
	settings.LeaseDuration = leaseDuration

	messageRetentionPeriod, err := decodeInt(settingsBucket.Get([]byte("MessageRetentionPeriod")))
	if err != nil {
		return QueueSettings{}, fmt.Errorf("Unable to retrieve/decode setting (MessageRetentionPeriod): %s", err)
	}
	settings.MessageRetentionPeriod = messageRetentionPeriod

	delaySeconds, err := decodeInt(settingsBucket.Get([]byte("DelaySeconds")))
	if err != nil {
		return QueueSettings{}, fmt.Errorf("Unable to retrieve/decode setting (DelaySeconds): %s", err)
	}
	settings.DelaySeconds = delaySeconds

	return settings, nil
}

// GetQueueSettings needs a comment TODO
func (s *Store) GetQueueSettings(name string) (QueueSettings, error) {
	var settings QueueSettings
	return settings, s.db.View(func(tx *bolt.Tx) error {
		s, err := s.getQueueSettings(tx, name)
		if err != nil {
			return err
		}
		settings = s
		return nil
	})
}
