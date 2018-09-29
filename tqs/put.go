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
	"github.com/boltdb/bolt"
	"github.com/vmihailenco/msgpack"
)

// PutMessages should have a comment TODO
func (s *Store) PutMessages(queueName string, messages []Message) ([]MessageID, error) {
	var ids []MessageID
	return ids, s.db.Update(func(tx *bolt.Tx) error {
		bucket := s.visible(tx, queueName)
		if bucket == nil {
			return ErrQueueNotFound
		}

		for i := range messages {
			// Why not introduce MessageSetting just like QueueSetting
			if messages[i].Settings.Priority == 0 {
				messages[i].Settings.Priority = DefaultPriority
			}

			if messages[i].Settings.DelaySeconds == 0 {
				messages[i].Settings.DelaySeconds = DefaultDelaySeconds
			}

			if messages[i].Settings.LeaseDuration == 0 {
				messages[i].Settings.LeaseDuration = DefaultLeaseDuration
			}

			if messages[i].Settings.MessageRetentionPeriod == 0 {
				messages[i].Settings.MessageRetentionPeriod = DefaultMessageRetentionPeriod
			}

			value, err := msgpack.Marshal(&messages[i])
			if err != nil {
				return err
			}

			key := generateMessageID(uint8(messages[i].Settings.Priority))
			ids = append(ids, key)
			if err := bucket.Put(key[:], value); err != nil {
				return nil
			}
		}
		return nil
	})
}
