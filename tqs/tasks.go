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
	"context"
	"log"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vmihailenco/msgpack"
)

const (
	expireLeasedMessagesInterval = 2500
	expireMessagesInterval       = 2500
	moveDelayedMessagesInterval  = 2500
)

func (s *Store) expireLeasedMessagesForQueue(tx *bolt.Tx, name string) error {
	queue := s.queue(tx, name)
	if queue == nil {
		return ErrQueueNotFound
	}

	count := 0

	visible := queue.Bucket([]byte("Messages")).Bucket([]byte("Visible"))
	leased := queue.Bucket([]byte("Messages")).Bucket([]byte("Leased"))
	cursor := leased.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		var leasedMessage LeasedMessage
		if err := msgpack.Unmarshal(v, &leasedMessage); err != nil {
			return err
		}

		if time.Now().After(leasedMessage.Expiration) {
			if err := leased.Delete(k); err != nil {
				return err
			}

			var leaseID LeaseID
			copy(leaseID[:], k)

			messageID := messageIDFromLeaseID(leaseID)

			if err := visible.Put(messageID[:], leasedMessage.Message); err != nil {
				return err
			}

			count += 1
		}
	}

	if s.debug {
		log.Printf("Expired <%d> messages from <%s/Messages/Leased>", count, name)
	}

	return nil
}

func (s *Store) expireLeasedMessages() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.queues(tx).ForEach(func(key, value []byte) error {
			return s.expireLeasedMessagesForQueue(tx, string(key))
		})
	})
}

func (s *Store) ExpireLeasedMessagesTask(ctx context.Context) {
	ticker := time.NewTicker(expireLeasedMessagesInterval * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if err := s.expireLeasedMessages(); err != nil {
				log.Println("Failed to expire leases: ", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) expireMessagesForQueue(tx *bolt.Tx, name string) error {
	queue := s.queue(tx, name)
	if queue == nil {
		return ErrQueueNotFound
	}

	settings, err := s.getQueueSettings(tx, name)
	if err != nil {
		return err
	}

	count := 0

	visible := queue.Bucket([]byte("Messages")).Bucket([]byte("Visible"))
	err = visible.ForEach(func(key, value []byte) error {
		if timeFromMessageKey(key).Add(time.Duration(settings.MessageRetentionPeriod) * time.Second).Before(time.Now()) {
			if err := visible.Delete(key); err != nil {
				return err
			}
			count++
		}
		return nil
	})

	if s.debug {
		log.Printf("Expired <%d> messages from <%s/Messages/Visible>", count, name)
	}

	return err
}

func (s *Store) expireMessages() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.queues(tx).ForEach(func(key, value []byte) error {
			return s.expireMessagesForQueue(tx, string(key))
		})
	})
}

func (s *Store) ExpireMessagesTask(ctx context.Context) {
	ticker := time.NewTicker(expireMessagesInterval * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if err := s.expireMessages(); err != nil {
				log.Println("Failed to visible messages: ", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) moveDelayedMessagesForQueue(tx *bolt.Tx, name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		// delayed := s.delayed(tx)
		// visible := s.visible(tx)

		// now := time.Now().Unix()

		// cursor := delayed.Cursor()
		// for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		//	if timeFromKey(k) >= now {
		//		// TODO Move
		//	}
		// }

		// return s.delayed(tx).ForEach(func(key, value []byte) error {
		//	for keyIsExpired() {
		//		// move
		//	}
		// })
		return nil
	})
}

func (s *Store) moveDelayedMessages() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.queues(tx).ForEach(func(key, value []byte) error {
			return s.moveDelayedMessagesForQueue(tx, string(key))
		})
	})
}

func (s *Store) MoveDelayedMessagesTask(ctx context.Context) {
	ticker := time.NewTicker(moveDelayedMessagesInterval * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if err := s.moveDelayedMessages(); err != nil {
				log.Println("Failed to move delayed messages: ", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
