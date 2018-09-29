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
	"github.com/vmihailenco/msgpack"
)

// GetMessages needs a comment TODO
func (s *Store) GetMessages(name string, maxNumberOfMessages int, leaseDuration int) ([]Message, []Lease, error) {
	messages := []Message{}
	leases := []Lease{}
	return messages, leases, s.db.Update(func(tx *bolt.Tx) error {
		visible := s.visible(tx, name)
		if visible == nil {
			return ErrQueueNotFound
		}

		leased := s.leased(tx, name)
		if leased == nil {
			return ErrQueueNotFound
		}

		settings, err := s.getQueueSettings(tx, name)
		if err != nil {
			return ErrQueueNotFound
		}

		if leaseDuration != 0 {
			settings.LeaseDuration = leaseDuration
		}

		//

		cursor := visible.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			var messageID MessageID
			for i := 0; i < len(messageID); i++ {
				messageID[i] = k[i]
			}

			// Return the message to the user
			var message Message
			if err := msgpack.Unmarshal(v, &message); err != nil {
				return err
			}
			messages = append(messages, message)

			// Save the message as a LeasedMessage in the Leased bucket.

			// TODO If the only thing we care about is the expiration
			// date, should we include it in the key instead? That
			// would allow us to not encode/decode a LeasedMessage, we
			// can simply unpack the timestamp from key and also
			// easily sort on it.

			leasedMessage := LeasedMessage{
				Expiration: time.Now().Add(time.Duration(leaseDuration) * time.Second),
				Message:    v,
			}

			encodedLeasedMessage, err := msgpack.Marshal(leasedMessage)
			if err != nil {
				return err
			}

			leaseID := generateLeaseID(messageID)
			if err := leased.Put(leaseID[:], encodedLeasedMessage); err != nil {
				return err
			}

			// Return the lease to the user
			lease := Lease{
				ID:         leaseID,
				Expiration: leasedMessage.Expiration,
			}
			leases = append(leases, lease)

			// Delete the message from the queue
			if err := visible.Delete(k); err != nil {
				return err
			}

			// If we got enough, return
			maxNumberOfMessages--
			if maxNumberOfMessages == 0 {
				return nil
			}
		}
		return nil
	})
}
