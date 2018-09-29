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

import "github.com/boltdb/bolt"

// PurgeQueue should have a comment TODO
func (s *Store) PurgeQueue(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := s.queue(tx, name)
		if bucket == nil {
			return ErrQueueNotFound
		}

		if err := bucket.DeleteBucket([]byte("Messages")); err != nil {
			return err
		}

		messages, err := bucket.CreateBucketIfNotExists([]byte("Messages"))
		if err != nil {
			return err
		}

		if _, err = messages.CreateBucketIfNotExists([]byte("Visible")); err != nil {
			return err
		}

		if _, err = messages.CreateBucketIfNotExists([]byte("Leased")); err != nil {
			return err
		}

		if _, err = messages.CreateBucketIfNotExists([]byte("Delayed")); err != nil {
			return err
		}

		return err
	})
}
