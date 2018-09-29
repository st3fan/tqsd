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
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
)

func (s *Store) bucket(tx *bolt.Tx, path ...string) *bolt.Bucket {
	bucket := tx.Bucket([]byte(path[0]))
	if bucket == nil {
		return nil
	}

	for _, bucketName := range path[1:] {
		bucket = bucket.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}
	}

	return bucket
}

func (s *Store) queues(tx *bolt.Tx) *bolt.Bucket {
	return s.bucket(tx, "Queues")
}

func (s *Store) queue(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name)
}

func (s *Store) meta(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Meta")
}

func (s *Store) settings(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Settings")
}

func (s *Store) messages(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Messages")
}

func (s *Store) visible(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Messages", "Visible")
}

func (s *Store) leased(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Messages", "Leased")
}

func (s *Store) delayed(tx *bolt.Tx, name string) *bolt.Bucket {
	return s.bucket(tx, "Queues", name, "Messages", "Delayed")
}

//

var queueNameRegexp = regexp.MustCompile("^[a-z0-9](?:[a-z0-9-_]*[a-z0-9]+)*$")

func isValidQueueName(name string) bool {
	return queueNameRegexp.MatchString(name)
}

//

func encodeInt(i int) []byte {
	return []byte(strconv.Itoa(i))
}

func decodeInt(v []byte) (int, error) {
	return strconv.Atoi(string(v))
}

func timeFromMessageKey(key []byte) time.Time {
	ts := binary.BigEndian.Uint64(key[1:])
	return time.Unix(0, int64(ts))
}

func encodeTime(t time.Time) []byte {
	return []byte(strconv.FormatInt(t.Unix(), 10))
}

func decodeTime(b []byte) (time.Time, error) {
	if b == nil {
		return time.Time{}, fmt.Errorf("Invalid encoded time")
	}
	sec, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(sec, 0), nil
}

func isInRange(v, min, max int) bool {
	return v >= min && v <= max
}
