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
	"time"
)

// TODO This file sould go, all types should move into store.go

type LeaseID [17]byte

// TODO Can LeaseID be a struct with a Marshal method?

func (id *LeaseID) MarshalJSON() ([]byte, error) {
	buffer := make([]byte, hex.EncodedLen(len(id))+2)
	buffer[0] = '"'
	n := hex.Encode(buffer[1:], []byte(id[:]))
	buffer[n+1] = '"'
	return buffer, nil
}

func generateLeaseID(messageID MessageID) LeaseID {
	now := uint64(time.Now().UnixNano())
	var buf [17]byte
	for i := 0; i < len(messageID); i++ {
		buf[i] = messageID[i]
	}
	binary.BigEndian.PutUint64(buf[9:], now)
	return LeaseID(buf)
}

type Lease struct {
	ID         LeaseID
	Expiration time.Time
}
