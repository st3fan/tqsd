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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MarshalJSON(t *testing.T) {
	id := &LeaseID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	bytes, err := id.MarshalJSON()
	assert.Nil(t, err)
	assert.Len(t, bytes, 36)
	assert.Equal(t, "\"0102030405060708090a0b0c0d0e0f1011\"", string(bytes))
}

func Benchmark_MarshalJSON(b *testing.B) {
	id := &LeaseID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
	for n := 0; n < b.N; n++ {
		_, _ = id.MarshalJSON()
	}
}
