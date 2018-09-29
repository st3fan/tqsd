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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func temporaryDatabase() string {
	return fmt.Sprintf("%s/%d.db", os.TempDir(), time.Now().UnixNano())
}

func Test_NewStore(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()
}

func Test_CreateQueue(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)
}

func Test_QueueGetMessage(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	messages, leases, err := store.GetMessages("hello", 1, DefaultLeaseDuration)
	assert.Zero(t, len(messages))
	assert.Zero(t, len(leases))
	assert.Nil(t, err)
}

func Test_QueuePutMessages1(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	messages := []Message{
		Message{Body: "Hello, world!"},
	}

	ids, err := store.PutMessages("hello", messages)
	assert.Len(t, ids, 1)
	assert.Nil(t, err)
}

func Test_QueuePutMessages3(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	messages := []Message{
		Message{Body: "Message1"},
		Message{Body: "Message2"},
		Message{Body: "Message3"},
	}

	ids, err := store.PutMessages("hello", messages)
	assert.Len(t, ids, 3)
	assert.Nil(t, err)
}

func Test_QueueGetMessages2(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	messages := []Message{
		Message{Body: "Message1"},
		Message{Body: "Message2"},
		Message{Body: "Message3"},
	}

	ids, err := store.PutMessages("hello", messages)
	assert.Len(t, ids, 3)
	assert.NotZero(t, ids[0])
	assert.NotZero(t, ids[1])
	assert.NotZero(t, ids[2])
	assert.Nil(t, err)

	if true {
		messages, leases, err := store.GetMessages("hello", 1, DefaultLeaseDuration)
		assert.Len(t, messages, 1)
		assert.Len(t, leases, 1)
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 5, DefaultLeaseDuration)
		assert.Len(t, messages, 2)
		assert.Len(t, leases, 2)
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 1, DefaultLeaseDuration)
		assert.Len(t, messages, 0)
		assert.Len(t, leases, 0)
		assert.Nil(t, err)
	}
}

func Test_DeleteMessage(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	if true {
		messages := []Message{
			Message{Body: "Message1"},
			Message{Body: "Message2"},
			Message{Body: "Message3"},
		}

		ids, err := store.PutMessages("hello", messages)
		assert.Len(t, ids, 3)
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 3, MinLeaseDuration)
		assert.Len(t, messages, 3)
		assert.Len(t, leases, 3)
		assert.Nil(t, err)

		for _, lease := range leases {
			err := store.DeleteLeasedMessage("hello", lease.ID)
			assert.Nil(t, err)
		}
	}

	if true {
		time.Sleep(MinLeaseDuration * time.Second)
		err := store.expireLeasedMessages()
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 3, DefaultLeaseDuration)
		assert.Len(t, messages, 0)
		assert.Len(t, leases, 0)
		assert.Nil(t, err)
	}
}

func Test_LeaseExpiration(t *testing.T) {
	store, err := NewStore(temporaryDatabase())
	assert.NotNil(t, store)
	assert.Nil(t, err)
	defer store.Close()

	_, _, err = store.CreateQueue("hello")
	assert.Nil(t, err)

	messages := []Message{
		Message{Body: "Message1"},
		Message{Body: "Message2"},
		Message{Body: "Message3"},
	}

	ids, err := store.PutMessages("hello", messages)
	assert.Len(t, ids, 3)
	assert.Nil(t, err)

	if true {
		messages, leases, err := store.GetMessages("hello", 3, MinLeaseDuration)
		assert.Len(t, messages, 3)
		assert.Len(t, leases, 3)
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 3, MinLeaseDuration)
		assert.Len(t, messages, 0)
		assert.Len(t, leases, 0)
		assert.Nil(t, err)
	}

	if true {
		time.Sleep(MinLeaseDuration * time.Second)
		err := store.expireLeasedMessages()
		assert.Nil(t, err)
	}

	if true {
		messages, leases, err := store.GetMessages("hello", 3, DefaultLeaseDuration)
		assert.Len(t, messages, 3)
		assert.Len(t, leases, 3)
		assert.Nil(t, err)
	}
}
