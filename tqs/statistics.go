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

// QueueStatistics needs a comment TODO
type QueueStatistics struct {
	Sends          uint64
	Receives       uint64
	Deletes        uint64
	LeaseExpires   uint64
	MessageExpires uint64
}

// GetQueueStatistics needs a comment TODO
func (s *Store) GetQueueStatistics(name string) (QueueStatistics, error) {
	return QueueStatistics{}, nil
}
