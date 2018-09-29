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

package api

import (
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/st3fan/tqsd/tqs"
)

func decodeLeaseID(s string) (tqs.LeaseID, error) {
	var leaseID tqs.LeaseID

	data, err := hex.DecodeString(s)
	if err != nil {
		return leaseID, fmt.Errorf("cannot decode hex lease id: %s", err)
	}

	copy(leaseID[:], data)
	return leaseID, nil
}

func (s *Server) deleteLease(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	leaseID, err := decodeLeaseID(vars["id"])
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	if err := s.store.DeleteLeasedMessage(vars["name"], leaseID); err != nil {
		if err == tqs.ErrQueueNotFound || err == tqs.ErrLeaseNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
	}
}
