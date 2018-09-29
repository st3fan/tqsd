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
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/st3fan/tqsd/tqs"
)

type receiveMessagesResponse struct {
	Messages []tqs.Message
	Leases   []tqs.Lease
}

func (s *Server) receiveMessages(w http.ResponseWriter, r *http.Request) {
	maxNumberOfMessages, err := getMaxNumberOfMessages(r)
	if err != nil {
		badRequestError(w, nil, "Invalid MaxNumberOfMessages: "+err.Error())
		return
	}

	leaseDuration, err := getLeaseDuration(r)
	if err != nil {
		badRequestError(w, nil, "Invalid LeaseDuration: "+err.Error())
		return
	}

	vars := mux.Vars(r)
	messages, leases, err := s.store.GetMessages(vars["name"], maxNumberOfMessages, leaseDuration)
	if err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
	}

	response := receiveMessagesResponse{
		Messages: messages,
		Leases:   leases,
	}

	// TODO This pattern is repeated in many handlers - could be a one liner?

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}
