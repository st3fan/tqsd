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

const (
	maxNumberOfMessages = 32
)

type sendMessagesRequest struct {
	Messages []tqs.Message
}

type sendMessagesResponse struct {
	MessageIDs []tqs.MessageID
}

func (s *Server) sendMessages(w http.ResponseWriter, r *http.Request) {
	var request sendMessagesRequest
	if err := unmarshalBody(r, &request, 32*tqs.MaxBodyLength); err != nil { // TODO Magic numbers
		internalServerError(w, err)
		return
	}

	for i := range request.Messages {
		if request.Messages[i].Settings.Priority == 0 {
			request.Messages[i].Settings.Priority = tqs.DefaultPriority
		}
	}

	vars := mux.Vars(r)
	messageIDs, err := s.store.PutMessages(vars["name"], request.Messages)
	if err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
	}

	response := sendMessagesResponse{
		MessageIDs: messageIDs,
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}
