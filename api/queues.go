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

//

type getQueuesResponse []QueueDetails

func (s *Server) getQueues(w http.ResponseWriter, r *http.Request) {
	queueNames, err := s.store.GetQueueNames()
	if err != nil {
		internalServerError(w, err)
		return
	}

	response := make(getQueuesResponse, 0)

	for _, queueName := range queueNames {
		queueSettings, err := s.store.GetQueueSettings(queueName)
		if err != nil {
			internalServerError(w, err)
			return
		}

		queueMeta, err := s.store.GetQueueMeta(queueName)
		if err != nil {
			internalServerError(w, err)
		}

		queue := QueueDetails{
			Name:                   queueMeta.Name,
			Created:                queueMeta.Created,
			LeaseDuration:          queueSettings.LeaseDuration,
			MessageRetentionPeriod: queueSettings.MessageRetentionPeriod,
			DelaySeconds:           queueSettings.DelaySeconds,
		}

		response = append(response, queue)
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}

//

type badRequestResponse struct {
	Error   string
	Message string
}

func badRequestError(w http.ResponseWriter, err error, msg string) {
	response := badRequestResponse{
		//Error:   err.Error(),
		Message: msg,
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusBadRequest)
	w.Write(encodedResponse)
}

//

type createQueueSettings struct {
	LeaseDuration          int
	MessageRetentionPeriod int
	DelaySeconds           int
}

type createQueueRequest struct {
	Name     string
	Settings createQueueSettings
}

type createQueueResponse struct {
	Meta     tqs.QueueMeta
	Settings tqs.QueueSettings
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request) {
	var request createQueueRequest
	if err := unmarshalBody(r, &request, 1024); err != nil {
		internalServerError(w, err)
	}

	queueSettings := make([]tqs.QueueSetting, 0)

	if request.Settings.LeaseDuration != 0 {
		queueSettings = append(queueSettings, tqs.LeaseDuration(request.Settings.LeaseDuration))
	}

	if request.Settings.MessageRetentionPeriod != 0 {
		queueSettings = append(queueSettings, tqs.MessageRetentionPeriod(request.Settings.MessageRetentionPeriod))
	}

	if request.Settings.DelaySeconds != 0 {
		queueSettings = append(queueSettings, tqs.DelaySeconds(request.Settings.DelaySeconds))
	}

	meta, settings, err := s.store.CreateQueue(request.Name, queueSettings...)
	if err != nil {
		// TODO Error handling sucks .. maybe CreateQueue can return a more detailed error
		if err == tqs.ErrInvalidQueueName {
			badRequestError(w, nil, "invalid queue name")
		} else if err == tqs.ErrInvalidLeaseDuration || err == tqs.ErrInvalidMessageRetentionPeriod || err == tqs.ErrInvalidDelaySeconds {
			badRequestError(w, nil, err.Error())
		} else if err == tqs.ErrQueueExists {
			http.Error(w, http.StatusText(http.StatusConflict), http.StatusConflict)
		} else {
			internalServerError(w, err)
		}
		return
	}

	response := createQueueResponse{
		Meta:     meta,
		Settings: settings,
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/queues/"+request.Name)
	w.WriteHeader(http.StatusCreated)
	w.Write(encodedResponse)
}

//

func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	settings, err := s.store.GetQueueSettings(vars["name"])
	if err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		}
		internalServerError(w, err)
		return
	}

	response := QueueDetails{
		Name:                   vars["name"],
		LeaseDuration:          settings.LeaseDuration,
		MessageRetentionPeriod: settings.MessageRetentionPeriod,
		DelaySeconds:           settings.DelaySeconds,
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}

//

func (s *Server) getQueueMeta(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	meta, err := s.store.GetQueueMeta(vars["name"])
	if err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
		return
	}

	encodedResponse, err := json.Marshal(&meta)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}

//

func (s *Server) getQueueSettings(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	settings, err := s.store.GetQueueSettings(vars["name"])
	if err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
		return
	}

	encodedResponse, err := json.Marshal(&settings)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}

//

func (s *Server) deleteQueue(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if err := s.store.DeleteQueue(vars["name"]); err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
	}
}
