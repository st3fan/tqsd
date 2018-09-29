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
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/st3fan/tqsd/tqs"
)

type Server struct {
	router  *mux.Router
	server  *http.Server
	version string
	store   *tqs.Store
}

type QueueDetails struct {
	Name                   string
	Created                time.Time
	LeaseDuration          int
	MessageRetentionPeriod int
	DelaySeconds           int
}

func unmarshalBody(r *http.Request, v interface{}, maxSize int64) error {
	body, err := ioutil.ReadAll(io.LimitReader(r.Body, maxSize))
	if err != nil {
		return err
	}
	defer r.Body.Close()

	return json.Unmarshal(body, v)
}

func internalServerError(w http.ResponseWriter, err error) {
	// TODO Log the error to Sentry or Logrus
	log.Println("Failure: ", err)
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

type versionResponse struct {
	Version string
}

func (s *Server) getVersion(w http.ResponseWriter, r *http.Request) {
	response := versionResponse{
		Version: s.version,
	}

	encodedResponse, err := json.Marshal(&response)
	if err != nil {
		internalServerError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(encodedResponse)
}

func (s *Server) purgeMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	if err := s.store.PurgeQueue(vars["name"]); err != nil {
		if err == tqs.ErrQueueNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			internalServerError(w, err)
		}
	}
}

//

func (s *Server) NewServer(version string, store *tqs.Store, addr string) (*Server, error) {
	router := mux.NewRouter()
	router.StrictSlash(true)

	router.HandleFunc("/version", s.getVersion).Methods("GET")

	router.HandleFunc("/queues", s.getQueues).Methods("GET")
	router.HandleFunc("/queues", s.createQueue).Methods("POST")

	router.HandleFunc("/queues/{name}", s.getQueue).Methods("GET")
	router.HandleFunc("/queues/{name}", s.deleteQueue).Methods("DELETE")

	router.HandleFunc("/queues/{name}/meta", s.getQueueMeta).Methods("GET")
	router.HandleFunc("/queues/{name}/settings", s.getQueueSettings).Methods("GET")

	router.HandleFunc("/queues/{name}/messages", s.receiveMessages).Methods("GET")
	router.HandleFunc("/queues/{name}/messages", s.sendMessages).Methods("POST")
	router.HandleFunc("/queues/{name}/messages", s.purgeMessages).Methods("DELETE")

	router.HandleFunc("/queues/{name}/leases/{id}", s.deleteLease).Methods("DELETE")

	loggedRouter := DebugHandler(router)

	server := &http.Server{
		Addr:         addr,
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      router,
	}

	return &Server{
		router:  router,
		server:  server,
		version: version,
		store:   store,
	}, nil
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown() error {
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
	return s.server.Shutdown(ctx)
}

type debugHandler struct {
	handler http.Handler
}

func (h debugHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	t := time.Now()
	h.handler.ServeHTTP(w, req)
	d := time.Now().Sub(t)
	log.Printf("%s %s (%s)\n", req.Method, req.URL.Path, d)
}

func DebugHandler(h http.Handler) http.Handler {
	return debugHandler{handler: h}
}

// // Run starts the server
// func (s *Server) Run(addr string) error {
// }
