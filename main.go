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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/st3fan/daemongroup"
	"github.com/st3fan/tqsd/api"
	"github.com/st3fan/tqsd/tqs"
)

var version = "untagged"

func main() {
	log.Printf("This is tqsd (%s)\n", version)

	databasePath := flag.String("database", "/var/lib/tqs.db", "path to the database file")
	address := flag.String("address", "0.0.0.0", "address to bind to")
	port := flag.Int("port", 8080, "port to bind to")
	flag.Parse()

	store, err := tqs.NewStore(*databasePath)
	if err != nil {
		log.Println("Cannot setup store: ", err)
		return
	}
	defer store.Close()

	server, err := api.NewServer(version, store)
	if err != nil {
		log.Println("Cannot setup server: ", err)
		return
	}

	log.Printf("Starting at http://%s:%d\n", *address, *port)

	serverTask := func(ctx context.Context) {
		// Start the web server in the background
		go func() {
			if err := server.Run(fmt.Sprintf("%s:%d", *address, *port)); err != nil {
				log.Println("Failed to run server: ", err)
			}
			// If we end up here then the server exited, how do we signal that/
			// to the app so that it can shut down?
		}()

		// Wait for the application to be done
		select {
		case <-ctx.Done():
			if err := server.Shutdown(); err != nil {
				log.Println("Failed to shutdown server: ", err)
			}
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	dg := daemongroup.NewDaemonGroup(ctx)
	dg.Go(store.ExpireLeasedMessagesTask)
	dg.Go(store.ExpireMessagesTask)
	dg.Go(store.MoveDelayedMessagesTask)
	dg.Go(serverTask)

	var c = make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)

	<-c

	cancel()
}
