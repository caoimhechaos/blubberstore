/**
 * (c) 2014, Caoimhe Chaos <caoimhechaos@protonmail.com>,
 *	     Ancient Solutions. All rights reserved.
 *
 * Redistribution and use in source  and binary forms, with or without
 * modification, are permitted  provided that the following conditions
 * are met:
 *
 * * Redistributions of  source code  must retain the  above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this  list of conditions and the  following disclaimer in
 *   the  documentation  and/or  other  materials  provided  with  the
 *   distribution.
 * * Neither  the  name  of  Ancient Solutions  nor  the  name  of its
 *   contributors may  be used to endorse or  promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS"  AND ANY EXPRESS  OR IMPLIED WARRANTIES  OF MERCHANTABILITY
 * AND FITNESS  FOR A PARTICULAR  PURPOSE ARE DISCLAIMED. IN  NO EVENT
 * SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED  TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE,  DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT  LIABILITY,  OR  TORT  (INCLUDING NEGLIGENCE  OR  OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

// Simple example program for uploading arbitrary files to blubberstore.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/caoimhechaos/blubberstore"
	"github.com/caoimhechaos/go-urlconnection"
)

var shutdown bool

func logger(errlog chan error) {
	var err error

	for !shutdown {
		err = <-errlog
		if err != nil {
			log.Print("error: ", err)
		}
	}
}

func main() {
	var client *blubberstore.BlubberStoreClient
	var status *blubberstore.BlockStatus
	var uri, cert, key, cacert, id string
	var errlog chan error = make(chan error)
	var doozer_uri, doozer_buri string
	var maxlen int = 1
	var insecure bool
	var err error

	flag.StringVar(&uri, "blockdirectory-uri", "",
		"URI of the block directory server")
	flag.StringVar(&id, "id", "",
		"ID of the block to retrieve information about")

	flag.StringVar(&cert, "cert", "", "Path to the X.509 certificate.")
	flag.StringVar(&key, "key", "", "Path to the X.509 private key.")
	flag.StringVar(&cacert, "cacert", "", "Path to the X.509 CA certificate.")
	flag.BoolVar(&insecure, "insecure", false,
		"Disable the use of client certificates (for development/debugging).")

	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"URI of the Doozer lock service.")
	flag.StringVar(&doozer_buri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"), "Boot URI of the Doozer lock service.")
	flag.Parse()

	if doozer_uri != "" {
		err = urlconnection.SetupDoozer(doozer_buri, doozer_uri)
		if err != nil {
			log.Print("Error setting up Doozer connection: ", err)
		}
	}

	if id == "" {
		log.Fatal("No block ID specified")
	}

	go logger(errlog)

	client, err = blubberstore.NewBlubberStoreClient(
		uri, cert, key, cacert, insecure, errlog)
	if err != nil {
		log.Fatal("Error creating blubber store client: ", err)
	}

	status, err = client.StatBlob([]byte(id))
	if err != nil && err != blubberstore.Err_NoHostsReached {
		log.Fatal("Error retrieving blob stats: ", err)
	}

	fmt.Printf("Blob ID:           %20s (%s)\n",
		hex.EncodeToString(status.GetBlockId()),
		string(status.GetBlockId()))
	fmt.Printf("Replication factor: %d\n\n", status.GetReplicationFactor())

	for _, srv := range status.GetServers() {
		if len(srv.GetHostPort()) > maxlen {
			maxlen = len(srv.GetHostPort())
		}
	}

	for _, srv := range status.GetServers() {
		if len(srv.Checksum) > 0 && srv.Timestamp != nil {
			var ts time.Time = time.Unix(int64(srv.GetTimestamp()), 0)
			fmt.Printf("    %"+strconv.FormatInt(int64(maxlen), 10)+
				"s: timestamp: %s, checksum %64s\n", srv.GetHostPort(),
				ts.String(), hex.EncodeToString(srv.GetChecksum()))
		} else {
			fmt.Printf("    %s (unreachable)\n", srv.GetHostPort())
		}
	}

	shutdown = true
	errlog <- nil // Poke the logger to ensure we exit quickly.
}
