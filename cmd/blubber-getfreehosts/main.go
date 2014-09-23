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

// Command line interface to query the list of free hosts.
package main

import (
	"flag"
	"log"
	"os"

	"code.google.com/p/goprotobuf/proto"
	"github.com/caoimhechaos/blubberstore"
	"github.com/caoimhechaos/go-urlconnection"
)

func main() {
	var req blubberstore.FreeHostsRequest
	var hosts *blubberstore.BlockHolderList
	var client *blubberstore.BlubberDirectoryClient

	var doozer_uri, doozer_buri string
	var cert, key, cacert string
	var num_blocks int
	var server string
	var host string
	var insecure bool
	var err error

	flag.IntVar(&num_blocks, "num-blocks", 3,
		"Number of blocks which should be looked up.")
	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"URI of the Doozer lock service.")
	flag.StringVar(&doozer_buri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"),
		"Boot URI of the Doozer lock service.")
	flag.StringVar(&server, "server", "tcp://[::]:0",
		"URL to connect and send commands to.")

	flag.StringVar(&cert, "cert", "", "Path to the X.509 certificate.")
	flag.StringVar(&key, "key", "", "Path to the X.509 private key.")
	flag.StringVar(&cacert, "cacert", "", "Path to the X.509 CA certificate.")
	flag.BoolVar(&insecure, "insecure", false,
		"Disable the use of client certificates (for development/debugging).")
	flag.Parse()

	if len(doozer_uri) > 0 {
		err = urlconnection.SetupDoozer(doozer_buri, doozer_uri)
		if err != nil {
			log.Fatal("Unable to setup Doozer client: ", err)
		}
	}

	client, err = blubberstore.NewBlubberDirectoryClient(server, cert, key,
		cacert, insecure)
	if err != nil {
		log.Fatal("Error establishing blubber connection to ", server,
			": ", err)
	}

	req.NumHosts = proto.Int32(int32(num_blocks))
	hosts, err = client.GetFreeHosts(req)
	if err != nil {
		log.Fatal("Unable to retrieve free host list: ", err)
	}

	log.Print("Candidates:")

	for _, host = range hosts.GetHostPort() {
		log.Print(host)
	}
}
