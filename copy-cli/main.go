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

// Command line interface to initiate copies of blocks between servers.
package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/caoimhechaos/blubberstore"
	"github.com/caoimhechaos/go-urlconnection"
)

func main() {
	var config *tls.Config = new(tls.Config)
	var client *rpc.Client
	var conn net.Conn

	var req blubberstore.BlockSource
	var res blubberstore.BlockId

	var doozer_uri, doozer_buri string
	var cert, key, cacert string
	var blob_id, source string
	var server string
	var insecure bool
	var err error

	flag.StringVar(&blob_id, "block-id", "",
		"ID of the block which should be copied.")
	flag.StringVar(&source, "source", "", "host:port pair to copy from.")
	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"URI of the Doozer lock service.")
	flag.StringVar(&doozer_uri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"),
		"Boot URI of the Doozer lock service.")
	flag.StringVar(&server, "server", "[::]:0",
		"URL to connect and send commands to.")

	flag.StringVar(&cert, "cert", "", "Path to the X.509 certificate.")
	flag.StringVar(&key, "key", "", "Path to the X.509 private key.")
	flag.StringVar(&cacert, "cacert", "", "Path to the X.509 CA certificate.")
	flag.BoolVar(&insecure, "insecure", false,
		"Disable the use of client certificates (for development/debugging).")
	flag.Parse()

	if !insecure {
		var tlscert tls.Certificate
		var certdata []byte

		config.ClientAuth = tls.VerifyClientCertIfGiven
		config.MinVersion = tls.VersionTLS12

		tlscert, err = tls.LoadX509KeyPair(cert, key)
		if err != nil {
			log.Fatal("Unable to load X.509 key pair: ", err)
		}
		config.Certificates = append(config.Certificates, tlscert)
		config.BuildNameToCertificate()

		config.ClientCAs = x509.NewCertPool()
		certdata, err = ioutil.ReadFile(cacert)
		if err != nil {
			log.Fatal("Error reading ", cacert, ": ", err)
		}
		if !config.ClientCAs.AppendCertsFromPEM(certdata) {
			log.Fatal("Unable to load the X.509 certificates from ", cacert)
		}

		// Configure client side encryption.
		config.RootCAs = config.ClientCAs
	}

	if len(doozer_uri) > 0 {
		err = urlconnection.SetupDoozer(doozer_buri, doozer_uri)
		if err != nil {
			log.Fatal("Unable to setup Doozer client: ", err)
		}
	}

	// Establish a TCP connection and start a TLS exchange.
	conn, err = urlconnection.Connect(server)
	if err != nil {
		log.Fatal("Error connecting to ", server, ": ", err)
	}
	if !insecure {
		conn = tls.Client(conn, config)
	}

	// Start the RPC connection.
	_, err = io.WriteString(conn,
		"CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\r\n\r\n")
	if err != nil {
		log.Fatal("Error requesting RPC channel to ", server, ": ", err)
	}

	// Let's see what the server thinks about that.
	_, err = http.ReadResponse(bufio.NewReader(conn),
		&http.Request{Method: "CONNECT"})
	if err != nil {
		log.Fatal("Error establishing RPC channel to ", server, ": ", err)
	}

	client = rpc.NewClient(conn)

	req.BlockId, err = hex.DecodeString(blob_id)
	if err != nil {
		log.Fatal("Unable to decode block ID: ", err)
	}
	req.SourceHost = &source

	err = client.Call("BlubberService.CopyBlob", &req, &res)
	if err != nil {
		log.Fatal("Unable to copy block ", blob_id, ": ", err)
	}

	log.Print("Copied block ", res.GetBlockId())
}
