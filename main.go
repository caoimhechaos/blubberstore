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

package main

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	_ "expvar"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"ancient-solutions.com/doozer/exportedservice"
)

func main() {
	var ra *RESTAdapter
	var bs *blubberStore
	var srv *BlubberService
	var config *tls.Config = new(tls.Config)
	var rsa_key *rsa.PrivateKey
	var l net.Listener

	var doozer_uri, doozer_buri string
	var cert, key, cacert string
	var blob_path string
	var bind string
	var insecure bool
	var err error

	flag.StringVar(&blob_path, "blob-path", "",
		"Path to find and store the blobs.")
	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"URI of the Doozer lock service.")
	flag.StringVar(&doozer_uri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"),
		"Boot URI of the Doozer lock service.")
	flag.StringVar(&bind, "bind", "[::]:0",
		"host:port pair or host name to bind to.")
	flag.BoolVar(&insecure, "insecure", false,
		"Disable the use of client certificates (for development/debugging).")
	flag.Parse()

	if !insecure {
		var tlscert tls.Certificate
		var certdata []byte
		var ok bool

		config.ClientAuth = tls.VerifyClientCertIfGiven
		config.MinVersion = tls.VersionTLS12

		tlscert, err = tls.LoadX509KeyPair(cert, key)
		if err != nil {
			log.Fatal("Unable to load X.509 key pair: ", err)
		}
		config.Certificates = append(config.Certificates, tlscert)
		config.BuildNameToCertificate()

		rsa_key, ok = tlscert.PrivateKey.(*rsa.PrivateKey)
		if !ok {
			log.Fatal("Private key type is not RSA.")
		}

		config.ClientCAs = x509.NewCertPool()
		certdata, err = ioutil.ReadFile(cacert)
		if err != nil {
			log.Fatal("Error reading ", cacert, ": ", err)
		}
		if !config.ClientCAs.AppendCertsFromPEM(certdata) {
			log.Fatal("Unable to load the X.509 certificates from ", cacert)
		}
	}

	if insecure {
		l, err = net.Listen("tcp", bind)
	} else if len(doozer_uri) > 0 {
		var exporter *exportedservice.ServiceExporter

		exporter, err = exportedservice.NewExporter(
			doozer_uri, doozer_buri)
		if err != nil {
			log.Fatal("Error creating port exporter: ", err)
		}
		l, err = exporter.NewExportedTLSPort(
			"tcp", bind, "blubber-service", config)
	} else {
		tls.Listen("tcp", bind, config)
	}

	log.Print("Started listening to http://", l.Addr())

	rpc.HandleHTTP()

	bs = &blubberStore{
		blob_path: blob_path,
		insecure:  insecure,
		priv:      rsa_key,
	}
	ra = &RESTAdapter{
		store: bs,
	}
	srv = &BlubberService{
		store: bs,
	}

	err = rpc.Register(srv)
	if err != nil {
		log.Fatal("Failed to register BlubberService: ", err)
	}

	http.Handle("/", ra)
	err = http.Serve(l, nil)
	if err != nil {
		log.Fatal("Error serving HTTP on ", l.Addr())
	}
}
