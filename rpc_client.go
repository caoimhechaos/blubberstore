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

// Simple client library to connect to a single blubber store server.
package blubberstore

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"

	"github.com/caoimhechaos/go-urlconnection"
)

// A client to connect to blubber servers.
type BlubberRPCClient struct {
	config   *tls.Config
	client   *rpc.Client
	insecure bool
}

// A client to a blubber directory.
type BlubberDirectoryClient struct {
	config   *tls.Config
	client   *rpc.Client
	insecure bool
}

/*
Create a new generic RPC client connecting to the given server.

uri should be an URI pointing to the desired server and port.
cert, key and cacert should be the path of the X.509 client
certificate, private key and CA certificate, respectively.
The insecure flag can be used to disable encryption (only for
testing and if the server is also running in insecure mode).
*/
func NewRPCClient(uri, cert, key, cacert string, insecure bool) (
	*rpc.Client, *tls.Config, error) {
	var config *tls.Config = new(tls.Config)
	var conn net.Conn
	var err error

	if !insecure {
		var tlscert tls.Certificate
		var certdata []byte

		config.ClientAuth = tls.VerifyClientCertIfGiven
		config.MinVersion = tls.VersionTLS12

		tlscert, err = tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, nil, errors.New("Unable to load X.509 key pair: " +
				err.Error())
		}
		config.Certificates = append(config.Certificates, tlscert)
		config.BuildNameToCertificate()

		config.ClientCAs = x509.NewCertPool()
		certdata, err = ioutil.ReadFile(cacert)
		if err != nil {
			return nil, nil, errors.New("Error reading " + cacert + ": " +
				err.Error())
		}
		if !config.ClientCAs.AppendCertsFromPEM(certdata) {
			return nil, nil, errors.New(
				"Unable to load the X.509 certificates from " + cacert)
		}

		// Configure client side encryption.
		config.RootCAs = config.ClientCAs
	}

	// Establish a TCP connection and start a TLS exchange.
	conn, err = urlconnection.Connect(uri)
	if err != nil {
		return nil, nil, errors.New("Error connecting to " + uri + ": " +
			err.Error())
	}
	if !insecure {
		conn = tls.Client(conn, config)
	}

	// Start the RPC connection.
	_, err = io.WriteString(conn,
		"CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\r\n\r\n")
	if err != nil {
		return nil, nil, errors.New("Error requesting RPC channel to " +
			uri + ": " + err.Error())
	}

	// Let's see what the server thinks about that.
	_, err = http.ReadResponse(bufio.NewReader(conn),
		&http.Request{Method: "CONNECT"})
	if err != nil {
		return nil, nil, errors.New("Error establishing RPC channel to " +
			uri + ": " + err.Error())
	}

	return rpc.NewClient(conn), config, nil
}

/*
Create a new blubber client connecting to the given server.

uri should be an URI pointing to the desired server and port.
cert, key and cacert should be the path of the X.509 client
certificate, private key and CA certificate, respectively.
The insecure flag can be used to disable encryption (only for
testing and if the server is also running in insecure mode).
*/
func NewBlubberRPCClient(uri, cert, key, cacert string, insecure bool) (
	*BlubberRPCClient, error) {
	var client *rpc.Client
	var config *tls.Config
	var err error

	client, config, err = NewRPCClient(uri, cert, key, cacert, insecure)
	if err != nil {
		return nil, err
	}

	return &BlubberRPCClient{
		config:   config,
		client:   client,
		insecure: insecure,
	}, nil
}

/*
Get the underlying RPC client from the connection. It can sometimes be
helpful, e.g. if the remote interface is double-bound.
*/
func (self *BlubberRPCClient) GetRPCClient() *rpc.Client {
	return self.client
}

/*
Send a blob to the server and store it under the given blob ID.

If you want to submit data that is too large to fit into memory, use the
HTTP client library instead.
*/
func (self *BlubberRPCClient) StoreBlob(data BlockWithData) (
	err error) {
	var rid BlockId
	err = self.client.Call("BlubberService.StoreBlob", data, &rid)
	return
}

/*
Retrieve all data contained in the specified blob. If you want to retrieve
more data than will fit into memory, use the HTTP client library instead.
*/
func (self *BlubberRPCClient) RetrieveBlob(id BlockId) (
	ret BlockWithData, err error) {
	err = self.client.Call("BlubberService.RetrieveBlob", id, &ret)
	return
}

/*
Delete the blob with the given ID.
*/
func (self *BlubberRPCClient) DeleteBlob(id BlockId) error {
	var rid BlockId
	return self.client.Call("BlubberService.DeleteBlob", id, &rid)
}

/*
Retrieve status information about the blob with the given ID.
*/
func (self *BlubberRPCClient) StatBlob(id BlockId) (
	stat BlubberStat, err error) {
	err = self.client.Call("BlubberService.StatBlob", id, &stat)
	return
}

/*
Instruct the server to copy the blob  with the given ID from the given
server. It will overwrite any local variants of the blob.
*/
func (self *BlubberRPCClient) CopyBlob(source BlockSource) error {
	var id BlockId
	return self.client.Call("BlubberService.CopyBlob", source, &id)
}

/*
Create a new blubber directory client connecting to the given server.

uri should be an URI pointing to the desired server and port.
cert, key and cacert should be the path of the X.509 client
certificate, private key and CA certificate, respectively.
The insecure flag can be used to disable encryption (only for
testing and if the server is also running in insecure mode).
*/
func NewBlubberDirectoryClient(uri, cert, key, cacert string, insecure bool) (
	*BlubberDirectoryClient, error) {
	var client *rpc.Client
	var config *tls.Config
	var err error

	client, config, err = NewRPCClient(uri, cert, key, cacert, insecure)
	if err != nil {
		return nil, err
	}

	return &BlubberDirectoryClient{
		config:   config,
		client:   client,
		insecure: insecure,
	}, nil
}

/*
Report to the directory that the block with the given properties is
now stored on the specified server.
*/
func (b *BlubberDirectoryClient) ReportBlob(report BlockReport) error {
	var id BlockId
	return b.client.Call("BlubberBlockDirectory.ReportBlob", report, &id)
}

/*
Look up all the hosts currently known to hold the requested block.
*/
func (b *BlubberDirectoryClient) LookupBlob(id BlockId) (list *BlockHolderList, err error) {
	list = new(BlockHolderList)
	err = b.client.Call("BlubberBlockDirectory.LookupBlob", id, &list)
	return
}

/*
Remove the given host from the holders of the blob.
*/
func (b *BlubberDirectoryClient) RemoveBlobHolder(rep BlockReport) error {
	var id BlockId
	return b.client.Call("BlubberBlockDirectory.RemoveBlobHolder", rep, &id)
}
