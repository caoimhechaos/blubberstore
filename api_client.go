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

// Client library for blubberstore end users.
//
// By design, you will most likely want to use the BlubberStoreClient
// class, which is part of this package and makes use of all the other
// packages to seamlessly provide access to blobs stored on remote hosts
// which are looked up for you in the blob directory.
package blubberstore

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"

	"code.google.com/p/goprotobuf/proto"
)

// A number of hosts have been contacted but storing the block
// failed on all of them (e.g. because they could not be reached).
// This means that no data has been written whatsoever.
var Err_NoHostsReached = errors.New("Unable to copy the block to even one server - please try again")

// Less than a quorum of the requested nodes have been written.
// This means that the data may disappear again in case of
// outages.
var Err_NoQuorumReached = errors.New("No quorum could be reached")

// The write failed to reach a fully replicated set, but a quorum has
// been achieved. This may be safe to ignore, but the file should
// be distributed better.
var Err_IncompleteWrite = errors.New("Write did not complete to a sufficiently large quorum")

// BlubberStore client which handles replication, quorums and server
// lookups transparently.
type BlubberStoreClient struct {
	directoryClient   *BlubberDirectoryClient
	cert, key, cacert string
	insecure          bool
	errorLog          chan error
}

/*
Create a new Blubber Store client which will talk independently to
the directory service, discover and talk to blubber backends, and
handle timeouts etc.

uri is an URI pointing to the blubber directory.
cert and key are the path to a PEM encoded X.509 certificate and
private key.
cacert is the path to a PEM encoded X.509 CA certificate.
Setting insecure to true disables the use of certificates.
errlog is a channel for reporting non-fatal errors which occur during
operation back to the clients.
*/
func NewBlubberStoreClient(uri, cert, key, cacert string,
	insecure bool, errorlog chan error) (*BlubberStoreClient, error) {
	var dirclient *BlubberDirectoryClient
	var err error

	dirclient, err = NewBlubberDirectoryClient(
		uri, cert, key, cacert, insecure)
	if err != nil {
		return nil, err
	}

	return &BlubberStoreClient{
		directoryClient: dirclient,
		cert:            cert,
		key:             key,
		cacert:          cacert,
		insecure:        insecure,
		errorLog:        errorlog,
	}, nil
}

/*
Write the given data to a blob with the given ID, overwriting it if
specified. This will ensure that there are more than replication/2
copies of the block.
overwrite=false will disable lookups for current block holders.
*/
func (b *BlubberStoreClient) StoreBlock(id []byte, data io.Reader,
	overwrite bool, replication int) error {
	var client *BlubberRPCClient
	var block BlockWithData
	var serverlist []string
	var bs BlockSource
	var attempt int
	var servers []string
	var server string
	var err error

	if overwrite {
		var bid BlockId
		var list *BlockHolderList

		bid.BlockId = make([]byte, len(id))
		copy(bid.BlockId, id)
		list, err = b.directoryClient.LookupBlob(bid)
		if err != nil {
			return err
		}

		for _, server = range list.GetHostPort() {
			servers = append(servers, server)
		}
	}

	if len(servers) < replication {
		var freeHostReq FreeHostsRequest
		var list *BlockHolderList

		freeHostReq.NumHosts = proto.Int32(int32(replication - len(servers)))
		list, err = b.directoryClient.GetFreeHosts(freeHostReq)
		if err != nil {
			return err
		}

		for _, server = range list.GetHostPort() {
			servers = append(servers, server)
		}
	}

	// TODO(caoimhe): use the HTTP interface so we can handle large blobs.
	block.BlockId = make([]byte, len(id))
	copy(block.BlockId, id)
	serverlist = make([]string, 0)

	block.BlockData, err = ioutil.ReadAll(data)
	if err != nil {
		return err
	}

	if len(block.BlockData) == 0 {
		return errors.New("Zero-length input was detected")
	}

	// TODO(caoimhe): write this to be asynchronous and get the result from
	// a channel.
	for _, server = range servers {
		client, err = NewBlubberRPCClient(
			"tcp://"+server, b.cert, b.key, b.cacert, b.insecure)
		if err != nil {
			b.errorLog <- err
			continue
		}

		err = client.StoreBlob(block)
		if err != nil {
			b.errorLog <- err
			continue
		}

		serverlist = append(serverlist, server)
	}

	if len(serverlist) == 0 {
		return Err_NoHostsReached
	}

	// Try to copy from the existing set to a bunch of other hosts.
	for len(serverlist) < replication && attempt < replication {
		var freeHostReq FreeHostsRequest
		var list *BlockHolderList
		var sid = attempt % len(serverlist)

		// Try twice as many servers as we had left, since we may just get
		// the ones back which we tried before.
		freeHostReq.NumHosts = proto.Int32(int32(
			2 * (replication - len(serverlist))))
		list, err = b.directoryClient.GetFreeHosts(freeHostReq)
		if err != nil {
			return err
		}

		// Start the server list from scratch.
		servers = make([]string, 0)
		for _, server = range list.GetHostPort() {
			servers = append(servers, server)
		}

		bs.BlockId = make([]byte, len(id))
		copy(bs.BlockId, id)

		// TODO(caoimhe): write this to be asynchronous and get the result
		// from a channel.
		for _, server = range servers {
			client, err = NewBlubberRPCClient(
				"tcp://"+server, b.cert, b.key, b.cacert, b.insecure)
			if err != nil {
				b.errorLog <- err
				continue
			}

			bs.SourceHost = proto.String(serverlist[sid])
			err = client.CopyBlob(bs)
			if err != nil {
				b.errorLog <- err
				continue
			}

			serverlist = append(serverlist, server)
		}

		attempt += 1
	}

	// We give up (or we're done).
	if len(serverlist) < (replication / 2) {
		return Err_NoQuorumReached
	}

	if len(serverlist) < replication {
		return Err_IncompleteWrite
	}

	return nil
}

/*
Read the blob with the given ID. The blob will be looked up for you in the
block directory and a read request will be made to an appropriate server.
*/
func (b *BlubberStoreClient) RetrieveBlob(id []byte) (io.Reader, error) {
	var holders *BlockHolderList
	var bid BlockId
	var server string
	var err error

	bid.BlockId = make([]byte, len(id))
	copy(bid.BlockId, id)
	holders, err = b.directoryClient.LookupBlob(bid)
	if err != nil {
		return nil, err
	}

	// TODO(caoimhe): Don't just go by random, choose the host which
	// seems most reasonable (most frequent agreed-upon version / majority
	// vote, distance vector, etc).
	for _, server = range holders.HostPort {
		var client *BlubberRPCClient
		var bwd BlockWithData

		// TODO(caoimhe): use the HTTP interface so we can handle large
		// blobs.
		client, err = NewBlubberRPCClient(
			"tcp://"+server, b.cert, b.key, b.cacert, b.insecure)
		if err != nil {
			b.errorLog <- err
			continue
		}

		bwd, err = client.RetrieveBlob(bid)
		if err != nil {
			b.errorLog <- err
			continue
		}

		return bytes.NewReader(bwd.GetBlockData()), nil
	}

	return nil, Err_NoHostsReached
}

/*
Gather information about the status of the blob with the given ID.
Returns an object representing the status of the requested blob or an error
which may have occurred when trying to gather the requested information.
*/
func (b *BlubberStoreClient) StatBlob(id []byte) (*BlockStatus, error) {
	var status *BlockStatus = new(BlockStatus)
	var bid BlockId
	var holders *BlockHolderList
	var successes int
	var server string
	var err error

	bid.BlockId = make([]byte, len(id))
	status.BlockId = make([]byte, len(id))
	copy(bid.BlockId, id)
	copy(status.BlockId, id)
	holders, err = b.directoryClient.LookupBlob(bid)
	if err != nil {
		return nil, err
	}

	status.ReplicationFactor = proto.Uint32(uint32(len(holders.HostPort)))

	for _, server = range holders.HostPort {
		var sbs *ServerBlockStatus = new(ServerBlockStatus)
		var client *BlubberRPCClient
		var stat BlubberStat

		sbs.HostPort = proto.String(server)

		client, err = NewBlubberRPCClient(
			"tcp://"+server, b.cert, b.key, b.cacert, b.insecure)
		if err != nil {
			b.errorLog <- err
			status.Servers = append(status.Servers, sbs)
			continue
		}

		stat, err = client.StatBlob(bid)
		if err != nil {
			b.errorLog <- err
			status.Servers = append(status.Servers, sbs)
			continue
		}

		sbs.Checksum = make([]byte, len(stat.Checksum))
		copy(sbs.Checksum, stat.Checksum)

		sbs.Timestamp = proto.Uint64(stat.GetTimestamp())

		status.Servers = append(status.Servers, sbs)
		successes += 1
	}

	if successes == 0 {
		return status, Err_NoHostsReached
	}

	return status, nil
}
