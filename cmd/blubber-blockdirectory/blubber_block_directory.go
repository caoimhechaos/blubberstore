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
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/caoimhechaos/blubberstore"
	"github.com/caoimhechaos/go-serialdata"
	"github.com/ha/doozer"
)

// Block directory service for blubberstore.
type BlubberBlockDirectory struct {
	// Pointer to the currently active journal.
	currentJournal *serialdata.SerialDataWriter
	journalFile    *os.File
	journalPrefix  string

	// Doozer connection (if any).
	doozerConn         *doozer.Conn
	blockServicePrefix string

	// Mapping of block IDs to hosts which contain them to block states.
	blockHostMap   map[string][]string
	blockMap       map[string]map[string]*blubberstore.ServerBlockStatus
	blockMapMtx    *sync.RWMutex
	blockMapPrefix string
}

func NewBlubberBlockDirectory(
	doozerClient *doozer.Conn,
	blockServicePrefix, journalPrefix, blockMapPrefix string) (
	*BlubberBlockDirectory, error) {
	var now time.Time = time.Now()
	var newpath string = journalPrefix + now.Format("2006-01-02.150405")
	var blockMap map[string]map[string]*blubberstore.ServerBlockStatus = make(map[string]map[string]*blubberstore.ServerBlockStatus)
	var blockHostMap map[string][]string = make(map[string][]string)
	var parentDir *os.File
	var stateDumpFile, newJournal *os.File
	var reader *serialdata.SerialDataReader
	var ret *BlubberBlockDirectory
	var names []string
	var name string
	var bs blubberstore.BlockStatus
	var err error

	// Try to find the latest complete block map.
	stateDumpFile, err = os.Open(blockMapPrefix + ".blockmap")
	if err != nil && os.IsNotExist(err) {
		stateDumpFile, err = os.Open(blockMapPrefix + ".old")
	}
	if err != nil && os.IsNotExist(err) {
		log.Print("Warning: created new empty block map")
		stateDumpFile, err = os.Create(blockMapPrefix + ".blockmap")
	} else if err != nil {
		return nil, errors.New("Unable to open block map: " + err.Error())
	}

	// Read all data from the state dump file.
	reader = serialdata.NewSerialDataReader(stateDumpFile)

	for err = reader.ReadMessage(&bs); err == nil; err = reader.ReadMessage(&bs) {
		var blockId string = string(bs.GetBlockId())
		var srv *blubberstore.ServerBlockStatus

		blockMap[blockId] = make(
			map[string]*blubberstore.ServerBlockStatus)

		for _, srv = range bs.Servers {
			blockMap[blockId][srv.GetHostPort()] = srv
			blockHostMap[srv.GetHostPort()] = append(
				blockHostMap[srv.GetHostPort()], blockId)
		}

		bs.Reset()
	}

	newJournal, err = os.Create(newpath)
	if err != nil {
		return nil, errors.New("Error creating " + newpath + ": " +
			err.Error())
	}

	ret = &BlubberBlockDirectory{
		currentJournal:     serialdata.NewSerialDataWriter(newJournal),
		journalFile:        newJournal,
		journalPrefix:      journalPrefix,
		doozerConn:         doozerClient,
		blockServicePrefix: blockServicePrefix,
		blockHostMap:       blockHostMap,
		blockMap:           blockMap,
		blockMapMtx:        new(sync.RWMutex),
		blockMapPrefix:     blockMapPrefix,
	}

	// Now replay all the journal files we can find.
	parentDir, err = os.Open(path.Dir(journalPrefix))
	if err != nil {
		return nil, errors.New("Unable to open " + path.Dir(journalPrefix) +
			": " + err.Error())
	}
	names, err = parentDir.Readdirnames(-1)
	if err != nil {
		return nil, errors.New("Unable to list " + path.Dir(journalPrefix) +
			": " + err.Error())
	}
	parentDir.Close()

	for _, name = range names {
		if strings.HasPrefix(name, path.Base(journalPrefix)) {
			var journal *os.File
			var br blubberstore.BlockReport

			journal, err = os.Open(parentDir.Name() + "/" + name)
			if err != nil {
				return nil, err
			}

			reader = serialdata.NewSerialDataReader(journal)
			for err = reader.ReadMessage(&br); err == nil; err = reader.ReadMessage(&br) {
				ret.applyBlockReport(&br)
			}
			journal.Close()
		}
	}

	// Start syncing the state dump to disk.
	go ret.periodicallyWriteStateDump()

	return ret, nil
}

// Write a complete state dump to disk regularly and remove the journals.
func (b *BlubberBlockDirectory) periodicallyWriteStateDump() {
	var c <-chan time.Time

	c = time.Tick(2 * time.Minute)
	for _ = range c {
		b.dumpState()
	}
}

// Apply a single BlockReport to the internal state without writing it to
// the journal. This is useful e.g. for replaying the journal.
func (b *BlubberBlockDirectory) applyBlockReport(
	status *blubberstore.BlockReport) {
	var host string

	// Register the updated block for all reported servers.
	b.blockMapMtx.Lock()
	defer b.blockMapMtx.Unlock()

	for _, host = range status.Server {
		// Create a new ServerBlockStatus record with the information we have.
		var sbs *blubberstore.ServerBlockStatus = &blubberstore.ServerBlockStatus{
			HostPort:  proto.String(host),
			Checksum:  make([]byte, len(status.Status.Checksum)),
			Timestamp: proto.Uint64(status.Status.GetTimestamp()),
		}
		var ok bool
		copy(sbs.Checksum, status.Status.Checksum)

		_, ok = b.blockMap[string(status.Status.BlockId)]
		if !ok {
			b.blockMap[string(status.Status.BlockId)] = make(
				map[string]*blubberstore.ServerBlockStatus)
		}
		b.blockMap[string(status.Status.BlockId)][host] = sbs
		b.blockHostMap[host] = append(b.blockHostMap[host],
			string(status.Status.BlockId))
	}
}

// Report to the directory that the block with the given properties is now
// stored on the specified server.
func (b *BlubberBlockDirectory) ReportBlob(status blubberstore.BlockReport,
	res *blubberstore.BlockId) error {
	var err error

	res.BlockId = make([]byte, len(status.Status.BlockId))
	copy(res.BlockId, status.Status.BlockId)

	// Write all data about the reported blob to the journal.
	err = b.currentJournal.WriteMessage(&status)
	if err != nil {
		return err
	}

	// Now apply the report to the internal state.
	b.applyBlockReport(&status)

	return nil
}

// Look up the block with the given ID.
func (b *BlubberBlockDirectory) LookupBlob(
	req blubberstore.BlockId, res *blubberstore.BlockHolderList) error {
	var host string
	var ok bool

	b.blockMapMtx.RLock()
	defer b.blockMapMtx.RUnlock()
	_, ok = b.blockMap[string(req.BlockId)]
	if !ok {
		// We don't know anything about the block.
		return nil
	}

	// FIXME: we should check the majority holders here instead of just
	// returning a list of all hosts.
	for host, _ = range b.blockMap[string(req.BlockId)] {
		res.HostPort = append(res.HostPort, host)
	}

	return nil
}

// Code to individually remove a given blob -> server association.
func (b *BlubberBlockDirectory) removeBlobHolder(blobId []byte,
	server string) {
	var blocks []string
	var block string

	delete(b.blockMap[string(blobId)], server)
	blocks = b.blockHostMap[server]
	b.blockHostMap[server] = make([]string, 0)

	for _, block = range blocks {
		if block != string(blobId) {
			b.blockHostMap[server] = append(b.blockHostMap[server],
				block)
		}
	}
}

// Remove the given host from the holders of the blob.
func (b *BlubberBlockDirectory) RemoveBlobHolder(
	report blubberstore.BlockRemovalReport,
	res *blubberstore.BlockId) error {
	var server string
	b.blockMapMtx.Lock()
	defer b.blockMapMtx.Unlock()

	for _, server = range report.GetServer() {
		b.removeBlobHolder(report.GetBlockId(), server)
	}

	res.BlockId = make([]byte, len(report.GetBlockId()))
	copy(res.BlockId, report.GetBlockId())

	return nil
}

// Delete all block ownerships associated with the given host. This is very
// useful e.g. if a host goes down or data is lost on it.
func (b *BlubberBlockDirectory) ExpireHost(hosts blubberstore.BlockHolderList,
	ret *blubberstore.BlockHolderList) error {
	var host string

	b.blockMapMtx.Lock()
	defer b.blockMapMtx.Unlock()
	for _, host = range hosts.HostPort {
		var blockId string
		for _, blockId = range b.blockHostMap[host] {
			b.removeBlobHolder([]byte(blockId), host)
		}

		ret.HostPort = append(ret.HostPort, host)
	}

	return nil
}

// Get a list of all hosts known to own blocks. This is mostly used by the
// cleanup jobs.
func (b *BlubberBlockDirectory) ListHosts(
	void blubberstore.Empty, hosts *blubberstore.BlockHolderList) error {
	var host string
	b.blockMapMtx.RLock()
	defer b.blockMapMtx.RUnlock()

	for host, _ = range b.blockHostMap {
		hosts.HostPort = append(hosts.HostPort, host)
	}

	return nil
}

// Pick a number of hosts from the available list. This will try to pick
// hosts which hold less keys than the others.
func (b *BlubberBlockDirectory) GetFreeHosts(req blubberstore.FreeHostsRequest,
	hosts *blubberstore.BlockHolderList) error {
	var allhosts []string
	var onehost string
	var i int
	var min int = -1

	if b.doozerConn == nil {
		var host string
		for host, _ = range b.blockHostMap {
			allhosts = append(allhosts, host)
		}
	} else {
		var names []string
		var name string
		var rev int64
		var err error

		rev, err = b.doozerConn.Rev()
		if err != nil {
			return err
		}

		// TODO(caoimhe): reading until the end may return MANY records.
		names, err = b.doozerConn.Getdir(b.blockServicePrefix, rev, 0, -1)
		if err != nil {
			return err
		}

		for _, name = range names {
			var data []byte
			data, _, err = b.doozerConn.Get(b.blockServicePrefix+"/"+name, &rev)
			if err == nil {
				allhosts = append(allhosts, string(data))
			} else {
				log.Print("Unable to retrieve version ", rev, " of ",
					b.blockServicePrefix, "/", name, ": ", err)
			}
		}
	}

	for i = 0; int32(i) < req.GetNumHosts(); i++ {
		var ok bool
		if i >= len(allhosts) {
			break
		}

		hosts.HostPort = append(hosts.HostPort, allhosts[i])
		_, ok = b.blockHostMap[allhosts[i]]
		if !ok {
			min = 0
		} else if min < 1 || len(b.blockHostMap[allhosts[i]]) < min {
			min = len(b.blockHostMap[allhosts[i]])
		}
	}

	for i, onehost = range allhosts {
		var ok bool

		if int32(i) < req.GetNumHosts() {
			continue
		}

		_, ok = b.blockHostMap[onehost]
		if !ok {
			hosts.HostPort = append(hosts.HostPort[1:], onehost)
			min = 0
		} else if len(b.blockHostMap[onehost]) <= min {
			hosts.HostPort = append(hosts.HostPort[1:], onehost)
			min = len(b.blockHostMap[onehost])
		}
	}

	return nil
}

// Write a new dump of the current state of block/server mappings.
func (b *BlubberBlockDirectory) dumpState() {
	var now time.Time = time.Now()
	var newpath string = b.journalPrefix + now.Format("2006-01-02.150405")
	var oldJournalFile *os.File = b.journalFile
	var newJournalFile *os.File
	var newStateDumpFile *os.File
	var stateDumpWriter *serialdata.SerialDataWriter
	var blockId string
	var serverMap map[string]*blubberstore.ServerBlockStatus
	var err error

	os.Remove(b.blockMapPrefix + ".new")
	newStateDumpFile, err = os.Create(b.blockMapPrefix + ".new")
	if err != nil {
		log.Print("Unable to open new journal file ", newpath, ": ", err)
		log.Print("Skipping state dump")
		return
	}

	newJournalFile, err = os.Create(newpath)
	if err != nil {
		log.Print("Unable to open new journal file ", newpath, ": ", err)
		log.Print("Skipping state dump")
		return
	}

	// Start a new journal to track future differences. We'll assume they're
	// idempotent so we can just replay them on top of an already-modified
	// tree.
	b.currentJournal = serialdata.NewSerialDataWriter(newJournalFile)
	b.journalFile = newJournalFile

	stateDumpWriter = serialdata.NewSerialDataWriter(newStateDumpFile)

	b.blockMapMtx.RLock()
	defer b.blockMapMtx.RUnlock()
	for blockId, serverMap = range b.blockMap {
		var bs blubberstore.BlockStatus
		var host string
		var srv *blubberstore.ServerBlockStatus

		bs.BlockId = make([]byte, len([]byte(blockId)))
		copy(bs.BlockId, []byte(blockId))

		// TODO(caoimhe): make this something user specified.
		bs.ReplicationFactor = proto.Uint32(3)

		for host, srv = range serverMap {
			if srv.GetHostPort() != host {
				log.Print("host:port mismatch for block ", []byte(blockId),
					": ", srv.GetHostPort(), " vs. ", host)
				srv.HostPort = proto.String(host)
			}

			bs.Servers = append(bs.Servers, srv)
		}

		err = stateDumpWriter.WriteMessage(&bs)
		if err != nil {
			log.Print("Error writing record to state dump file: ", err)
			newStateDumpFile.Close()
			os.Remove(b.blockMapPrefix + ".new")
		}
	}

	err = newStateDumpFile.Close()
	if err != nil {
		log.Print("Error writing state dump: ", err)
		os.Remove(b.blockMapPrefix + ".new")
		return
	}

	// Try moving the old state dump out of the way.
	err = os.Rename(b.blockMapPrefix+".blockmap", b.blockMapPrefix+".old")
	if err != nil && !os.IsNotExist(err) {
		log.Print("Error moving away old state dump: ", err)
	}

	// Move the new state dump into the place where the old one used to be.
	err = os.Rename(b.blockMapPrefix+".new", b.blockMapPrefix+".blockmap")
	if err != nil {
		log.Print("Error moving new state dump into place: ", err)
		os.Remove(b.blockMapPrefix + ".new")
		return
	}

	err = os.Remove(b.blockMapPrefix + ".old")
	if err != nil && !os.IsNotExist(err) {
		log.Print("Error deleting old state dump: ", err)
	}

	err = os.Remove(oldJournalFile.Name())
	if err != nil {
		log.Print("Error deleting old journal file ", oldJournalFile.Name(),
			": ", err)
	}
}
