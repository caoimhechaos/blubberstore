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

package blubberstore

import (
	"errors"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/caoimhechaos/go-serialdata"
)

// Block directory service for blubberstore.
type BlubberBlockDirectory struct {
	// Pointer to the currently active journal.
	currentJournal *serialdata.SerialDataWriter
	journalFile    *os.File
	journalPrefix  string

	// Mapping of block IDs to hosts which contain them to block states.
	blockMap       map[string]map[string]*ServerBlockStatus
	blockMapMtx    *sync.RWMutex
	blockMapPrefix string
}

func NewBlubberBlockDirectory(
	journalPrefix, blockMapPrefix string) (*BlubberBlockDirectory, error) {
	var now time.Time = time.Now()
	var newpath string = journalPrefix + now.Format("2006-01-02.150405")
	var blockMap map[string]map[string]*ServerBlockStatus
	var parentDir *os.File
	var stateDumpFile, newJournal *os.File
	var reader *serialdata.SerialDataReader
	var ret *BlubberBlockDirectory
	var names []string
	var name string
	var bs BlockStatus
	var err error

	// Try to find the latest complete block map.
	stateDumpFile, err = os.Open(blockMapPrefix + ".blockmap")
	if err != nil && os.IsNotExist(err) {
		stateDumpFile, err = os.Open(blockMapPrefix + ".old")
	}
	if err != nil && os.IsNotExist(err) {
		log.Print("Warning: created new empty block map")
		stateDumpFile, err = os.Create(blockMapPrefix + ".blockmap")
	} else {
		return nil, errors.New("Unable to open block map: " + err.Error())
	}

	// Read all data from the state dump file.
	reader = serialdata.NewSerialDataReader(stateDumpFile)

	for err = reader.ReadMessage(&bs); err != nil; err = reader.ReadMessage(&bs) {
		var blockId string = string(bs.GetBlockId())
		var srv *ServerBlockStatus

		blockMap[blockId] = make(map[string]*ServerBlockStatus)

		for _, srv = range bs.Servers {
			blockMap[blockId][srv.GetHostPort()] = srv
		}

		bs.Reset()
	}

	newJournal, err = os.Create(newpath)
	if err != nil {
		return nil, errors.New("Error creating " + newpath + ": " +
			err.Error())
	}

	ret = &BlubberBlockDirectory{
		currentJournal: serialdata.NewSerialDataWriter(newJournal),
		journalFile:    newJournal,
		journalPrefix:  journalPrefix,
		blockMap:       blockMap,
		blockMapMtx:    new(sync.RWMutex),
		blockMapPrefix: blockMapPrefix,
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
			var br BlockReport

			journal, err = os.Open(parentDir.Name() + "/" + name)
			if err != nil {
				return nil, err
			}

			reader = serialdata.NewSerialDataReader(journal)
			for err = reader.ReadMessage(&br); err != nil; err = reader.ReadMessage(&br) {
				ret.ApplyBlockReport(&br)
			}
			journal.Close()
		}
	}

	// Start syncing the state dump to disk.
	go ret.PeriodicallyWriteStateDump()

	return ret, nil
}

// Write a complete state dump to disk regularly and remove the journals.
func (b *BlubberBlockDirectory) PeriodicallyWriteStateDump() {
	var c <-chan time.Time

	c = time.Tick(2 * time.Minute)
	for _ = range c {
		b.dumpState()
	}
}

// Apply a single BlockReport to the internal state without writing it to
// the journal. This is useful e.g. for replaying the journal.
func (b *BlubberBlockDirectory) ApplyBlockReport(status *BlockReport) {
	var host string

	// Register the updated block for all reported servers.
	b.blockMapMtx.Lock()
	defer b.blockMapMtx.Unlock()

	for _, host = range status.Server {
		// Create a new ServerBlockStatus record with the information we have.
		var sbs *ServerBlockStatus = &ServerBlockStatus{
			HostPort:  proto.String(host),
			Checksum:  make([]byte, len(status.Status.Checksum)),
			Timestamp: proto.Uint64(status.Status.GetTimestamp()),
		}
		var ok bool
		copy(sbs.Checksum, status.Status.Checksum)

		_, ok = b.blockMap[string(status.Status.BlockId)]
		if !ok {
			b.blockMap[string(status.Status.BlockId)] =
				make(map[string]*ServerBlockStatus)
		}
		b.blockMap[string(status.Status.BlockId)][host] = sbs
	}
}

// Report to the directory that the block with the given properties is now
// stored on the specified server.
func (b *BlubberBlockDirectory) ReportBlob(status BlockReport, res *BlockId) error {
	var err error

	res.BlockId = make([]byte, len(status.Status.BlockId))
	copy(res.BlockId, status.Status.BlockId)

	// Write all data about the reported blob to the journal.
	err = b.currentJournal.WriteMessage(&status)
	if err != nil {
		return err
	}

	// Now apply the report to the internal state.
	b.ApplyBlockReport(&status)

	return nil
}

// Look up the block with the given ID.
func (b *BlubberBlockDirectory) LookupBlob(
	req BlockId, res *BlockHolderList) error {
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

// Write a new dump of the current state of block/server mappings.
func (b *BlubberBlockDirectory) dumpState() {
	var now time.Time = time.Now()
	var newpath string = b.journalPrefix + now.Format("2006-01-02.150405")
	var oldJournalFile *os.File = b.journalFile
	var newJournalFile *os.File
	var newStateDumpFile *os.File
	var stateDumpWriter *serialdata.SerialDataWriter
	var blockId string
	var serverMap map[string]*ServerBlockStatus
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
		var bs BlockStatus
		var host string
		var srv *ServerBlockStatus

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
	err = os.Rename(b.blockMapPrefix+".statedump", b.blockMapPrefix+".old")
	if err != nil {
		log.Print("Error moving away old state dump: ", err)
	}

	// Move the new state dump into the place where the old one used to be.
	err = os.Rename(b.blockMapPrefix+".new", b.blockMapPrefix+".statedump")
	if err != nil {
		log.Print("Error moving new state dump into place: ", err)
		os.Remove(b.blockMapPrefix + ".new")
		return
	}

	err = os.Remove(b.blockMapPrefix + ".old")
	if err != nil {
		log.Print("Error deleting old state dump: ", err)
	}

	err = os.Remove(oldJournalFile.Name())
	if err != nil {
		log.Print("Error deleting old journal file ", oldJournalFile.Name(),
			": ", err)
	}
}
