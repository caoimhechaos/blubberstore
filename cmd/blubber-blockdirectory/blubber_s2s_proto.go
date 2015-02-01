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
	"sync"

	"code.google.com/p/goprotobuf/proto"
	"github.com/caoimhechaos/blubberstore"
	"github.com/ha/doozer"
)

// Serivce object for server-to-server manipulation of the blubber block
// directory.
type BlubberS2SProto struct {
	// Doozer connection (if any).
	doozerConn *doozer.Conn

	// Pointers to the inner server state and its mutex.
	blockMap        *map[string]map[string]*blubberstore.ServerBlockStatus
	blockMapMtx     *sync.RWMutex
	blockMapVersion *uint64
}

// Create a new server2server object of the pointed-to block directory state.
func NewBlubberS2SProto(conn *doozer.Conn,
	blockmap *map[string]map[string]*blubberstore.ServerBlockStatus,
	blockmapmtx *sync.RWMutex) *BlubberS2SProto {
	return &BlubberS2SProto{
		doozerConn:  conn,
		blockMap:    blockmap,
		blockMapMtx: blockmapmtx,
	}
}

// Determine the current block directory version.
func (b *BlubberS2SProto) GetBlockDirectoryRevision(
	empty *blubberstore.Empty, rev *blubberstore.BlockDirectoryRevision) error {
	rev.Revision = proto.Uint64(*b.blockMapVersion)
	return nil
}

func (b *BlubberS2SProto) GetBlockDirectoryFull(
	empty *blubberstore.Empty, rv *blubberstore.BlockDirectoryRevision) error {
	var states map[string]*blubberstore.ServerBlockStatus
	var id string

	// Make sure we're not being disturbed.
	b.blockMapMtx.RLock()
	defer b.blockMapMtx.RUnlock()

	rv.Revision = proto.Uint64(*b.blockMapVersion)

	for id, states = range *b.blockMap {
		var status *blubberstore.BlockStatus = new(blubberstore.BlockStatus)
		var sstat *blubberstore.ServerBlockStatus

		status.BlockId = make([]byte, len(id))
		copy(status.BlockId, []byte(id))

		status.ReplicationFactor = proto.Uint32(uint32(len(states)))
		status.Servers = make([]*blubberstore.ServerBlockStatus, 0)

		for _, sstat = range states {
			status.Servers = append(status.Servers, sstat)
		}

		rv.Status = append(rv.Status, status)
	}

	return nil
}
