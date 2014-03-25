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
	"bytes"

	"github.com/caoimhechaos/blubberstore"
)

type BlubberService struct {
	store *blubberStore
}

func (self *BlubberService) StoreBlob(
	req blubberstore.BlockWithData, res *blubberstore.BlockId) error {
	var rd *bytes.Reader = bytes.NewReader(req.BlockData)
	res.BlockId = make([]byte, len(req.BlockId))
	copy(res.BlockId, req.BlockId)
	return self.store.StoreBlob(req.GetBlockId(), rd)
}

func (self *BlubberService) RetrieveBlob(
	req blubberstore.BlockId, res *blubberstore.BlockWithData) error {
	var buf *bytes.Buffer = new(bytes.Buffer)
	var err error

	err = self.store.RetrieveBlob(req.GetBlockId(), buf)
	if err != nil {
		return err
	}

	res.BlockId = make([]byte, len(req.BlockId))
	copy(res.BlockId, req.BlockId)
	res.BlockData = make([]byte, buf.Len())
	copy(res.BlockData, buf.Bytes())
	return nil
}

func (self *BlubberService) DeleteBlob(
	req blubberstore.BlockId, res *blubberstore.BlockId) error {
	res.BlockId = make([]byte, len(req.BlockId))
	copy(res.BlockId, req.BlockId)
	return self.store.DeleteBlob(req.GetBlockId())
}

func (self *BlubberService) StatBlob(
	req blubberstore.BlockId, res *blubberstore.BlubberStat) error {
	var stat blubberstore.BlubberStat
	var err error

	stat, err = self.store.StatBlob(req.GetBlockId())
	if err != nil {
		return err
	}

	res.BlockId = make([]byte, len(stat.BlockId))
	copy(res.BlockId, stat.BlockId)
	res.Checksum = make([]byte, len(stat.Checksum))
	copy(res.Checksum, stat.Checksum)
	res.Size = stat.Size

	return nil
}

func (self *BlubberService) CopyBlob(
	src blubberstore.BlockSource, res *blubberstore.BlockId) error {
	res.BlockId = make([]byte, len(src.BlockId))
	copy(res.BlockId, src.BlockId)
	return self.store.CopyBlob(src.GetBlockId(), src.GetSourceHost())
}
