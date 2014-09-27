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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"github.com/caoimhechaos/blubberstore"
)

// Special "writer" which just counts the number of bytes passed through.
type CountingWriter struct {
	n uint64
}

// Add the length of p to the counter.
func (self *CountingWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	self.n += uint64(n)
	return
}

// Return the full number of bytes passed through Write().
func (self *CountingWriter) BytesWritten() uint64 {
	return self.n
}

type blubberStore struct {
	bindHostPort    string
	blobPath        string
	directoryClient *blubberstore.BlubberDirectoryClient
	insecure        bool
	priv            *rsa.PrivateKey
	tlsConfig       *tls.Config
}

// Construct the file name prefix for the blob with the given blob ID.
// Returns the full name prefix and the parent directory.
func (self *blubberStore) blobId2FileName(blobId []byte) (
	string, string) {
	var parent_dir, first_dir, second_dir string
	var path_name string = hex.EncodeToString(blobId)

	// Extract the first and second directory part piece.
	if len(blobId) > 0 {
		first_dir = strconv.FormatUint(uint64(blobId[0]), 16)
	} else {
		path_name = "zz"
		first_dir = "00"
	}
	if len(blobId) > 1 {
		second_dir = first_dir + strconv.FormatUint(uint64(blobId[1]), 16)
	} else {
		second_dir = first_dir + "00"
	}

	parent_dir = self.blobPath + "/" + first_dir + "/" + second_dir
	return parent_dir + "/" + path_name, parent_dir
}

// Create a new blob with the given blobId, or overwrite an existing one.
// The contents of the blob will be read from input.
func (self *blubberStore) StoreBlob(blobId []byte, input io.Reader,
	overwrite bool) error {
	var outstream cipher.StreamWriter
	var bh blubberstore.BlubberBlockHeader
	var report blubberstore.BlockReport
	var aescipher cipher.Block
	var cksum hash.Hash = sha256.New()
	var parent_dir, file_prefix string
	var counter CountingWriter
	var outfile *os.File
	var buf []byte
	var flag int
	var n int
	var err error

	// Create a block key and IV for the blob data.
	bh.BlockKey = make([]byte, aes.BlockSize)
	n, err = rand.Read(bh.BlockKey)
	if err != nil {
		return err
	}
	if n != aes.BlockSize {
		return errors.New("Unexpected length of random data")
	}

	bh.Iv = make([]byte, aes.BlockSize)
	n, err = rand.Read(bh.Iv)
	if err != nil {
		return err
	}
	if n != aes.BlockSize {
		return errors.New("Unexpected length of random data")
	}

	file_prefix, parent_dir = self.blobId2FileName(blobId)

	// Ensure we have the full directory path in place.
	err = os.MkdirAll(parent_dir, 0700)
	if err != nil {
		return err
	}

	// Now, write the actual data.
	flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if !overwrite {
		flag |= os.O_EXCL
	}
	outfile, err = os.OpenFile(file_prefix+".data", flag, 0600)
	if err != nil {
		return err
	}
	aescipher, err = aes.NewCipher(bh.BlockKey)
	if err != nil {
		return err
	}
	outstream = cipher.StreamWriter{
		S: cipher.NewCTR(aescipher, bh.Iv),
		W: outfile,
	}
	_, err = io.Copy(io.MultiWriter(outstream, cksum, &counter), input)
	if err != nil {
		return err
	}
	err = outstream.Close()
	if err != nil {
		return err
	}

	// Fill in the last bits of the blob header.
	bh.Checksum = cksum.Sum(bh.Checksum)
	bh.Size = proto.Uint64(counter.BytesWritten())
	bh.Timestamp = proto.Uint64(uint64(time.Now().Unix()))
	buf, err = proto.Marshal(&bh)
	if err != nil {
		return err
	}
	if !self.insecure {
		// Encrypt the AES key and IV with the RSA key.
		buf, err = rsa.DecryptPKCS1v15(rand.Reader, self.priv, buf)
		if err != nil {
			return err
		}
	}

	// Write out the crypto head with the IV and the blob key.
	outfile, err = os.Create(file_prefix + ".crypthead")
	if err != nil {
		return err
	}
	n, err = outfile.Write(buf)
	if err != nil {
		return err
	}
	if n < len(buf) {
		return errors.New("Short write to file")
	}

	err = outfile.Close()
	if err != nil {
		return err
	}

	// If we don't run under a blob directory, this is the end of it.
	if self.directoryClient == nil {
		return nil
	}

	// Now, report the new blob to the directory service.
	report.Server = append(report.Server, self.bindHostPort)
	report.Status = new(blubberstore.BlubberStat)
	report.Status.BlockId = make([]byte, len(blobId))
	report.Status.Checksum = make([]byte, len(bh.Checksum))
	report.Status.Size = bh.Size
	report.Status.Timestamp = bh.Timestamp

	copy(report.Status.BlockId, blobId)
	copy(report.Status.Checksum, bh.Checksum)

	// Since people need to be able to do e.g. quorum writes, we need to
	// wait here synchronously for the directory service.
	return self.directoryClient.ReportBlob(report)
}

// Extract the blubber block head for the given blob ID and return it.
func (self *blubberStore) extractBlockHead(blobId []byte) (
	bh *blubberstore.BlubberBlockHeader, err error) {
	var file_prefix string
	var data []byte

	file_prefix, _ = self.blobId2FileName(blobId)
	data, err = ioutil.ReadFile(file_prefix + ".crypthead")

	if !self.insecure {
		// Decrypt the AES key and IV with the RSA key.
		data, err = rsa.DecryptPKCS1v15(rand.Reader, self.priv, data)
		if err != nil {
			return
		}
	}

	bh = new(blubberstore.BlubberBlockHeader)
	err = proto.Unmarshal(data, bh)
	return
}

// Read the blob with the specified blob ID and return it to the caller.
// The contents will be sent as a regular HTTP response.
func (self *blubberStore) RetrieveBlob(blobId []byte, rw io.Writer) error {
	var file_prefix string
	var infile *os.File
	var bh *blubberstore.BlubberBlockHeader
	var instream cipher.StreamReader
	var aescipher cipher.Block
	var err error

	// Get the metadata.
	bh, err = self.extractBlockHead(blobId)
	if err != nil {
		return err
	}

	file_prefix, _ = self.blobId2FileName(blobId)
	infile, err = os.Open(file_prefix + ".data")
	if err != nil {
		return err
	}

	aescipher, err = aes.NewCipher(bh.BlockKey)
	if err != nil {
		infile.Close()
		return err
	}
	instream = cipher.StreamReader{
		S: cipher.NewCTR(aescipher, bh.Iv),
		R: infile,
	}

	_, err = io.Copy(rw, instream)
	if err != nil {
		infile.Close()
		return err
	}

	return infile.Close()
}

// Delete the blob with the given blob ID.
func (self *blubberStore) DeleteBlob(blobId []byte) error {
	var report blubberstore.BlockRemovalReport
	var prefix string
	var err error

	prefix, _ = self.blobId2FileName(blobId)
	err = os.Remove(prefix + ".data")
	if err != nil {
		return err
	}
	err = os.Remove(prefix + ".crypthead")
	if err != nil {
		return err
	}

	// If we don't run under a blob directory, this is the end of it.
	if self.directoryClient == nil {
		return nil
	}

	// Now report the deletion of the blob to the directory server.
	report.BlockId = make([]byte, len(blobId))
	report.Server = append(report.Server, self.bindHostPort)

	copy(report.BlockId, blobId)
	return self.directoryClient.RemoveBlobHolder(report)
}

// Get some details about the specified blob.
func (self *blubberStore) StatBlob(blobId []byte) (
	ret blubberstore.BlubberStat, err error) {
	var bh *blubberstore.BlubberBlockHeader

	bh, err = self.extractBlockHead(blobId)
	if err != nil {
		return
	}

	ret.BlockId = make([]byte, len(blobId))
	copy(ret.BlockId, blobId)

	ret.Checksum = make([]byte, len(bh.Checksum))
	copy(ret.Checksum, bh.Checksum)

	ret.Size = bh.Size
	ret.Timestamp = bh.Timestamp
	return
}

// Copy a block from the given host.
func (self *blubberStore) CopyBlob(blobId []byte, source string) error {
	var cli http.Client = http.Client{
		Transport: &http.Transport{
			TLSClientConfig: self.tlsConfig,
		},
	}
	var proto string
	var stat, remote_stat blubberstore.BlubberStat
	var resp *http.Response
	var err error

	if self.insecure {
		proto = "http"
	} else {
		proto = "https"
	}

	// Request blob from remote blubber server.
	resp, err = cli.Get(proto + "://" + source + "/" +
		hex.EncodeToString(blobId))
	if err != nil {
		return err
	}

	// Copy the remote contents into a local blob.
	err = self.StoreBlob(blobId, resp.Body, true)
	if err != nil {
		return err
	}

	// Now, let's do an integrity check.
	stat, err = self.StatBlob(blobId)
	if err != nil {
		return err
	}

	// Check the block length and signature against what the server
	// spefified in the HTTP response.
	remote_stat.BlockId, err = hex.DecodeString(
		resp.Header.Get("Content-Id"))
	remote_stat.Checksum, err = hex.DecodeString(
		resp.Header.Get("Content-Checksum"))
	remote_stat.Size = new(uint64)
	*remote_stat.Size, err = strconv.ParseUint(
		resp.Header.Get("Content-Length"), 10, 64)

	if *remote_stat.Size != *stat.Size {
		return errors.New("Size mismatch after copying block (orig: " +
			strconv.FormatUint(*remote_stat.Size, 10) + " (" +
			resp.Header.Get("Content-Length") + "), new: " +
			strconv.FormatUint(*stat.Size, 10) + ")")
	}

	if bytes.Compare(remote_stat.BlockId, stat.BlockId) != 0 {
		return errors.New("BlockId different than requested")
	}

	if bytes.Compare(remote_stat.Checksum, stat.Checksum) != 0 {
		return errors.New("Checksum mismatch (orig: " +
			hex.EncodeToString(remote_stat.Checksum) + ", new: " +
			hex.EncodeToString(stat.Checksum) + ")")
	}

	// If we reached this point we apparently copied the block successfully.
	return nil
}
