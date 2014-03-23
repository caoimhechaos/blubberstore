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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"code.google.com/p/goprotobuf/proto"
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
	blob_path string
	insecure  bool
	priv      *rsa.PrivateKey
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

	parent_dir = self.blob_path + "/" + first_dir + "/" + second_dir
	return parent_dir + "/" + path_name, parent_dir
}

// Create a new blob with the given blobId, or overwrite an existing one.
// The contents of the blob will be read from input.
func (self *blubberStore) StoreBlob(blobId []byte, input io.Reader) error {
	var outstream cipher.StreamWriter
	var bh BlubberBlockHeader
	var aescipher cipher.Block
	var cksum hash.Hash = sha256.New()
	var parent_dir, file_prefix string
	var counter CountingWriter
	var outfile *os.File
	var buf []byte
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
	outfile, err = os.Create(file_prefix + ".data")
	if err != nil {
		return err
	}
	aescipher, err = aes.NewCipher(bh.BlockKey)
	if err != nil {
		return err
	}
	outstream = cipher.StreamWriter{
		S: cipher.NewCTR(aescipher, bh.Iv),
		W: io.MultiWriter(outfile, cksum, &counter),
	}
	_, err = io.Copy(outstream, input)
	if err != nil {
		return err
	}
	err = outfile.Close()
	if err != nil {
		return err
	}

	// Fill in the last bits of the blob header.
	bh.Checksum = cksum.Sum(bh.Checksum)
	bh.Size = new(uint64)
	*bh.Size = counter.BytesWritten()
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
	return outstream.Close()
}

// Extract the blubber block head for the given blob ID and return it.
func (self *blubberStore) extractBlockHead(blobId []byte) (
	bh *BlubberBlockHeader, err error) {
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

	bh = new(BlubberBlockHeader)
	err = proto.Unmarshal(data, bh)
	return
}

// Read the blob with the specified blob ID and return it to the caller.
// The contents will be sent as a regular HTTP response.
func (self *blubberStore) RetrieveBlob(blobId []byte, rw io.Writer) error {
	var file_prefix string
	var infile *os.File
	var bh *BlubberBlockHeader
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
	return errors.New("Not yet implemented")
}

// Get some details about the specified blob.
func (self *blubberStore) StatBlob(blobId []byte) (
	ret BlubberStat, err error) {
	var bh *BlubberBlockHeader

	bh, err = self.extractBlockHead(blobId)
	if err != nil {
		return
	}

	ret.BlockId = make([]byte, len(blobId))
	copy(ret.BlockId, blobId)

	ret.Checksum = make([]byte, len(bh.Checksum))
	copy(ret.Checksum, bh.Checksum)

	ret.Size = bh.Size
	return
}
