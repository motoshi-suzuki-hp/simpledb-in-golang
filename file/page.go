package file

import (
	"encoding/binary"
	"errors"
)

// Page holds the contents of a disk block.
// Uses big-endian byte order for compatibility with Java ByteBuffer.
type Page struct {
	buf []byte
}

// bytesPerChar represents bytes per character for US-ASCII encoding.
const bytesPerChar = 1

// NewPage creates a new page with the specified block size.
func NewPage(blocksize int) *Page {
	return &Page{buf: make([]byte, blocksize)}
}

// NewPageFromBytes creates a page that wraps the given byte slice.
func NewPageFromBytes(b []byte) *Page {
	return &Page{buf: b}
}

// Buffer returns the underlying byte buffer.
func (p *Page) Buffer() []byte {
	return p.buf
}

// GetInt reads a 32-bit integer from the specified offset.
func (p *Page) GetInt(offset int) (int, error) {
	if offset+4 > len(p.buf) {
		return 0, errors.New("GetInt: out of bounds")
	}
	return int(binary.BigEndian.Uint32(p.buf[offset:])), nil
}

// SetInt writes a 32-bit integer to the specified offset.
func (p *Page) SetInt(offset int, v int) error {
	if offset+4 > len(p.buf) {
		return errors.New("SetInt: out of bounds")
	}
	binary.BigEndian.PutUint32(p.buf[offset:], uint32(v))
	return nil
}

// GetBytes reads a byte array from the specified offset.
// The format is: 4-byte length followed by the actual bytes.
func (p *Page) GetBytes(offset int) ([]byte, error) {
	if offset+4 > len(p.buf) {
		return nil, errors.New("GetBytes(len): out of bounds")
	}
	length := int(binary.BigEndian.Uint32(p.buf[offset:]))
	start := offset + 4
	end := start + length
	if end > len(p.buf) {
		return nil, errors.New("GetBytes(data): out of bounds")
	}
	out := make([]byte, length)
	copy(out, p.buf[start:end])
	return out, nil
}

// SetBytes writes a byte array to the specified offset.
// The format is: 4-byte length followed by the actual bytes.
func (p *Page) SetBytes(offset int, b []byte) error {
	if offset+4+len(b) > len(p.buf) {
		return errors.New("SetBytes: out of bounds")
	}
	binary.BigEndian.PutUint32(p.buf[offset:], uint32(len(b)))
	copy(p.buf[offset+4:], b)
	return nil
}

// GetString reads a string from the specified offset.
func (p *Page) GetString(offset int) (string, error) {
	b, err := p.GetBytes(offset)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// SetString writes a string to the specified offset.
func (p *Page) SetString(offset int, s string) error {
	return p.SetBytes(offset, []byte(s))
}

// MaxLength returns the maximum space needed to store a string of the given length.
// Includes 4 bytes for length prefix plus the string bytes.
func MaxLength(strlen int) int {
	return 4 + strlen*bytesPerChar
}
