package file

import "fmt"

// BlockId identifies a specific block by filename and block number.
type BlockId struct {
	filename string
	blknum   int
}

// NewBlockId creates a new BlockId.
func NewBlockId(filename string, blknum int) BlockId {
	return BlockId{
		filename: filename,
		blknum:   blknum,
	}
}

// FileName returns the filename of the block.
func (b BlockId) FileName() string {
	return b.filename
}

// Number returns the block number.
func (b BlockId) Number() int {
	return b.blknum
}

// String returns a string representation of the BlockId.
func (b BlockId) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blknum)
}
