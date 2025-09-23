package file

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileMgr handles interaction with the OS file system.
type FileMgr struct {
	dbDirectory string
	blocksize   int
	isNew       bool

	mu        sync.Mutex
	openFiles map[string]*os.File
}

// NewFileMgr creates a new file manager for the specified directory and block size.
func NewFileMgr(dbDirectory string, blocksize int) (*FileMgr, error) {
	fi, err := os.Stat(dbDirectory)
	isNew := os.IsNotExist(err)
	if isNew {
		if mkErr := os.MkdirAll(dbDirectory, 0o755); mkErr != nil {
			return nil, mkErr
		}
	} else if err == nil && !fi.IsDir() {
		return nil, fmt.Errorf("%s exists and is not a directory", dbDirectory)
	}

	// Remove leftover temporary files
	entries, _ := os.ReadDir(dbDirectory)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "temp") {
			_ = os.Remove(filepath.Join(dbDirectory, e.Name()))
		}
	}

	return &FileMgr{
		dbDirectory: dbDirectory,
		blocksize:   blocksize,
		isNew:       isNew,
		openFiles:   make(map[string]*os.File),
	}, nil
}

// IsNew returns true if this is a new database.
func (fm *FileMgr) IsNew() bool { return fm.isNew }

// BlockSize returns the block size in bytes.
func (fm *FileMgr) BlockSize() int { return fm.blocksize }

// Length returns the number of blocks in the specified file.
func (fm *FileMgr) Length(filename string) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	f, err := fm.getFile(filename)
	if err != nil {
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return int(fi.Size() / int64(fm.blocksize)), nil
}

// Read reads a block into the specified page.
func (fm *FileMgr) Read(blk BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if len(p.buf) != fm.blocksize {
		return errors.New("Read: page size != blocksize")
	}
	f, err := fm.getFile(blk.FileName())
	if err != nil {
		return err
	}
	offset := int64(blk.Number() * fm.blocksize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	_, err = io.ReadFull(f, p.buf)
	return err
}

// Write writes a page to the specified block.
func (fm *FileMgr) Write(blk BlockId, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if len(p.buf) != fm.blocksize {
		return errors.New("Write: page size != blocksize")
	}
	f, err := fm.getFile(blk.FileName())
	if err != nil {
		return err
	}
	offset := int64(blk.Number() * fm.blocksize)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(p.buf); err != nil {
		return err
	}
	// Sync to ensure data is written to disk immediately
	return f.Sync()
}

// Append adds a new zero-filled block to the end of the file and returns its BlockId.
func (fm *FileMgr) Append(filename string) (BlockId, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	f, err := fm.getFile(filename)
	if err != nil {
		return BlockId{}, err
	}
	// Calculate new block number
	fi, err := f.Stat()
	if err != nil {
		return BlockId{}, err
	}
	newBlkNum := int(fi.Size() / int64(fm.blocksize))
	blk := NewBlockId(filename, newBlkNum)

	// Write zero-filled block
	zero := make([]byte, fm.blocksize)
	if _, err := f.WriteAt(zero, int64(newBlkNum*fm.blocksize)); err != nil {
		return BlockId{}, err
	}
	if err := f.Sync(); err != nil {
		return BlockId{}, err
	}
	return blk, nil
}

// getFile returns an open file handle, opening it if necessary.
func (fm *FileMgr) getFile(filename string) (*os.File, error) {
	if f, ok := fm.openFiles[filename]; ok {
		return f, nil
	}
	full := filepath.Join(fm.dbDirectory, filename)
	// Open for read/write, create if not exists
	f, err := os.OpenFile(full, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	fm.openFiles[filename] = f
	return f, nil
}
