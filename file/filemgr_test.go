package file

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewFileMgr(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			setupDir    func(string) error
			dbDirectory string
			blocksize   int
		}
		wants struct {
			isNew    bool
			hasError bool
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "new directory",
			args: args{
				setupDir:    func(dir string) error { return nil }, // Don't create directory
				dbDirectory: "testdb_new",
				blocksize:   512,
			},
			wants: wants{isNew: true, hasError: false},
		},
		{
			name: "existing empty directory",
			args: args{
				setupDir:    func(dir string) error { return os.MkdirAll(dir, 0755) },
				dbDirectory: "testdb_existing",
				blocksize:   1024,
			},
			wants: wants{isNew: false, hasError: false},
		},
		{
			name: "existing directory with temp files",
			args: args{
				setupDir: func(dir string) error {
					if err := os.MkdirAll(dir, 0755); err != nil {
						return err
					}
					// Create temp files that should be cleaned up
					tempFile := filepath.Join(dir, "temp123.dat")
					return os.WriteFile(tempFile, []byte("test"), 0644)
				},
				dbDirectory: "testdb_temp",
				blocksize:   2048,
			},
			wants: wants{isNew: false, hasError: false},
		},
		{
			name: "path exists but is file",
			args: args{
				setupDir: func(path string) error {
					return os.WriteFile(path, []byte("test"), 0644)
				},
				dbDirectory: "testfile.txt",
				blocksize:   512,
			},
			wants: wants{isNew: false, hasError: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup
			testDir := filepath.Join(os.TempDir(), tt.args.dbDirectory)
			defer os.RemoveAll(testDir)

			if err := tt.args.setupDir(testDir); err != nil {
				t.Fatalf("Setup failed: %v", err)
			}

			// Test
			fm, err := NewFileMgr(testDir, tt.args.blocksize)
			if (err != nil) != tt.wants.hasError {
				t.Errorf("NewFileMgr() error = %v, wantError %v", err, tt.wants.hasError)
				return
			}

			if tt.wants.hasError {
				return
			}

			if fm.IsNew() != tt.wants.isNew {
				t.Errorf("NewFileMgr().IsNew() = %v, want %v", fm.IsNew(), tt.wants.isNew)
			}

			if fm.BlockSize() != tt.args.blocksize {
				t.Errorf("NewFileMgr().BlockSize() = %v, want %v", fm.BlockSize(), tt.args.blocksize)
			}

			// Check that temp files were cleaned up
			if !tt.wants.isNew {
				entries, _ := os.ReadDir(testDir)
				for _, e := range entries {
					if strings.HasPrefix(e.Name(), "temp") {
						t.Errorf("Temp file %s was not cleaned up", e.Name())
					}
				}
			}
		})
	}
}

func TestFileMgr_Length(t *testing.T) {
	// Note: Cannot use t.Parallel() here because subtests share the same FileMgr instance

	type (
		args struct {
			filename  string
			setupFile func(string) error
		}
		wants struct {
			length   int
			hasError bool
		}
	)

	testDir := filepath.Join(os.TempDir(), "testdb_length")
	defer os.RemoveAll(testDir)

	fm, err := NewFileMgr(testDir, 512)
	if err != nil {
		t.Fatalf("NewFileMgr() failed: %v", err)
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "non-existent file",
			args: args{
				filename:  "nonexistent.db",
				setupFile: func(string) error { return nil },
			},
			wants: wants{length: 0, hasError: false},
		},
		{
			name: "empty file",
			args: args{
				filename: "empty.db",
				setupFile: func(path string) error {
					return os.WriteFile(path, []byte{}, 0644)
				},
			},
			wants: wants{length: 0, hasError: false},
		},
		{
			name: "one block file",
			args: args{
				filename: "oneblock.db",
				setupFile: func(path string) error {
					return os.WriteFile(path, make([]byte, 512), 0644)
				},
			},
			wants: wants{length: 1, hasError: false},
		},
		{
			name: "multiple blocks file",
			args: args{
				filename: "multiblock.db",
				setupFile: func(path string) error {
					return os.WriteFile(path, make([]byte, 1536), 0644) // 3 blocks
				},
			},
			wants: wants{length: 3, hasError: false},
		},
		{
			name: "partial block file",
			args: args{
				filename: "partial.db",
				setupFile: func(path string) error {
					return os.WriteFile(path, make([]byte, 600), 0644) // 1.17 blocks
				},
			},
			wants: wants{length: 1, hasError: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := filepath.Join(testDir, tt.args.filename)
			if err := tt.args.setupFile(filePath); err != nil {
				t.Fatalf("Setup failed: %v", err)
			}

			got, err := fm.Length(tt.args.filename)
			if (err != nil) != tt.wants.hasError {
				t.Errorf("FileMgr.Length() error = %v, wantError %v", err, tt.wants.hasError)
				return
			}

			if got != tt.wants.length {
				t.Errorf("FileMgr.Length() = %v, want %v", got, tt.wants.length)
			}
		})
	}
}

func TestFileMgr_ReadWrite(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "testdb_readwrite")
	defer os.RemoveAll(testDir)

	blocksize := 512
	fm, err := NewFileMgr(testDir, blocksize)
	if err != nil {
		t.Fatalf("NewFileMgr() failed: %v", err)
	}

	tests := []struct {
		name      string
		filename  string
		blockNum  int
		pageData  []byte
		wantError bool
	}{
		{
			"write and read first block",
			"test1.db",
			0,
			[]byte("Hello, World!"),
			false,
		},
		{
			"write and read second block",
			"test2.db",
			1,
			[]byte("Second block data"),
			false,
		},
		{
			"write full block",
			"test3.db",
			0,
			make([]byte, blocksize),
			false,
		},
		{
			"empty data",
			"test4.db",
			0,
			[]byte{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create page with test data
			page := NewPage(blocksize)
			if len(tt.pageData) > 0 {
				copy(page.buf, tt.pageData)
			}

			blockId := NewBlockId(tt.filename, tt.blockNum)

			// Test Write
			err := fm.Write(blockId, page)
			if (err != nil) != tt.wantError {
				t.Errorf("FileMgr.Write() error = %v, wantError %v", err, tt.wantError)
				return
			}

			if tt.wantError {
				return
			}

			// Test Read
			readPage := NewPage(blocksize)
			err = fm.Read(blockId, readPage)
			if err != nil {
				t.Errorf("FileMgr.Read() unexpected error = %v", err)
				return
			}

			// Compare the relevant portion of the data
			expectedLen := len(tt.pageData)
			if expectedLen == 0 {
				expectedLen = blocksize // For empty data, check entire block
			}

			for i := 0; i < expectedLen && i < len(tt.pageData); i++ {
				if readPage.buf[i] != tt.pageData[i] {
					t.Errorf("Read data mismatch at position %d: got %v, want %v", i, readPage.buf[i], tt.pageData[i])
					break
				}
			}
		})
	}
}

func TestFileMgr_ReadWrite_Errors(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			pageSize  int
			operation string
		}
		wants struct {
			hasError bool
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "wrong page size - write",
			args:  args{pageSize: 256, operation: "write"},
			wants: wants{hasError: true},
		},
		{
			name:  "wrong page size - read",
			args:  args{pageSize: 1024, operation: "read"},
			wants: wants{hasError: true},
		},
		{
			name:  "correct page size - write",
			args:  args{pageSize: 512, operation: "write"},
			wants: wants{hasError: false},
		},
		{
			name:  "correct page size - read from empty file",
			args:  args{pageSize: 512, operation: "read"},
			wants: wants{hasError: true}, // EOF error expected when reading from non-existent block
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testDir := filepath.Join(os.TempDir(), "testdb_errors_"+tt.name)
			defer os.RemoveAll(testDir)

			blocksize := 512
			fm, err := NewFileMgr(testDir, blocksize)
			if err != nil {
				t.Fatalf("NewFileMgr() failed: %v", err)
			}

			page := NewPage(tt.args.pageSize)
			blockId := NewBlockId("test.db", 0)

			var opErr error
			if tt.args.operation == "write" {
				opErr = fm.Write(blockId, page)
			} else {
				opErr = fm.Read(blockId, page)
			}

			if (opErr != nil) != tt.wants.hasError {
				t.Errorf("FileMgr.%s() error = %v, wantError %v", tt.args.operation, opErr, tt.wants.hasError)
			}
		})
	}
}

func TestFileMgr_Append(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "testdb_append")
	defer os.RemoveAll(testDir)

	blocksize := 512
	fm, err := NewFileMgr(testDir, blocksize)
	if err != nil {
		t.Fatalf("NewFileMgr() failed: %v", err)
	}

	tests := []struct {
		name             string
		filename         string
		initialBlocks    int
		expectedBlockNum int
	}{
		{"append to new file", "new.db", 0, 0},
		{"append to existing file", "existing.db", 2, 2},
		{"append multiple times", "multi.db", 0, 0}, // Will test multiple appends
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup initial blocks if needed
			if tt.initialBlocks > 0 {
				for i := range tt.initialBlocks {
					page := NewPage(blocksize)
					blockId := NewBlockId(tt.filename, i)
					if err := fm.Write(blockId, page); err != nil {
						t.Fatalf("Setup write failed: %v", err)
					}
				}
			}

			// Test single append
			blockId, err := fm.Append(tt.filename)
			if err != nil {
				t.Errorf("FileMgr.Append() error = %v", err)
				return
			}

			if blockId.FileName() != tt.filename {
				t.Errorf("Append() filename = %v, want %v", blockId.FileName(), tt.filename)
			}

			if blockId.Number() != tt.expectedBlockNum {
				t.Errorf("Append() block number = %v, want %v", blockId.Number(), tt.expectedBlockNum)
			}

			// Verify the file length increased
			expectedLength := tt.initialBlocks + 1
			length, err := fm.Length(tt.filename)
			if err != nil {
				t.Errorf("Length() after append failed: %v", err)
			}
			if length != expectedLength {
				t.Errorf("Length() after append = %v, want %v", length, expectedLength)
			}

			// For multi test, append multiple times
			if tt.name == "append multiple times" {
				for i := 1; i < 3; i++ {
					blockId, err := fm.Append(tt.filename)
					if err != nil {
						t.Errorf("Multiple append %d failed: %v", i, err)
						continue
					}
					if blockId.Number() != i {
						t.Errorf("Multiple append %d block number = %v, want %v", i, blockId.Number(), i)
					}
				}
			}
		})
	}
}

func TestFileMgr_Concurrency(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "testdb_concurrent")
	defer os.RemoveAll(testDir)

	blocksize := 512
	fm, err := NewFileMgr(testDir, blocksize)
	if err != nil {
		t.Fatalf("NewFileMgr() failed: %v", err)
	}

	// Test concurrent operations don't crash
	filename := "concurrent.db"
	numGoroutines := 10

	done := make(chan bool, numGoroutines)

	for i := range numGoroutines {
		go func(id int) {
			defer func() { done <- true }()

			// Each goroutine appends one block
			blockId, err := fm.Append(filename)
			if err != nil {
				t.Errorf("Concurrent append %d failed: %v", id, err)
				return
			}

			// Write some data
			page := NewPage(blocksize)
			page.SetInt(0, id)
			if err := fm.Write(blockId, page); err != nil {
				t.Errorf("Concurrent write %d failed: %v", id, err)
				return
			}

			// Read it back
			readPage := NewPage(blocksize)
			if err := fm.Read(blockId, readPage); err != nil {
				t.Errorf("Concurrent read %d failed: %v", id, err)
				return
			}

			value, err := readPage.GetInt(0)
			if err != nil || value != id {
				t.Errorf("Concurrent read verification %d failed: got %v, want %v", id, value, id)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for range numGoroutines {
		<-done
	}

	// Verify final file length
	length, err := fm.Length(filename)
	if err != nil {
		t.Errorf("Final length check failed: %v", err)
	}
	if length != numGoroutines {
		t.Errorf("Final length = %v, want %v", length, numGoroutines)
	}
}
