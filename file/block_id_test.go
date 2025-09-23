package file

import (
	"testing"
)

func TestNewBlockId(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			filename string
			blknum   int
		}
		wants struct {
			filename string
			number   int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "basic case",
			args:  args{filename: "test.db", blknum: 0},
			wants: wants{filename: "test.db", number: 0},
		},
		{
			name:  "with path",
			args:  args{filename: "data/users.db", blknum: 42},
			wants: wants{filename: "data/users.db", number: 42},
		},
		{
			name:  "empty filename",
			args:  args{filename: "", blknum: 0},
			wants: wants{filename: "", number: 0},
		},
		{
			name:  "negative block",
			args:  args{filename: "file.db", blknum: -1},
			wants: wants{filename: "file.db", number: -1},
		},
		{
			name:  "large block number",
			args:  args{filename: "big.db", blknum: 999999},
			wants: wants{filename: "big.db", number: 999999},
		},
		{
			name:  "special chars in filename",
			args:  args{filename: "file@#$.db", blknum: 123},
			wants: wants{filename: "file@#$.db", number: 123},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			blockId := NewBlockId(tt.args.filename, tt.args.blknum)
			if blockId.FileName() != tt.wants.filename {
				t.Errorf("NewBlockId().FileName() = %v, want %v", blockId.FileName(), tt.wants.filename)
			}
			if blockId.Number() != tt.wants.number {
				t.Errorf("NewBlockId().Number() = %v, want %v", blockId.Number(), tt.wants.number)
			}
		})
	}
}

func TestBlockId_FileName(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			filename string
			blknum   int
		}
		wants struct {
			filename string
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "simple filename",
			args:  args{filename: "test.db", blknum: 0},
			wants: wants{filename: "test.db"},
		},
		{
			name:  "path filename",
			args:  args{filename: "data/test.db", blknum: 1},
			wants: wants{filename: "data/test.db"},
		},
		{
			name:  "empty filename",
			args:  args{filename: "", blknum: 2},
			wants: wants{filename: ""},
		},
		{
			name:  "special chars",
			args:  args{filename: "file-name_123.db", blknum: 3},
			wants: wants{filename: "file-name_123.db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			blockId := NewBlockId(tt.args.filename, tt.args.blknum)
			got := blockId.FileName()
			if got != tt.wants.filename {
				t.Errorf("BlockId.FileName() = %v, want %v", got, tt.wants.filename)
			}
		})
	}
}

func TestBlockId_Number(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			filename string
			blknum   int
		}
		wants struct {
			number int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "zero block",
			args:  args{filename: "test.db", blknum: 0},
			wants: wants{number: 0},
		},
		{
			name:  "positive block",
			args:  args{filename: "test.db", blknum: 42},
			wants: wants{number: 42},
		},
		{
			name:  "negative block",
			args:  args{filename: "test.db", blknum: -1},
			wants: wants{number: -1},
		},
		{
			name:  "large block",
			args:  args{filename: "test.db", blknum: 999999},
			wants: wants{number: 999999},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			blockId := NewBlockId(tt.args.filename, tt.args.blknum)
			got := blockId.Number()
			if got != tt.wants.number {
				t.Errorf("BlockId.Number() = %v, want %v", got, tt.wants.number)
			}
		})
	}
}

func TestBlockId_String(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			filename string
			blknum   int
		}
		wants struct {
			str string
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "basic case",
			args:  args{filename: "test.db", blknum: 0},
			wants: wants{str: "[file test.db, block 0]"},
		},
		{
			name:  "with path",
			args:  args{filename: "data/users.db", blknum: 42},
			wants: wants{str: "[file data/users.db, block 42]"},
		},
		{
			name:  "empty filename",
			args:  args{filename: "", blknum: 1},
			wants: wants{str: "[file , block 1]"},
		},
		{
			name:  "negative block",
			args:  args{filename: "file.db", blknum: -1},
			wants: wants{str: "[file file.db, block -1]"},
		},
		{
			name:  "large numbers",
			args:  args{filename: "big.db", blknum: 999999},
			wants: wants{str: "[file big.db, block 999999]"},
		},
		{
			name:  "special chars",
			args:  args{filename: "file@#$.db", blknum: 123},
			wants: wants{str: "[file file@#$.db, block 123]"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			blockId := NewBlockId(tt.args.filename, tt.args.blknum)
			got := blockId.String()
			if got != tt.wants.str {
				t.Errorf("BlockId.String() = %v, want %v", got, tt.wants.str)
			}
		})
	}
}

func TestBlockId_Equality(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			b1 BlockId
			b2 BlockId
		}
		wants struct {
			equal bool
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "identical blocks",
			args: args{
				b1: NewBlockId("test.db", 0),
				b2: NewBlockId("test.db", 0),
			},
			wants: wants{equal: true},
		},
		{
			name: "different filenames",
			args: args{
				b1: NewBlockId("test1.db", 0),
				b2: NewBlockId("test2.db", 0),
			},
			wants: wants{equal: false},
		},
		{
			name: "different block numbers",
			args: args{
				b1: NewBlockId("test.db", 0),
				b2: NewBlockId("test.db", 1),
			},
			wants: wants{equal: false},
		},
		{
			name: "both different",
			args: args{
				b1: NewBlockId("test1.db", 0),
				b2: NewBlockId("test2.db", 1),
			},
			wants: wants{equal: false},
		},
		{
			name: "empty filenames",
			args: args{
				b1: NewBlockId("", 0),
				b2: NewBlockId("", 0),
			},
			wants: wants{equal: true},
		},
		{
			name: "negative blocks",
			args: args{
				b1: NewBlockId("test.db", -1),
				b2: NewBlockId("test.db", -1),
			},
			wants: wants{equal: true},
		},
		{
			name: "large blocks",
			args: args{
				b1: NewBlockId("test.db", 999999),
				b2: NewBlockId("test.db", 999999),
			},
			wants: wants{equal: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.args.b1 == tt.args.b2
			if got != tt.wants.equal {
				t.Errorf("BlockId equality (%v == %v) = %v, want %v", tt.args.b1, tt.args.b2, got, tt.wants.equal)
			}
		})
	}
}
