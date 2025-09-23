package file

import (
	"bytes"
	"testing"
)

func TestNewPage(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			blocksize int
		}
		wants struct {
			bufferLen int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "small block",
			args:  args{blocksize: 512},
			wants: wants{bufferLen: 512},
		},
		{
			name:  "medium block",
			args:  args{blocksize: 4096},
			wants: wants{bufferLen: 4096},
		},
		{
			name:  "large block",
			args:  args{blocksize: 8192},
			wants: wants{bufferLen: 8192},
		},
		{
			name:  "zero block",
			args:  args{blocksize: 0},
			wants: wants{bufferLen: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPage(tt.args.blocksize)
			if len(page.buf) != tt.wants.bufferLen {
				t.Errorf("NewPage() buffer length = %v, want %v", len(page.buf), tt.wants.bufferLen)
			}
		})
	}
}

func TestNewPageFromBytes(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			input []byte
		}
		wants struct {
			bufferLen int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "empty bytes",
			args:  args{input: []byte{}},
			wants: wants{bufferLen: 0},
		},
		{
			name:  "small bytes",
			args:  args{input: []byte{1, 2, 3}},
			wants: wants{bufferLen: 3},
		},
		{
			name:  "large bytes",
			args:  args{input: make([]byte, 1024)},
			wants: wants{bufferLen: 1024},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPageFromBytes(tt.args.input)
			if len(page.buf) != tt.wants.bufferLen {
				t.Errorf("NewPageFromBytes() buffer length = %v, want %v", len(page.buf), tt.wants.bufferLen)
			}
			if !bytes.Equal(page.buf, tt.args.input) {
				t.Errorf("NewPageFromBytes() buffer content mismatch")
			}
		})
	}
}

func TestPage_GetInt_SetInt(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			pageSize int
			offset   int
			value    int
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
			name:  "valid offset start",
			args:  args{pageSize: 512, offset: 0, value: 42},
			wants: wants{hasError: false},
		},
		{
			name:  "valid offset middle",
			args:  args{pageSize: 512, offset: 100, value: 123},
			wants: wants{hasError: false},
		},
		{
			name:  "valid offset near end",
			args:  args{pageSize: 512, offset: 508, value: 999},
			wants: wants{hasError: false},
		},
		{
			name:  "max int32 value",
			args:  args{pageSize: 512, offset: 0, value: 2147483647},
			wants: wants{hasError: false},
		},
		{
			name:  "positive value",
			args:  args{pageSize: 512, offset: 0, value: 456},
			wants: wants{hasError: false},
		},
		{
			name:  "zero value",
			args:  args{pageSize: 512, offset: 0, value: 0},
			wants: wants{hasError: false},
		},
		{
			name:  "out of bounds",
			args:  args{pageSize: 512, offset: 509, value: 42},
			wants: wants{hasError: true},
		},
		{
			name:  "exactly at boundary",
			args:  args{pageSize: 512, offset: 512, value: 42},
			wants: wants{hasError: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPage(tt.args.pageSize)

			// Test SetInt
			err := page.SetInt(tt.args.offset, tt.args.value)
			if (err != nil) != tt.wants.hasError {
				t.Errorf("SetInt() error = %v, wantError %v", err, tt.wants.hasError)
				return
			}

			if tt.wants.hasError {
				return
			}

			// Test GetInt
			got, err := page.GetInt(tt.args.offset)
			if err != nil {
				t.Errorf("GetInt() unexpected error = %v", err)
				return
			}
			if got != tt.args.value {
				t.Errorf("GetInt() = %v, want %v", got, tt.args.value)
			}
		})
	}
}

func TestPage_GetBytes_SetBytes(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			pageSize int
			offset   int
			data     []byte
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
			name:  "empty bytes",
			args:  args{pageSize: 512, offset: 0, data: []byte{}},
			wants: wants{hasError: false},
		},
		{
			name:  "small bytes",
			args:  args{pageSize: 512, offset: 0, data: []byte{1, 2, 3}},
			wants: wants{hasError: false},
		},
		{
			name:  "medium bytes",
			args:  args{pageSize: 512, offset: 100, data: []byte("hello world")},
			wants: wants{hasError: false},
		},
		{
			name:  "large bytes",
			args:  args{pageSize: 512, offset: 0, data: make([]byte, 500)},
			wants: wants{hasError: false},
		},
		{
			name:  "exactly fits",
			args:  args{pageSize: 512, offset: 0, data: make([]byte, 508)},
			wants: wants{hasError: false},
		},
		{
			name:  "too large",
			args:  args{pageSize: 512, offset: 0, data: make([]byte, 509)},
			wants: wants{hasError: true},
		},
		{
			name:  "offset too large",
			args:  args{pageSize: 512, offset: 509, data: []byte{1}},
			wants: wants{hasError: true},
		},
		{
			name:  "offset plus data too large",
			args:  args{pageSize: 512, offset: 500, data: []byte("hello world")},
			wants: wants{hasError: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPage(tt.args.pageSize)

			// Test SetBytes
			err := page.SetBytes(tt.args.offset, tt.args.data)
			if (err != nil) != tt.wants.hasError {
				t.Errorf("SetBytes() error = %v, wantError %v", err, tt.wants.hasError)
				return
			}

			if tt.wants.hasError {
				return
			}

			// Test GetBytes
			got, err := page.GetBytes(tt.args.offset)
			if err != nil {
				t.Errorf("GetBytes() unexpected error = %v", err)
				return
			}
			if !bytes.Equal(got, tt.args.data) {
				t.Errorf("GetBytes() = %v, want %v", got, tt.args.data)
			}
		})
	}
}

func TestPage_GetString_SetString(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			pageSize int
			offset   int
			str      string
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
			name:  "empty string",
			args:  args{pageSize: 512, offset: 0, str: ""},
			wants: wants{hasError: false},
		},
		{
			name:  "short string",
			args:  args{pageSize: 512, offset: 0, str: "hello"},
			wants: wants{hasError: false},
		},
		{
			name:  "ascii string",
			args:  args{pageSize: 512, offset: 100, str: "Hello, World!"},
			wants: wants{hasError: false},
		},
		{
			name:  "numbers",
			args:  args{pageSize: 512, offset: 0, str: "123456789"},
			wants: wants{hasError: false},
		},
		{
			name:  "special chars",
			args:  args{pageSize: 512, offset: 0, str: "!@#$%^&*()"},
			wants: wants{hasError: false},
		},
		{
			name:  "long string",
			args:  args{pageSize: 512, offset: 0, str: "This is a very long string that tests the limits of our string storage"},
			wants: wants{hasError: false},
		},
		{
			name:  "max string that fits",
			args:  args{pageSize: 512, offset: 0, str: string(make([]byte, 508))},
			wants: wants{hasError: false},
		},
		{
			name:  "string too large",
			args:  args{pageSize: 512, offset: 0, str: string(make([]byte, 509))},
			wants: wants{hasError: true},
		},
		{
			name:  "offset too large",
			args:  args{pageSize: 512, offset: 509, str: "test"},
			wants: wants{hasError: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPage(tt.args.pageSize)

			// Test SetString
			err := page.SetString(tt.args.offset, tt.args.str)
			if (err != nil) != tt.wants.hasError {
				t.Errorf("SetString() error = %v, wantError %v", err, tt.wants.hasError)
				return
			}

			if tt.wants.hasError {
				return
			}

			// Test GetString
			got, err := page.GetString(tt.args.offset)
			if err != nil {
				t.Errorf("GetString() unexpected error = %v", err)
				return
			}
			if got != tt.args.str {
				t.Errorf("GetString() = %v, want %v", got, tt.args.str)
			}
		})
	}
}

func TestMaxLength(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			strlen int
		}
		wants struct {
			result int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "zero length",
			args:  args{strlen: 0},
			wants: wants{result: 4},
		},
		{
			name:  "short string",
			args:  args{strlen: 5},
			wants: wants{result: 9},
		},
		{
			name:  "medium string",
			args:  args{strlen: 100},
			wants: wants{result: 104},
		},
		{
			name:  "long string",
			args:  args{strlen: 1000},
			wants: wants{result: 1004},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MaxLength(tt.args.strlen)
			if got != tt.wants.result {
				t.Errorf("MaxLength() = %v, want %v", got, tt.wants.result)
			}
		})
	}
}

func TestPage_Buffer(t *testing.T) {
	t.Parallel()

	type (
		args struct {
			pageSize int
		}
		wants struct {
			bufferLen int
		}
	)

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "small page",
			args:  args{pageSize: 512},
			wants: wants{bufferLen: 512},
		},
		{
			name:  "large page",
			args:  args{pageSize: 4096},
			wants: wants{bufferLen: 4096},
		},
		{
			name:  "zero page",
			args:  args{pageSize: 0},
			wants: wants{bufferLen: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page := NewPage(tt.args.pageSize)
			buf := page.Buffer()
			if len(buf) != tt.wants.bufferLen {
				t.Errorf("Buffer() length = %v, want %v", len(buf), tt.wants.bufferLen)
			}
			// Verify it's the same underlying buffer
			if cap(buf) != cap(page.buf) {
				t.Errorf("Buffer() returns different buffer than internal")
			}
		})
	}
}
