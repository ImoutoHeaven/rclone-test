package readers

import "os"

// UnderlyingFile is an optional interface that readers can implement
// to expose an underlying *os.File for backends that can take
// advantage of random access on local files.
type UnderlyingFile interface {
	UnderlyingFile() *os.File
}

