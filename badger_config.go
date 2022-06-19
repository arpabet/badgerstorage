/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package badgerstorage

import (
	"errors"
	"go.uber.org/zap"
	"os"
	"time"
)

var (
	ValueLogMaxEntries  = uint32(1024 * 1024 * 1024)
	KeyRotationDuration = time.Hour * 24 * 7

	KeySize = 32

	DefaultDirPerm = os.FileMode(0775)

	MaxPendingWrites = 4096

	ErrInvalidKeySize   = errors.New("invalid key size")
	ErrCanceled         = errors.New("operation was canceled")
	ErrDatabaseExist    = errors.New("database exist")
	ErrDatabaseNotExist = errors.New("database not exist")
	ErrItemNotExist     = errors.New("item not exist")
)

type BadgerAction uint8

const (
	DeleteIfExist BadgerAction = iota
	CreateIfNotExist
)

type BadgerConfig struct {
	DataDir    string
	Action     BadgerAction
	StorageKey []byte // optional
	UseZSTD    bool
	TruncateDB bool
	Debug      bool
	Log        *zap.Logger // optional
	DirPerm    os.FileMode
}
