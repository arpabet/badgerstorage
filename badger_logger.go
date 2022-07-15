/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package badgerstorage

import (
	"github.com/dgraph-io/badger/v2"
	"log"
)

type loggerAdapter struct {
	debug bool
}

func (t *loggerAdapter) Errorf(format string, args ...interface{}) {
	log.Printf("ERROR "+format, args...)
}

func (t *loggerAdapter) Warningf(format string, args ...interface{}) {
	log.Printf("WARN "+format, args...)
}

func (t *loggerAdapter) Infof(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}

func (t *loggerAdapter) Debugf(format string, args ...interface{}) {
	if t.debug {
		log.Printf("DEBUG "+format, args...)
	}
}

func NewLogger(debug bool) badger.Logger {
	return &loggerAdapter{debug: debug}
}
