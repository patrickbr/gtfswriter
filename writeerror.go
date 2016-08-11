// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"fmt"
)

type WriteError struct {
	filename string
	msg      string
}

func (e WriteError) Error() string {
	return fmt.Sprintf("%s:%d - %s", e.filename, e.msg)
}
