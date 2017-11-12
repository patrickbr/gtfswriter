// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"fmt"
)

type writeError struct {
	filename string
	msg      string
}

func (e writeError) Error() string {
	return fmt.Sprintf("%s - %s", e.filename, e.msg)
}
