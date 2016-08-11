// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"encoding/csv"
	"io"
)

type CsvWriter struct {
	writer      *csv.Writer
	headers     []string
	headerUsage []bool
	lines       [][]string
}

func NewCsvWriter(file io.Writer) CsvWriter {
	writer := csv.NewWriter(file)
	p := CsvWriter{
		writer:      writer,
		headers:     make([]string, 0),
		headerUsage: make([]bool, 0),
		lines:       make([][]string, 0),
	}

	return p
}

func (p *CsvWriter) SetHeader(val []string, required []string) {
	p.headerUsage = make([]bool, len(val))
	p.headers = val
	for _, req := range required {
		for i, v := range p.headers {
			if v == req {
				p.headerUsage[i] = true
			}
		}
	}
}

func (p *CsvWriter) WriteCsvLine(val []string) {
	p.lines = append(p.lines, val)

	for i, v := range val {
		if len(v) > 0 {
			p.headerUsage[i] = true
		}
	}
}

func (p *CsvWriter) Flush() {
	if len(p.lines) == 0 {
		return
	}

	// mask header
	p.maskLine(&p.headers)

	// write header
	e := p.writer.Write(p.headers)

	if e != nil {
		panic(e.Error())
	}

	for _, v := range p.lines {
		p.maskLine(&v)
		e := p.writer.Write(v)

		if e != nil {
			panic(e.Error())
		}
	}
	p.writer.Flush()
}

func (p *CsvWriter) maskLine(val *[]string) {
	j := 0
	for i, h := range p.headerUsage {
		if !h {
			*val = append((*val)[:(i-j)], (*val)[(i-j)+1:]...)
			j = j + 1
		}
	}
}
