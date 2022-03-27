// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"encoding/csv"
	"io"
	"sort"
)

// Lines describes a slice of slice-encoded CSV lines
type Lines [][]string

// SortedLines is a Lines object extended by information
// on the sorting depth (1 = sort by first column, 2 =
// sort by first and second column, and so on)
type SortedLines struct {
	Lines     Lines
	SortDepth int
}

func (l SortedLines) Len() int      { return len(l.Lines) }
func (l SortedLines) Swap(i, j int) { l.Lines[i], l.Lines[j] = l.Lines[j], l.Lines[i] }
func (l SortedLines) Less(i, j int) bool {
	for a := 0; a < l.SortDepth && a < len(l.Lines[i]); a++ {
		if l.Lines[i][a] < l.Lines[j][a] {
			return true
		} else if l.Lines[i][a] != l.Lines[j][a] {
			return false
		}
	}
	return false
}

// A CsvWriter is a wrapper around csv.Writer
type CsvWriter struct {
	writer           *csv.Writer
	headers          []string
	headersMap       map[string]int
	headerUsage      []bool
	headerUsageCount int
	lines            Lines
	order            map[string]int
}

// NewCsvWriter returns a new CsvWriter instance
func NewCsvWriter(file io.Writer) CsvWriter {
	writer := csv.NewWriter(file)
	p := CsvWriter{
		writer:           writer,
		headers:          make([]string, 0),
		headersMap:       make(map[string]int, 0),
		headerUsage:      make([]bool, 0),
		headerUsageCount: 0,
		lines:            make(Lines, 0),
		order:            make(map[string]int, 0),
	}

	return p
}

// SetHeader sets the header for this CSV file
func (p *CsvWriter) SetHeader(val []string, required []string) {
	p.headerUsage = make([]bool, len(val))
	p.headers = val
	for i, h := range val {
		p.headersMap[h] = i
	}

	for _, req := range required {
		for i, v := range p.headers {
			if v == req {
				p.headerUsage[i] = true
			}
		}
	}
}

func (p *CsvWriter) SetOrder(order []string) {
	a := 0
	for _, name := range order {
		// don't write order for headers we don't use!
		if _, ok := p.headersMap[name]; ok {
			p.order[name] = a
			a = a + 1
		}
	}
}

// WriteCsvLine writes a single slice of values to the CSV file
func (p *CsvWriter) WriteCsvLine(val []string) {
	p.lines = append(p.lines, val)

	p.HeaderUsage(val)
}

// WriteCsvLineRaw writes a single slice of values to the CSV file
func (p *CsvWriter) WriteCsvLineRaw(val []string) {
	p.maskLine(&val)
	e := p.writer.Write(val)

	if e != nil {
		panic(e.Error())
	}
}

// HeaderUsage updates the header usage for a single row
func (p *CsvWriter) HeaderUsage(val []string) {
	for i, v := range val {
		if len(v) > 0 {
			p.headerUsage[i] = true
		}
	}
}

// SortByCols sorts the current line cache by depth
func (p *CsvWriter) SortByCols(depth int) {
	sort.Sort(SortedLines{p.lines, depth})
}

// Flush the current line cache into the CSV file
func (p *CsvWriter) Flush() {
	if len(p.lines) == 0 {
		e := p.writer.Write(p.headers)
		p.writer.Flush()
		if e != nil {
			panic(e.Error())
		}
		return
	}

	p.WriteHeader()

	for _, v := range p.lines {
		p.WriteCsvLineRaw(v)
	}
	p.FlushFile()
	p.lines = nil
}

func (p *CsvWriter) WriteHeader() {
	// mask header
	headerCp := append([]string(nil), p.headers...)
	p.maskLine(&headerCp)

	// write header
	e := p.writer.Write(headerCp)

	if e != nil {
		panic(e.Error())
	}

}

// Flush the current line cache into the CSV file
func (p *CsvWriter) FlushFile() {
	p.writer.Flush()
}

func (p *CsvWriter) maskLine(val *[]string) {
	if len(p.order) > 0 {

		a := make([]string, len(p.order))
		for i, h := range p.headerUsage {
			if order, ok := p.order[p.headers[i]]; ok {
				a[order] = (*val)[i]
			} else if h {
				a = append(a, (*val)[i])
			}
		}

		*val = append([]string(nil), a...)
		return
	}

	j := 0
	for i, h := range p.headerUsage {
		if !h {
			*val = append((*val)[:(i-j)], (*val)[(i-j)+1:]...)
			j = j + 1
		}
	}
}
