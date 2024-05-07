// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	// "archive/zip"
	"compress/flate"
	"errors"
	"github.com/klauspost/compress/zip"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"io"
	"math"
	"os"
	opath "path"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

type EntAttr struct {
	attr   *gtfs.Attribution
	route  *gtfs.Route
	agency *gtfs.Agency
	trip   *gtfs.Trip
}

// A Writer for GTFS files
type Writer struct {
	//case write in Dir
	curFileHandle *os.File
	//case write in File
	zipFile             *zip.Writer
	ZipCompressionLevel int
	Sorted              bool
	ExplicitCalendar    bool
	KeepColOrder        bool
	buff                []byte
}

// Write a single GTFS feed to a system path, either a folder or a ZIP file
func (writer *Writer) Write(feed *gtfsparser.Feed, path string) error {
	writer.buff = make([]byte, 0, 64)
	var e error

	// collected route, trip and agency attributions
	attributions := make([]EntAttr, 0)

	e = writer.writeAgencies(path, feed, &attributions)

	if e == nil {
		e = writer.writeFeedInfos(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeStops(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeShapes(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeRoutes(path, feed, &attributions)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeCalendar(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeCalendarDates(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeTrips(path, feed, &attributions)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeStopTimes(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeFareAttributes(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeFareAttributeRules(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeFrequencies(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeTransfers(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeLevels(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writePathways(path, feed)
	}
	runtime.GC()
	if e == nil {
		e = writer.writeAttributions(path, feed, attributions)
	}
	runtime.GC()

	if e != nil {
		return e
	}

	if writer.curFileHandle != nil {
		writer.curFileHandle.Close()
	}
	if writer.zipFile != nil {
		e = writer.zipFile.Close()
	}

	return e
}

func (writer *Writer) delExistingFile(path string, name string) error {
	fileInfo, err := os.Stat(path)

	if err != nil {
		return err
	}

	if fileInfo.IsDir() {
		if _, err := os.Stat(opath.Join(path, name)); err == nil {
			err := os.Remove(opath.Join(path, name))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (writer *Writer) getFileForWriting(path string, name string) (io.Writer, error) {
	fileInfo, err := os.Stat(path)

	if err != nil {
		return nil, err
	}

	if fileInfo.IsDir() {
		if writer.curFileHandle != nil {
			// close previous handle
			writer.curFileHandle.Close()
		}

		return os.Create(opath.Join(path, name))
	}

	// ZIP Archive
	if writer.zipFile == nil {
		zipF, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		writer.zipFile = zip.NewWriter(zipF)

		if writer.ZipCompressionLevel == 0 {
			writer.zipFile.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
				return flate.NewWriter(out, flate.DefaultCompression)
			})
		} else if writer.ZipCompressionLevel == -1 {
			writer.zipFile.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
				return flate.NewWriter(out, flate.NoCompression)
			})
		} else if writer.ZipCompressionLevel > 0 {
			writer.zipFile.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
				return flate.NewWriter(out, writer.ZipCompressionLevel)
			})
		}
	}
	return writer.zipFile.Create(name)
}

func (writer *Writer) writeAgencies(path string, feed *gtfsparser.Feed, attrs *[]EntAttr) (err error) {
	file, e := writer.getFileForWriting(path, "agency.txt")

	if e != nil {
		return errors.New("Could not open required file agency.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"agency.txt", r.(error).Error()}
		}
	}()

	header := []string{"agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone", "agency_fare_url", "agency_email"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.AgenciesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"agency_name", "agency_url", "agency_timezone"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Agencies)
	}

	for _, v := range feed.Agencies {
		fareurl := ""
		if v.Fare_url != nil {
			fareurl = v.Fare_url.String()
		}

		for _, attr := range v.Attributions {
			*attrs = append(*attrs, EntAttr{attr, nil, v, nil})
		}

		url := ""
		if v.Url != nil {
			url = v.Url.String()
		}

		email := ""
		if v.Email != nil {
			email = v.Email.Address
		}

		row := []string{v.Id, strings.Replace(v.Name, "\n", " ", -1), url, v.Timezone.GetTzString(), v.Lang.GetLangString(), v.Phone, fareurl, email}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.AgenciesAddFlds[name][v.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}

	csvwriter.Flush()

	return e
}

func (writer *Writer) writeFeedInfos(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.FeedInfos) == 0 {
		return writer.delExistingFile(path, "feed_info.txt")
	}
	file, e := writer.getFileForWriting(path, "feed_info.txt")

	if e != nil {
		return errors.New("Could not open required file feed_info.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"feed_info.txt", r.(error).Error()}
		}
	}()

	header := []string{"feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version", "feed_contact_email", "feed_contact_url"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.FeedInfosAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"feed_publisher_name", "feed_publisher_url", "feed_lang"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.FeedInfos)
	}

	for _, v := range feed.FeedInfos {
		puburl := ""
		if v.Publisher_url != nil {
			puburl = v.Publisher_url.String()
		}
		contacturl := ""
		if v.Contact_url != nil {
			contacturl = v.Contact_url.String()
		}

		contactemail := ""
		if v.Contact_email != nil {
			contactemail = v.Contact_email.Address
		}

		row := []string{strings.Replace(v.Publisher_name, "\n", " ", -1), puburl, v.Lang, dateToString(v.Start_date), dateToString(v.End_date), strings.Replace(v.Version, "\n", " ", -1), contactemail, contacturl}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.FeedInfosAddFlds[name][v]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	csvwriter.Flush()

	return e
}

func (writer *Writer) writeStops(path string, feed *gtfsparser.Feed) (err error) {
	file, e := writer.getFileForWriting(path, "stops.txt")

	if e != nil {
		return errors.New("Could not open required file stops.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"stops.txt", r.(error).Error()}
		}
	}()

	header := []string{"stop_name", "parent_station", "stop_code", "zone_id", "stop_id", "stop_desc", "stop_lat", "stop_lon", "stop_url", "location_type", "stop_timezone", "wheelchair_boarding", "level_id", "platform_code"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.StopsAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"stop_name", "stop_id", "stop_lat", "stop_lon"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Stops)
	}

	for _, v := range feed.Stops {
		locType := int(v.Location_type)
		if locType == 0 {
			// dont print locType 0
			locType = -1
		}
		wb := v.Wheelchair_boarding
		if wb == 0 {
			wb = -1
		}
		parentStID := ""
		if v.Parent_station != nil {
			parentStID = v.Parent_station.Id
		}
		url := ""
		if v.Url != nil {
			url = v.Url.String()
		}
		levelId := ""
		if v.Level != nil {
			levelId = v.Level.Id
		}

		row := make([]string, 0)

		if v.HasLatLon() {
			row = []string{strings.Replace(v.Name, "\n", " ", -1), parentStID, v.Code, v.Zone_id, v.Id, strings.Replace(v.Desc, "\n", " ", -1), writer.formatFloat(v.Lat), writer.formatFloat(v.Lon), url, posIntToString(locType), v.Timezone.GetTzString(), posIntToString(int(wb)), levelId, v.Platform_code}
		} else {
			row = []string{strings.Replace(v.Name, "\n", " ", -1), parentStID, v.Code, v.Zone_id, v.Id, strings.Replace(v.Desc, "\n", " ", -1), "", "", url, posIntToString(locType), v.Timezone.GetTzString(), posIntToString(int(wb)), levelId, v.Platform_code}
		}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.StopsAddFlds[name][v.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(12)
	}
	csvwriter.Flush()

	return e
}

type shapeLine struct {
	Shape *gtfs.Shape
}

type shapeLines []shapeLine

func (sl shapeLines) Len() int      { return len(sl) }
func (sl shapeLines) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl shapeLines) Less(i, j int) bool {
	return sl[i].Shape.Id < sl[j].Shape.Id
}

func (writer *Writer) formatFloat(f float32) string {
	writer.buff = writer.buff[:0]
	writer.buff = strconv.AppendFloat(writer.buff, float64(f), 'f', -1, 32)
	return string(writer.buff)
}

func (writer *Writer) shapePointLine(v *gtfs.Shape, vp *gtfs.ShapePoint, ret []string) {
	distTrav := ""
	if vp.HasDistanceTraveled() {
		distTrav = writer.formatFloat(vp.Dist_traveled)
	}

	ret[0] = v.Id
	ret[1] = writer.formatFloat(vp.Lat)
	ret[2] = writer.formatFloat(vp.Lon)
	ret[3] = posIntToString(int(vp.Sequence))
	ret[4] = distTrav
}

func (writer *Writer) writeShapes(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Shapes) == 0 {
		return writer.delExistingFile(path, "shapes.txt")
	}
	file, e := writer.getFileForWriting(path, "shapes.txt")

	if e != nil {
		return errors.New("Could not open required file shapes.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"shapes.txt", r.(error).Error()}
		}
	}()

	header := []string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.ShapesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Shapes)
	}

	lines := make(shapeLines, len(feed.Shapes))
	i := 0

	row := make([]string, 5+len(feed.ShapesAddFlds))

	for _, v := range feed.Shapes {
		lines[i] = shapeLine{v}

		i += 1

		for _, vp := range v.Points {
			writer.shapePointLine(v, &vp, row)

			// fill them with dummy values to make sure they count as non-empty
			for i := 0; i < len(feed.ShapesAddFlds); i++ {
				row[5+i] = "-"
			}
			csvwriter.HeaderUsage(row)
		}
	}

	if writer.Sorted {
		sort.Sort(lines)
	}

	csvwriter.WriteHeader()

	for _, v := range lines {
		for _, vp := range v.Shape.Points {
			writer.shapePointLine(v.Shape, &vp, row)

			// additional fields
			for i, name := range addFieldsOrder {
				if vald, ok := feed.ShapesAddFlds[name][v.Shape.Id][int(vp.Sequence)]; ok {
					row[5+i] = vald
				} else {
					row[5+i] = ""
				}
			}

			csvwriter.WriteCsvLineRaw(row)
		}
	}

	csvwriter.FlushFile()

	return e
}

func (writer *Writer) writeRoutes(path string, feed *gtfsparser.Feed, attrs *[]EntAttr) (err error) {
	file, e := writer.getFileForWriting(path, "routes.txt")

	if e != nil {
		return errors.New("Could not open required file routes.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"routes.txt", r.(error).Error()}
		}
	}()

	header := []string{"route_long_name", "route_short_name", "agency_id", "route_desc", "route_type", "route_id", "route_url", "route_color", "route_text_color", "route_sort_order", "continuous_pickup", "continuous_drop_off"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.RoutesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"route_long_name", "route_short_name", "route_type", "route_id"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Routes)
	}

	for _, r := range feed.Routes {
		agency := ""
		if r.Agency != nil {
			agency = r.Agency.Id
		}

		for _, attr := range r.Attributions {
			*attrs = append(*attrs, EntAttr{attr, r, nil, nil})
		}

		color := r.Color
		if color == "FFFFFF" {
			color = ""
		}
		textColor := r.Text_color
		if textColor == "000000" {
			textColor = ""
		}
		url := ""
		if r.Url != nil {
			url = r.Url.String()
		}
		contPickup := int(r.Continuous_pickup)
		if contPickup == 1 {
			contPickup = -1
		}
		contDropOff := int(r.Continuous_drop_off)
		if contDropOff == 1 {
			contDropOff = -1
		}

		row := []string{strings.Replace(r.Long_name, "\n", " ", -1), strings.Replace(r.Short_name, "\n", " ", -1), agency, strings.Replace(r.Desc, "\n", " ", -1), posIntToString(int(r.Type)), r.Id, url, color, textColor, posIntToString(r.Sort_order), posIntToString(contPickup), posIntToString(contDropOff)}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.RoutesAddFlds[name][r.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(9)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeCalendar(path string, feed *gtfsparser.Feed) (err error) {
	hasCalendarEntries := false
	for _, v := range feed.Services {
		if v.RawDaymap() > 0 || v.IsEmpty() {
			hasCalendarEntries = true
			break
		}
	}
	if !hasCalendarEntries && !writer.ExplicitCalendar {
		return writer.delExistingFile(path, "calendar.txt")
	}
	file, e := writer.getFileForWriting(path, "calendar.txt")

	if e != nil {
		return errors.New("Could not open required file calendar.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"calendar.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date", "service_id"},
		[]string{"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date", "service_id"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Calendar)
	}

	for _, v := range feed.Services {
		if v.RawDaymap() > 0 || v.IsEmpty() {
			csvwriter.WriteCsvLine([]string{boolToGtfsBool(v.Daymap(1), true), boolToGtfsBool(v.Daymap(2), true), boolToGtfsBool(v.Daymap(3), true), boolToGtfsBool(v.Daymap(4), true), boolToGtfsBool(v.Daymap(5), true), boolToGtfsBool(v.Daymap(6), true), boolToGtfsBool(v.Daymap(0), true), dateToString(v.Start_date()), dateToString(v.End_date()), v.Id()})
		} else if writer.ExplicitCalendar {
			csvwriter.WriteCsvLine([]string{"0", "0", "0", "0", "0", "0", "0", dateToString(v.GetFirstDefinedDate()), dateToString(v.GetLastDefinedDate()), v.Id()})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(10)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeCalendarDates(path string, feed *gtfsparser.Feed) (err error) {
	hasCalendarDatesEntries := false
	for _, v := range feed.Services {
		if len(v.Exceptions()) > 0 {
			hasCalendarDatesEntries = true
			break
		}
	}
	if !hasCalendarDatesEntries {
		return writer.delExistingFile(path, "calendar_dates.txt")
	}
	file, e := writer.getFileForWriting(path, "calendar_dates.txt")

	if e != nil {
		return errors.New("Could not open required file calendar_dates.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"calendar_dates.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"service_id", "exception_type", "date"}, []string{"service_id", "exception_type", "date"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.CalendarDates)
	}

	for _, v := range feed.Services {
		for d, traw := range v.Exceptions() {
			t := int8(1)
			if !traw {
				t = 2
			}
			csvwriter.WriteCsvLine([]string{v.Id(), posIntToString(int(t)), dateToString(d)})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(3)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeTrips(path string, feed *gtfsparser.Feed, attrs *[]EntAttr) (err error) {
	file, e := writer.getFileForWriting(path, "trips.txt")

	if e != nil {
		return errors.New("Could not open required file trips.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"trips.txt", r.(error).Error()}
		}
	}()

	header := []string{"route_id", "service_id", "trip_headsign", "trip_short_name", "direction_id", "block_id", "shape_id", "trip_id", "wheelchair_accessible", "bikes_allowed"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.TripsAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"route_id", "service_id", "trip_id"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Trips)
	}

	for _, t := range feed.Trips {
		wa := int(t.Wheelchair_accessible)
		if wa == 0 {
			wa = -1
		}
		if t.Attributions != nil {
			for _, attr := range *t.Attributions {
				*attrs = append(*attrs, EntAttr{attr, nil, nil, t})
			}
		}
		ba := int(t.Bikes_allowed)
		if ba == 0 {
			ba = -1
		}

		row := make([]string, 0)

		blockid := ""
		shortname := ""

		if t.Block_id != nil {
			blockid = *t.Block_id
		}

		if t.Short_name != nil {
			shortname = *t.Short_name
		}

		if t.Shape == nil {
			row = []string{t.Route.Id, t.Service.Id(), strings.Replace(*t.Headsign, "\n", " ", -1), strings.Replace(shortname, "\n", " ", -1), posIntToString(int(t.Direction_id)), blockid, "", t.Id, posIntToString(wa), posIntToString(ba)}
		} else {
			row = []string{t.Route.Id, t.Service.Id(), strings.Replace(*t.Headsign, "\n", " ", -1), strings.Replace(shortname, "\n", " ", -1), posIntToString(int(t.Direction_id)), blockid, t.Shape.Id, t.Id, posIntToString(wa), posIntToString(ba)}
		}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.TripsAddFlds[name][t.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(10)
	}
	csvwriter.Flush()

	return e
}

type tripLine struct {
	Trip *gtfs.Trip
}

type tripLines []tripLine

func (tl tripLines) Len() int      { return len(tl) }
func (tl tripLines) Swap(i, j int) { tl[i], tl[j] = tl[j], tl[i] }
func (tl tripLines) Less(i, j int) bool {
	return tl[i].Trip.Route.Type < tl[j].Trip.Route.Type ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name < tl[j].Trip.Route.Long_name) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && *tl[i].Trip.Headsign < *tl[j].Trip.Headsign) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && *tl[i].Trip.Headsign == *tl[j].Trip.Headsign && tl[i].Trip.Route.Id < tl[j].Trip.Route.Id) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && *tl[i].Trip.Headsign == *tl[j].Trip.Headsign && tl[i].Trip.Route.Id == tl[j].Trip.Route.Id && tl[i].Trip.Id < tl[j].Trip.Id)
}

func (writer *Writer) stopTimeLine(v *gtfs.Trip, st *gtfs.StopTime, row []string) {
	distTrav := ""
	if st.HasDistanceTraveled() {
		distTrav = writer.formatFloat(st.Shape_dist_traveled())
	}
	puType := int(st.Pickup_type())
	if puType == 0 {
		puType = -1
	}
	doType := int(st.Drop_off_type())
	if doType == 0 {
		doType = -1
	}
	contPickup := int(st.Continuous_pickup())
	if contPickup == 1 {
		contPickup = -1
	}
	contDropOff := int(st.Continuous_drop_off())
	if contDropOff == 1 {
		contDropOff = -1
	}

	row[0] = v.Id
	row[3] = st.Stop().Id
	row[4] = posIntToString(st.Sequence())
	row[5] = *st.Headsign()
	row[6] = posIntToString(puType)
	row[7] = posIntToString(doType)
	row[8] = posIntToString(contPickup)
	row[9] = posIntToString(contDropOff)
	row[10] = distTrav
	row[11] = ""

	if st.Arrival_time().Empty() || st.Departure_time().Empty() {
		row[1] = ""
		row[2] = ""
	} else {
		if st.Timepoint() {
			row[1] = timeToString(st.Arrival_time())
			row[2] = timeToString(st.Departure_time())
		} else {
			row[1] = timeToString(st.Arrival_time())
			row[2] = timeToString(st.Departure_time())
			row[11] = "0"
		}
	}
}

func (writer *Writer) writeStopTimes(path string, feed *gtfsparser.Feed) (err error) {
	file, e := writer.getFileForWriting(path, "stop_times.txt")

	if e != nil {
		return errors.New("Could not open required file stop_times.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"stop_times.txt", r.(error).Error()}
		}
	}()

	header := []string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type", "continuous_pickup", "continuous_drop_off", "shape_dist_traveled", "timepoint"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.StopTimesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.StopTimes)
	}

	lines := make(tripLines, len(feed.Trips))
	i := 0

	row := make([]string, 12+len(feed.StopTimesAddFlds))

	for _, v := range feed.Trips {
		lines[i] = tripLine{v}
		i += 1

		for _, st := range v.StopTimes {
			writer.stopTimeLine(v, &st, row)

			// fill them with dummy values to make sure they count as non-empty
			for i := 0; i < len(feed.StopTimesAddFlds); i++ {
				row[12+i] = "-"
			}
			csvwriter.HeaderUsage(row)
		}
	}

	// always keep additional header
	if writer.Sorted {
		sort.Sort(lines)
	}

	csvwriter.WriteHeader()

	for _, v := range lines {
		for _, st := range v.Trip.StopTimes {
			writer.stopTimeLine(v.Trip, &st, row)

			// additional fields
			for i, name := range addFieldsOrder {
				if vald, ok := feed.StopTimesAddFlds[name][v.Trip.Id][st.Sequence()]; ok {
					row[12+i] = vald
				} else {
					row[12+i] = ""
				}
			}

			csvwriter.WriteCsvLineRaw(row)
		}
	}

	csvwriter.FlushFile()

	return e
}

func (writer *Writer) writeFareAttributes(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.FareAttributes) == 0 {
		return writer.delExistingFile(path, "fare_attributes.txt")
	}
	file, e := writer.getFileForWriting(path, "fare_attributes.txt")

	if e != nil {
		return errors.New("Could not open required file fare_attributes.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"fare_attributes.txt", r.(error).Error()}
		}
	}()

	header := []string{"fare_id", "price", "currency_type", "payment_method", "transfers", "transfer_duration", "agency_id"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.FareAttributesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"fare_id", "price", "currency_type", "payment_method", "transfers"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.FareAttributes)
	}

	for _, v := range feed.FareAttributes {
		agencyId := ""
		if v.Agency != nil {
			agencyId = v.Agency.Id
		}

		row := []string{v.Id, v.Price, v.Currency_type, posIntToString(v.Payment_method), posIntToString(v.Transfers), posIntToString(v.Transfer_duration), agencyId}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.FareAttributesAddFlds[name][v.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeFareAttributeRules(path string, feed *gtfsparser.Feed) (err error) {
	hasFareAttrRules := false
	for _, v := range feed.FareAttributes {
		if len(v.Rules) > 0 {
			hasFareAttrRules = true
			break
		}
	}
	if !hasFareAttrRules {
		return writer.delExistingFile(path, "fare_rules.txt")
	}
	file, e := writer.getFileForWriting(path, "fare_rules.txt")

	if e != nil {
		return errors.New("Could not open required file fare_rules.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"fare_rules.txt", r.(error).Error()}
		}
	}()

	header := []string{"fare_id", "route_id", "origin_id", "destination_id", "contains_id"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.FareRulesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"fare_id"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.FareAttributeRules)
	}

	for _, v := range feed.FareAttributes {
		for _, r := range v.Rules {
			row := make([]string, 0)

			if r.Route == nil {
				row = []string{v.Id, "", r.Origin_id, r.Destination_id, r.Contains_id}
			} else {
				row = []string{v.Id, r.Route.Id, r.Origin_id, r.Destination_id, r.Contains_id}
			}

			for _, name := range addFieldsOrder {
				if vald, ok := feed.FareRulesAddFlds[name][v.Id][r]; ok {
					row = append(row, vald)
				} else {
					row = append(row, "")
				}
			}

			csvwriter.WriteCsvLine(row)
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(5)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeFrequencies(path string, feed *gtfsparser.Feed) (err error) {
	hasFrequencies := false
	for _, v := range feed.Trips {
		if v.Frequencies == nil {
			continue
		}
		if len(*v.Frequencies) > 0 {
			hasFrequencies = true
			break
		}
	}
	if !hasFrequencies {
		return writer.delExistingFile(path, "frequencies.txt")
	}
	file, e := writer.getFileForWriting(path, "frequencies.txt")

	if e != nil {
		return errors.New("Could not open required file frequencies.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"frequencies.txt", r.(error).Error()}
		}
	}()

	header := []string{"trip_id", "start_time", "end_time", "headway_secs", "exact_times"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.FrequenciesAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"trip_id", "start_time", "end_time", "headway_secs"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Frequencies)
	}

	for _, v := range feed.Trips {
		if v.Frequencies == nil {
			continue
		}
		for _, f := range *v.Frequencies {
			row := make([]string, 0)
			if !f.Exact_times {
				row = []string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), posIntToString(f.Headway_secs), ""}
			} else {
				row = []string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), posIntToString(f.Headway_secs), "1"}
			}

			for _, name := range addFieldsOrder {
				if vald, ok := feed.FrequenciesAddFlds[name][v.Id][f]; ok {
					row = append(row, vald)
				} else {
					row = append(row, "")
				}
			}

			csvwriter.WriteCsvLine(row)
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(5)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeTransfers(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Transfers) == 0 {
		return writer.delExistingFile(path, "transfers.txt")
	}
	file, e := writer.getFileForWriting(path, "transfers.txt")

	if e != nil {
		return errors.New("Could not open required file transfers.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"transfers.txt", r.(error).Error()}
		}
	}()

	header := []string{"from_stop_id", "to_stop_id", "from_route_id", "to_route_id", "from_trip_id", "to_trip_id", "transfer_type", "min_transfer_time"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.TransfersAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"transfer_type"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Transfers)
	}

	for tk, tv := range feed.Transfers {
		transferType := tv.Transfer_type
		if transferType == 0 {
			transferType = -1
		}

		from_sid := ""
		to_sid := ""
		from_rid := ""
		to_rid := ""
		from_tid := ""
		to_tid := ""

		if tk.From_stop != nil {
			from_sid = tk.From_stop.Id
		}
		if tk.To_stop != nil {
			to_sid = tk.To_stop.Id
		}
		if tk.From_route != nil {
			from_rid = tk.From_route.Id
		}
		if tk.To_route != nil {
			to_rid = tk.To_route.Id
		}
		if tk.From_trip != nil {
			from_tid = tk.From_trip.Id
		}
		if tk.To_trip != nil {
			to_tid = tk.To_trip.Id
		}

		row := []string{from_sid, to_sid, from_rid, to_rid, from_tid, to_tid, posIntToString(transferType), posIntToString(tv.Min_transfer_time)}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.TransfersAddFlds[name][tk]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(4)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeLevels(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Levels) == 0 {
		return writer.delExistingFile(path, "levels.txt")
	}
	file, e := writer.getFileForWriting(path, "levels.txt")

	if e != nil {
		return errors.New("Could not open required file levels.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"levels.txt", r.(error).Error()}
		}
	}()

	header := []string{"level_id", "level_index", "level_name"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.LevelsAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"fare_id", "level_index"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Levels)
	}

	for _, v := range feed.Levels {
		row := []string{v.Id, writer.formatFloat(v.Index), v.Name}
		for _, name := range addFieldsOrder {
			if vald, ok := feed.LevelsAddFlds[name][v.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}
		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writePathways(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Pathways) == 0 {
		return writer.delExistingFile(path, "pathways.txt")
	}
	file, e := writer.getFileForWriting(path, "pathways.txt")

	if e != nil {
		return errors.New("Could not open required file pathways.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"pathways.txt", r.(error).Error()}
		}
	}()

	header := []string{"pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "length", "traversal_time", "stair_count", "max_slope", "min_width", "signposted_as", "reversed_signposted_as"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.PathwaysAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header,
		[]string{"pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Pathways)
	}

	for _, v := range feed.Pathways {
		length := ""
		if !math.IsNaN(float64(v.Length)) {
			length = writer.formatFloat(v.Length)
		}
		mwidth := ""
		if !math.IsNaN(float64(v.Min_width)) {
			mwidth = writer.formatFloat(v.Min_width)
		}
		maxslope := ""
		if v.Max_slope != 0 {
			maxslope = writer.formatFloat(v.Max_slope)
		}

		row := []string{v.Id, v.From_stop.Id, v.To_stop.Id, posIntToString(int(v.Mode)), boolToGtfsBool(v.Is_bidirectional, true), length, posIntToString(v.Traversal_time), posNegIntToString(v.Stair_count), maxslope, mwidth, v.Signposted_as, v.Reversed_signposted_as}

		for _, name := range addFieldsOrder {
			if vald, ok := feed.PathwaysAddFlds[name][v.Id]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeAttributions(path string, feed *gtfsparser.Feed, attrs []EntAttr) (err error) {
	if len(feed.Attributions) == 0 && len(attrs) == 0 {
		return writer.delExistingFile(path, "attributions.txt")
	}

	file, e := writer.getFileForWriting(path, "attributions.txt")

	if e != nil {
		return errors.New("Could not open required file attributions.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"attributions.txt", r.(error).Error()}
		}
	}()

	header := []string{"attribution_id", "agency_id", "route_id", "trip_id", "organization_name", "is_producer", "is_operator", "is_authority", "attribution_url", "attribution_email", "attribution_phone"}

	addFieldsOrder := make([]string, 0)

	for k := range feed.AttributionsAddFlds {
		header = append(header, k)
		addFieldsOrder = append(addFieldsOrder, k)
	}

	// write header
	csvwriter.SetHeader(header, []string{"organization_name"})

	if writer.KeepColOrder {
		csvwriter.SetOrder(feed.ColOrders.Attributions)
	}

	for _, a := range feed.Attributions {
		url := ""
		if a.Url != nil {
			url = a.Url.String()
		}

		email := ""
		if a.Email != nil {
			email = a.Email.Address
		}

		row := []string{a.Id, "", "", "", a.Organization_name, boolToGtfsBool(a.Is_producer, false), boolToGtfsBool(a.Is_operator, false), boolToGtfsBool(a.Is_authority, false), url, email, a.Phone}

		// additional fields
		for _, name := range addFieldsOrder {
			if vald, ok := feed.AttributionsAddFlds[name][a]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	for _, entattr := range attrs {
		url := ""
		a := entattr.attr
		if a.Url != nil {
			url = a.Url.String()
		}

		email := ""
		if a.Email != nil {
			email = a.Email.Address
		}

		routeid := ""
		agencyid := ""
		tripid := ""

		if entattr.trip != nil {
			tripid = entattr.trip.Id
		}
		if entattr.route != nil {
			routeid = entattr.route.Id
		}
		if entattr.agency != nil {
			agencyid = entattr.agency.Id
		}

		row := []string{a.Id, agencyid, routeid, tripid, a.Organization_name, boolToGtfsBool(a.Is_producer, false), boolToGtfsBool(a.Is_operator, false), boolToGtfsBool(a.Is_authority, false), url, email, a.Phone}

		// additional fields
		for _, name := range addFieldsOrder {
			if vald, ok := feed.AttributionsAddFlds[name][a]; ok {
				row = append(row, vald)
			} else {
				row = append(row, "")
			}
		}

		csvwriter.WriteCsvLine(row)
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}

	csvwriter.Flush()

	return e
}

func fmtIntPadded(val int, sb *strings.Builder) {
	// Left-pad with zeroes so it's at least 2 digits
	if val < 10 {
		sb.WriteRune('0')
	}
	sb.WriteString(strconv.FormatInt(int64(val), 10))
}

func dateToString(date gtfs.Date) string {
	if date.IsEmpty() {
		// null value
		return ""
	}

	var sb strings.Builder
	sb.WriteString(strconv.FormatInt(int64(date.Year()), 10))
	fmtIntPadded(int(date.Month()), &sb)
	fmtIntPadded(int(date.Day()), &sb)
	return sb.String()
}

func timeToString(time gtfs.Time) string {
	// Code would be simpler with Sprintf, but also very inefficient.
	// That's bad, as this is a very hot function (>25% of time was spent here before optimization).
	var sb strings.Builder
	fmtIntPadded(int(time.Hour), &sb)
	sb.WriteRune(':')
	fmtIntPadded(int(time.Minute), &sb)
	sb.WriteRune(':')
	fmtIntPadded(int(time.Second), &sb)
	return sb.String()
}

func posIntToString(i int) string {
	if i < 0 {
		// encoding of "empty"
		return ""
	}
	return strconv.FormatInt(int64(i), 10)
}

func posNegIntToString(i int) string {
	if i == 0 {
		// encoding of "empty"
		return ""
	}
	return strconv.FormatInt(int64(i), 10)
}

func boolToGtfsBool(v bool, full bool) string {
	if v {
		return "1"
	}
	if full {
		return "0"
	}
	return ""
}

func floatEquals(a float64, b float64, e float64) bool {
	if (a-b) < e && (b-a) < e {
		return true
	}
	return false
}
