// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"archive/zip"
	"compress/flate"
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"io"
	"os"
	opath "path"
	"sort"
	"strconv"
)

// A Writer for GTFS files
type Writer struct {
	//case write in Dir
	curFileHandle *os.File
	//case write in File
	zipFile             *zip.Writer
	ZipCompressionLevel int
	Sorted              bool
}

// Write a single GTFS feed to a system path, either a folder or a ZIP file
func (writer *Writer) Write(feed *gtfsparser.Feed, path string) error {
	var e error

	e = writer.writeAgencies(path, feed)

	if e == nil {
		e = writer.writeFeedInfos(path, feed)
	}
	if e == nil {
		e = writer.writeStops(path, feed)
	}
	if e == nil {
		e = writer.writeShapes(path, feed)
	}
	if e == nil {
		e = writer.writeRoutes(path, feed)
	}
	if e == nil {
		e = writer.writeCalendar(path, feed)
	}
	if e == nil {
		e = writer.writeCalendarDates(path, feed)
	}
	if e == nil {
		e = writer.writeTrips(path, feed)
	}
	if e == nil {
		e = writer.writeStopTimes(path, feed)
	}
	if e == nil {
		e = writer.writeFareAttributes(path, feed)
	}
	if e == nil {
		e = writer.writeFareAttributeRules(path, feed)
	}
	if e == nil {
		e = writer.writeFrequencies(path, feed)
	}
	if e == nil {
		e = writer.writeTransfers(path, feed)
	}
	if e == nil {
		e = writer.writeLevels(path, feed)
	}
	if e == nil {
		e = writer.writePathways(path, feed)
	}

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

func (writer *Writer) writeAgencies(path string, feed *gtfsparser.Feed) (err error) {
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

	// write header
	csvwriter.SetHeader([]string{"agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang", "agency_phone", "agency_fare_url", "agency_email"},
		[]string{"agency_name", "agency_url", "agency_timezone"})

	for _, v := range feed.Agencies {
		fareurl := ""
		if v.Fare_url != nil {
			fareurl = v.Fare_url.String()
		}

		url := ""
		if v.Url != nil {
			url = v.Url.String()
		}

		email := ""
		if v.Email != nil {
			email = v.Email.Address
		}
		csvwriter.WriteCsvLine([]string{v.Id, v.Name, url, v.Timezone.GetTzString(), v.Lang.GetLangString(), v.Phone, fareurl, email})
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}

	csvwriter.Flush()

	return e
}

func (writer *Writer) writeFeedInfos(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.FeedInfos) == 0 {
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version", "feed_contact_email", "feed_contact_url"},
		[]string{"feed_publisher_name", "feed_publisher_url", "feed_lang"})

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
		csvwriter.WriteCsvLine([]string{v.Publisher_name, puburl, v.Lang, dateToString(v.Start_date), dateToString(v.End_date), v.Version, contactemail, contacturl})
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

	// write header
	csvwriter.SetHeader([]string{"stop_name", "parent_station", "stop_code", "zone_id", "stop_id", "stop_desc", "stop_lat", "stop_lon", "stop_url", "location_type", "stop_timezone", "wheelchair_boarding", "level_id", "platform_code"},
		[]string{"stop_name", "stop_id", "stop_lat", "stop_lon"})

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

		if v.Has_LatLon {
			csvwriter.WriteCsvLine([]string{v.Name, parentStID, v.Code, v.Zone_id, v.Id, v.Desc, strconv.FormatFloat(float64(v.Lat), 'f', -1, 32), strconv.FormatFloat(float64(v.Lon), 'f', -1, 32), url, posIntToString(locType), v.Timezone.GetTzString(), posIntToString(int(wb)), levelId, v.Platform_code})
		} else {
			csvwriter.WriteCsvLine([]string{v.Name, parentStID, v.Code, v.Zone_id, v.Id, v.Desc, "", "", url, posIntToString(locType), v.Timezone.GetTzString(), posIntToString(int(wb)), levelId, v.Platform_code})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(12)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeShapes(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Shapes) == 0 {
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon", "shape_dist_traveled"},
		[]string{"shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"})

	for _, v := range feed.Shapes {
		for _, vp := range v.Points {
			distTrav := ""
			if vp.HasDistanceTraveled() {
				distTrav = strconv.FormatFloat(float64(vp.Dist_traveled), 'f', -1, 32)
			}
			csvwriter.WriteCsvLine([]string{v.Id, posIntToString(vp.Sequence), strconv.FormatFloat(float64(vp.Lat), 'f', -1, 32), strconv.FormatFloat(float64(vp.Lon), 'f', -1, 32), distTrav})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(2)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeRoutes(path string, feed *gtfsparser.Feed) (err error) {
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

	// write header
	csvwriter.SetHeader([]string{"route_long_name", "route_short_name", "agency_id", "route_desc", "route_type", "route_id", "route_url", "route_color", "route_text_color", "route_sort_order"},
		[]string{"route_long_name", "route_short_name", "route_type", "route_id"})

	for _, v := range feed.Routes {
		agency := ""
		if v.Agency != nil {
			agency = v.Agency.Id
		}

		color := v.Color
		if color == "FFFFFF" {
			color = ""
		}
		textColor := v.Text_color
		if textColor == "000000" {
			textColor = ""
		}
		url := ""
		if v.Url != nil {
			url = v.Url.String()
		}
		csvwriter.WriteCsvLine([]string{v.Long_name, v.Short_name, agency, v.Desc, posIntToString(int(v.Type)), v.Id, url, color, textColor, posIntToString(v.Sort_order)})
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
		if v.Daymap[0] || v.Daymap[1] || v.Daymap[2] || v.Daymap[3] || v.Daymap[4] || v.Daymap[5] || v.Daymap[6] {
			hasCalendarEntries = true
			break
		}
	}
	if !hasCalendarEntries {
		return nil
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

	for _, v := range feed.Services {
		if v.Daymap[0] || v.Daymap[1] || v.Daymap[2] || v.Daymap[3] || v.Daymap[4] || v.Daymap[5] || v.Daymap[6] {
			csvwriter.WriteCsvLine([]string{boolToGtfsBool(v.Daymap[1]), boolToGtfsBool(v.Daymap[2]), boolToGtfsBool(v.Daymap[3]), boolToGtfsBool(v.Daymap[4]), boolToGtfsBool(v.Daymap[5]), boolToGtfsBool(v.Daymap[6]), boolToGtfsBool(v.Daymap[0]), dateToString(v.Start_date), dateToString(v.End_date), v.Id})
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
		if len(v.Exceptions) > 0 {
			hasCalendarDatesEntries = true
			break
		}
	}
	if !hasCalendarDatesEntries {
		return nil
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

	for _, v := range feed.Services {
		for d, t := range v.Exceptions {
			csvwriter.WriteCsvLine([]string{v.Id, posIntToString(int(t)), dateToString(d)})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(3)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeTrips(path string, feed *gtfsparser.Feed) (err error) {
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

	// write header
	csvwriter.SetHeader([]string{"route_id", "service_id", "trip_headsign", "trip_short_name", "direction_id", "block_id", "shape_id", "trip_id", "wheelchair_accessible", "bikes_allowed"},
		[]string{"route_id", "service_id", "trip_id"})

	for _, v := range feed.Trips {
		wa := int(v.Wheelchair_accessible)
		if wa == 0 {
			wa = -1
		}
		ba := int(v.Bikes_allowed)
		if ba == 0 {
			ba = -1
		}
		if v.Shape == nil {
			csvwriter.WriteCsvLine([]string{v.Route.Id, v.Service.Id, v.Headsign, v.Short_name, posIntToString(int(v.Direction_id)), v.Block_id, "", v.Id, posIntToString(wa), posIntToString(ba)})
		} else {
			csvwriter.WriteCsvLine([]string{v.Route.Id, v.Service.Id, v.Headsign, v.Short_name, posIntToString(int(v.Direction_id)), v.Block_id, v.Shape.Id, v.Id, posIntToString(wa), posIntToString(ba)})
		}
	}

	if writer.Sorted {
		csvwriter.SortByCols(10)
	}
	csvwriter.Flush()

	return e
}

type tripLine struct {
	Trip     *gtfs.Trip
	Sequence int
	Line     []string
}

type tripLines []tripLine

func (tl tripLines) Len() int      { return len(tl) }
func (tl tripLines) Swap(i, j int) { tl[i], tl[j] = tl[j], tl[i] }
func (tl tripLines) Less(i, j int) bool {
	return tl[i].Trip.Route.Type < tl[j].Trip.Route.Type ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name < tl[j].Trip.Route.Long_name) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && tl[i].Trip.Headsign < tl[j].Trip.Headsign) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && tl[i].Trip.Headsign == tl[j].Trip.Headsign && tl[i].Trip.Route.Id < tl[j].Trip.Route.Id) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && tl[i].Trip.Headsign == tl[j].Trip.Headsign && tl[i].Trip.Route.Id == tl[j].Trip.Route.Id && tl[i].Trip.Id < tl[j].Trip.Id) ||
		(tl[i].Trip.Route.Type == tl[j].Trip.Route.Type && tl[i].Trip.Route.Long_name == tl[j].Trip.Route.Long_name && tl[i].Trip.Headsign == tl[j].Trip.Headsign && tl[i].Trip.Route.Id == tl[j].Trip.Route.Id && tl[i].Trip.Id == tl[j].Trip.Id && tl[i].Sequence < tl[j].Sequence)
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

	// write header
	csvwriter.SetHeader([]string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled", "timepoint"},
		[]string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"})

	var lines tripLines

	for _, v := range feed.Trips {
		for _, st := range v.StopTimes {
			distTrav := ""
			if st.HasDistanceTraveled() {
				distTrav = strconv.FormatFloat(float64(st.Shape_dist_traveled), 'f', -1, 32)
			}
			puType := int(st.Pickup_type)
			if puType == 0 {
				puType = -1
			}
			doType := int(st.Drop_off_type)
			if doType == 0 {
				doType = -1
			}
			if st.Arrival_time.Empty() || st.Departure_time.Empty() {
				lines = append(lines, tripLine{v, st.Sequence, []string{v.Id, "", "", st.Stop.Id, posIntToString(st.Sequence), st.Headsign, posIntToString(puType), posIntToString(doType), distTrav, ""}})
			} else {
				if st.Timepoint {
					lines = append(lines, tripLine{v, st.Sequence, []string{v.Id, timeToString(st.Arrival_time), timeToString(st.Departure_time), st.Stop.Id, posIntToString(st.Sequence), st.Headsign, posIntToString(puType), posIntToString(doType), distTrav, ""}})
				} else {
					lines = append(lines, tripLine{v, st.Sequence, []string{v.Id, timeToString(st.Arrival_time), timeToString(st.Departure_time), st.Stop.Id, posIntToString(st.Sequence), st.Headsign, posIntToString(puType), posIntToString(doType), distTrav, "0"}})
				}
			}
		}
	}

	if writer.Sorted {
		sort.Sort(lines)
	}

	for _, v := range lines {
		csvwriter.WriteCsvLine(v.Line)
	}

	csvwriter.Flush()

	return e
}

func (writer *Writer) writeFareAttributes(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.FareAttributes) == 0 {
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"fare_id", "price", "currency_type", "payment_method", "transfers", "transfer_duration", "agency_id"},
		[]string{"fare_id", "price", "currency_type", "payment_method", "transfers"})

	for _, v := range feed.FareAttributes {
		agencyId := ""
		if v.Agency != nil {
			agencyId = v.Agency.Id
		}
		csvwriter.WriteCsvLine([]string{v.Id, v.Price, v.Currency_type, posIntToString(v.Payment_method), posIntToString(v.Transfers), posIntToString(v.Transfer_duration), agencyId})
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
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"fare_id", "route_id", "origin_id", "destination_id", "contains_id"}, []string{"fare_id"})

	for _, v := range feed.FareAttributes {
		for _, r := range v.Rules {
			if r.Route == nil {
				csvwriter.WriteCsvLine([]string{v.Id, "", r.Origin_id, r.Destination_id, r.Contains_id})
			} else {
				csvwriter.WriteCsvLine([]string{v.Id, r.Route.Id, r.Origin_id, r.Destination_id, r.Contains_id})
			}
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
		if len(v.Frequencies) > 0 {
			hasFrequencies = true
			break
		}
	}
	if !hasFrequencies {
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"trip_id", "start_time", "end_time", "headway_secs", "exact_times"}, []string{"trip_id", "start_time", "end_time", "headway_secs"})

	for _, v := range feed.Trips {
		for _, f := range v.Frequencies {
			if !f.Exact_times {
				csvwriter.WriteCsvLine([]string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), posIntToString(f.Headway_secs), ""})
			} else {
				csvwriter.WriteCsvLine([]string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), posIntToString(f.Headway_secs), "1"})
			}
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
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time"},
		[]string{"from_stop_id", "to_stop_id", "transfer_type"})

	for _, v := range feed.Transfers {
		transferType := v.Transfer_type
		if transferType == 0 {
			transferType = -1
		}
		csvwriter.WriteCsvLine([]string{v.From_stop.Id, v.To_stop.Id, posIntToString(transferType), posIntToString(v.Min_transfer_time)})
	}

	if writer.Sorted {
		csvwriter.SortByCols(4)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writeLevels(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Levels) == 0 {
		return nil
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

	// write header
	csvwriter.SetHeader([]string{"level_id", "level_index", "level_name"},
		[]string{"fare_id", "level_index"})

	for _, v := range feed.Levels {
		csvwriter.WriteCsvLine([]string{v.Id, strconv.FormatFloat(float64(v.Index), 'f', -1, 32), v.Name})
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}
	csvwriter.Flush()

	return e
}

func (writer *Writer) writePathways(path string, feed *gtfsparser.Feed) (err error) {
	if len(feed.Pathways) == 0 {
		return nil
	}
	file, e := writer.getFileForWriting(path, "pathways.txt")

	if e != nil {
		return errors.New("Could not open required file pathways.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = writeError{"levels.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional", "length", "traversal_time", "stair_count", "max_slope", "min_width", "signposted_as", "reversed_signposted_as"},
		[]string{"pathway_id", "from_stop_id", "to_stop_id", "pathway_mode", "is_bidirectional"})

	for _, v := range feed.Pathways {
		length := ""
		if v.Has_length {
			length = strconv.FormatFloat(float64(v.Length), 'f', -1, 32)
		}
		mwidth := ""
		if v.Has_min_width {
			mwidth = strconv.FormatFloat(float64(v.Min_width), 'f', -1, 32)
		}
		maxslope := ""
		if v.Max_slope != 0 {
			maxslope = strconv.FormatFloat(float64(v.Max_slope), 'f', -1, 32)
		}
		csvwriter.WriteCsvLine([]string{v.Id, v.From_stop.Id, v.To_stop.Id, posIntToString(int(v.Mode)), boolToGtfsBool(v.Is_bidirectional), length, posIntToString(v.Traversal_time), posNegIntToString(v.Stair_count), maxslope, mwidth, v.Signposted_as, v.Reversed_signposted_as})
	}

	if writer.Sorted {
		csvwriter.SortByCols(1)
	}
	csvwriter.Flush()

	return e
}

func dateToString(date gtfs.Date) string {
	if date.Year == 0 && date.Month == 0 && date.Day == 0 {
		// null value
		return ""
	}
	return fmt.Sprintf("%d%02d%02d", date.Year, date.Month, date.Day)
}

func timeToString(time gtfs.Time) string {
	return fmt.Sprintf("%02d:%02d:%02d", time.Hour, time.Minute, time.Second)
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

func boolToGtfsBool(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

func floatEquals(a float64, b float64, e float64) bool {
	if (a-b) < e && (b-a) < e {
		return true
	}
	return false
}
