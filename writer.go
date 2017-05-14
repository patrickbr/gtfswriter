// Copyright 2016 Patrick Brosi
// Authors: info@patrickbrosi.de
//
// Use of this source code is governed by a GPL v2
// license that can be found in the LICENSE file

package gtfswriter

import (
	"errors"
	"fmt"
	"github.com/patrickbr/gtfsparser"
	gtfs "github.com/patrickbr/gtfsparser/gtfs"
	"io"
	"os"
	opath "path"
	"strconv"
)

type Writer struct {
	curFileHandle *os.File
}

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
	} else {
		return nil, errors.New("Output to file not yet supported.")
	}

	return nil, errors.New("Could not open for writing.")
}

func (writer *Writer) writeAgencies(path string, feed *gtfsparser.Feed) (err error) {
	file, e := writer.getFileForWriting(path, "agency.txt")

	if e != nil {
		return errors.New("Could not open required file agency.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = WriteError{"agency.txt", r.(error).Error()}
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
			err = WriteError{"feed_info.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"feed_publisher_name", "feed_publisher_url", "feed_lang", "feed_start_date", "feed_end_date", "feed_version"},
		[]string{"feed_publisher_name", "feed_publisher_url", "feed_lang"})

	for _, v := range feed.FeedInfos {
		puburl := ""
		if v.Publisher_url != nil {
			puburl = v.Publisher_url.String()
		}
		csvwriter.WriteCsvLine([]string{v.Publisher_name, puburl, v.Lang, dateToString(v.Start_date), dateToString(v.End_date), v.Version})
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
			err = WriteError{"stops.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone", "wheelchair_boarding"},
		[]string{"stop_id", "stop_name", "stop_lat", "stop_lon"})

	for _, v := range feed.Stops {
		locTypeBool := v.Location_type
		locType := 1
		if !locTypeBool {
			locType = -1
		}
		wb := v.Wheelchair_boarding
		if wb == 0 {
			wb = -1
		}
		parentStId := ""
		if v.Parent_station != nil {
			parentStId = v.Parent_station.Id
		}
		url := ""
		if v.Url != nil {
			url = v.Url.String()
		}
		csvwriter.WriteCsvLine([]string{v.Id, v.Code, v.Name, v.Desc, strconv.FormatFloat(float64(v.Lat), 'f', -1, 32), strconv.FormatFloat(float64(v.Lon), 'f', -1, 32), v.Zone_id, url, intToString(locType), parentStId, v.Timezone.GetTzString(), intToString(int(wb))})
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
			err = WriteError{"shapes.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence", "shape_dist_traveled"},
		[]string{"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"})

	for _, v := range feed.Shapes {
		for _, vp := range v.Points {
			dist_trav := ""
			if vp.HasDistanceTraveled() {
				dist_trav = strconv.FormatFloat(float64(vp.Dist_traveled), 'f', -1, 32)
			}
			csvwriter.WriteCsvLine([]string{v.Id, strconv.FormatFloat(float64(vp.Lat), 'f', -1, 32), strconv.FormatFloat(float64(vp.Lon), 'f', -1, 32), intToString(vp.Sequence), dist_trav})
		}
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
			err = WriteError{"routes.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"route_id", "agency_id", "route_short_name", "route_long_name", "route_desc", "route_type", "route_url", "route_color", "route_text_color"},
		[]string{"route_id", "route_short_name", "route_long_name", "route_type"})

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
		csvwriter.WriteCsvLine([]string{v.Id, agency, v.Short_name, v.Long_name, v.Desc, intToString(int(v.Type)), url, color, textColor})
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
			err = WriteError{"calendar.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"},
		[]string{"service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"})

	for _, v := range feed.Services {
		if v.Daymap[0] || v.Daymap[1] || v.Daymap[2] || v.Daymap[3] || v.Daymap[4] || v.Daymap[5] || v.Daymap[6] {
			csvwriter.WriteCsvLine([]string{v.Id, boolToGtfsBool(v.Daymap[1]), boolToGtfsBool(v.Daymap[2]), boolToGtfsBool(v.Daymap[3]), boolToGtfsBool(v.Daymap[4]), boolToGtfsBool(v.Daymap[5]), boolToGtfsBool(v.Daymap[6]), boolToGtfsBool(v.Daymap[0]), dateToString(v.Start_date), dateToString(v.End_date)})
		}
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
			err = WriteError{"calendar_dates.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"service_id", "date", "exception_type"}, []string{"service_id", "date", "exception_type"})

	for _, v := range feed.Services {
		for _, e := range v.Exceptions {
			csvwriter.WriteCsvLine([]string{v.Id, dateToString(e.Date), intToString(int(e.Type))})
		}
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
			err = WriteError{"trips.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name", "direction_id", "block_id", "shape_id", "wheelchair_accessible", "bikes_allowed"},
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
			csvwriter.WriteCsvLine([]string{v.Route.Id, v.Service.Id, v.Id, v.Headsign, v.Short_name, intToString(int(v.Direction_id)), v.Block_id, "", intToString(wa), intToString(ba)})
		} else {
			csvwriter.WriteCsvLine([]string{v.Route.Id, v.Service.Id, v.Id, v.Headsign, v.Short_name, intToString(int(v.Direction_id)), v.Block_id, v.Shape.Id, intToString(wa), intToString(ba)})
		}
	}

	csvwriter.Flush()

	return e
}

func (writer *Writer) writeStopTimes(path string, feed *gtfsparser.Feed) (err error) {
	file, e := writer.getFileForWriting(path, "stop_times.txt")

	if e != nil {
		return errors.New("Could not open required file stop_times.txt for writing")
	}

	csvwriter := NewCsvWriter(file)

	defer func() {
		if r := recover(); r != nil {
			err = WriteError{"stop_times.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled", "timepoint"},
		[]string{"trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"})

	for _, v := range feed.Trips {
		for _, st := range v.StopTimes {
			dist_trav := ""
			if st.HasDistanceTraveled() {
				dist_trav = strconv.FormatFloat(float64(st.Shape_dist_traveled), 'f', -1, 32)
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
				csvwriter.WriteCsvLine([]string{v.Id, "", "", st.Stop.Id, intToString(st.Sequence), st.Headsign, intToString(puType), intToString(doType), dist_trav, ""})
			} else {
				if st.Timepoint {
					csvwriter.WriteCsvLine([]string{v.Id, timeToString(st.Arrival_time), timeToString(st.Departure_time), st.Stop.Id, intToString(st.Sequence), st.Headsign, intToString(puType), intToString(doType), dist_trav, ""})
				} else {
					csvwriter.WriteCsvLine([]string{v.Id, timeToString(st.Arrival_time), timeToString(st.Departure_time), st.Stop.Id, intToString(st.Sequence), st.Headsign, intToString(puType), intToString(doType), dist_trav, "0"})
				}
			}
		}
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
			err = WriteError{"fare_attributes.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"fare_id", "price", "currency_type", "payment_method", "transfers", "transfer_duration"},
		[]string{"fare_id", "price", "currency_type", "payment_method", "transfers"})

	for _, v := range feed.FareAttributes {
		csvwriter.WriteCsvLine([]string{v.Id, v.Price, v.Currency_type, intToString(v.Payment_method), intToString(v.Transfers), intToString(v.Transfer_duration)})
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
			err = WriteError{"fare_rules.txt", r.(error).Error()}
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
			err = WriteError{"frequencies.txt", r.(error).Error()}
		}
	}()

	// write header
	csvwriter.SetHeader([]string{"trip_id", "start_time", "end_time", "headway_secs", "exact_times"}, []string{"trip_id", "start_time", "end_time", "headway_secs"})

	for _, v := range feed.Trips {
		for _, f := range v.Frequencies {
			if !f.Exact_times {
				csvwriter.WriteCsvLine([]string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), intToString(f.Headway_secs), ""})
			} else {
				csvwriter.WriteCsvLine([]string{v.Id, timeToString(f.Start_time), timeToString(f.End_time), intToString(f.Headway_secs), "1"})
			}
		}
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
			err = WriteError{"transfers.txt", r.(error).Error()}
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
		csvwriter.WriteCsvLine([]string{v.From_stop.Id, v.To_stop.Id, intToString(transferType), intToString(v.Min_transfer_time)})
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
	return fmt.Sprintf("%d:%02d:%02d", time.Hour, time.Minute, time.Second)
}

func intToString(i int) string {
	if i < 0 {
		// encoding of "empty"
		return ""
	} else {
		return strconv.FormatInt(int64(i), 10)
	}
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
