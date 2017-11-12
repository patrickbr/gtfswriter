[![Go Report Card](https://goreportcard.com/badge/github.com/patrickbr/gtfswriter)](https://goreportcard.com/report/github.com/patrickbr/gtfswriter) [![Build Status](https://travis-ci.org/patrickbr/gtfswriter.svg?branch=master)](https://travis-ci.org/patrickbr/gtfswriter) [![GoDoc](https://godoc.org/github.com/patrickbr/gtfswriter?status.png)](https://godoc.org/github.com/patrickbr/gtfswriter)

# go gtfswriter

A writer for the GTFS structure created by the [go gtfsparser](https://github.com/patrickbr/gtfsparser). This can be used to write feeds that have been changed programmatically back to a GTFS feed.

## Usage
    feed := gtfsparser.NewFeed()
    error := feed.Parse("sample-feed.zip")

    // do stuff with feed

    w := gtfswriter.Writer{}
    werror := w.Write(feed, "/path/to/output")

## Features

Optional fields are not outputted if empty, if default values are used, the writer outputs them empty.

The ZIP compression level can be specified by setting `ZipCompressionLevel`:

    w := gtfswriter.Writer{ZipCompressionLevel : 9}
    werror := w.Write(feed, "/path/to/output")

The following options are supported:

* `0` (default): default compression
* `1`-`9`: Compression levels from `1` (fastest) to `9` (best)
* `-1`: no compression

## Known restrictions

For direct output in ZIP file, you must create it before:

    // do stuff with feed
    os.Create("/path/to/output.zip")

    w := gtfswriter.Writer{}
    werror := w.Write(feed, "/path/to/output")

## License

GPL v2, see LICENSE
