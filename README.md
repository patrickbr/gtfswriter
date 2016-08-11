# go gtfswriter

A writer for the GTFS structure created by the [go gtfsparser](https://github.com/patrickbr/gtfsparser). This can be used to write feeds that have been changed programmatically back to a file.

## Usage
    feed := gtfsparser.NewFeed()
    error := feed.Parse("sample-feed.zip")

    // do stuff with feed

    w := gtfswriter.Writer{}
    werror := w.Write(feed, "/path/to/output")

## *Known restrictions

Direct output to ZIP files not supported at the moment.

## License

GPL v2, see LICENSE
