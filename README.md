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

## Known restrictions

For direct output in ZIP file, you must create it before: 

    // do stuff with feed
    os.Create("/path/to/output.zip")
    
    w := gtfswriter.Writer{}
    werror := w.Write(feed, "/path/to/output")

## License

GPL v2, see LICENSE
