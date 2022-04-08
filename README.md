# Tram Locator
## About
Script to locate all running trams within the VBZ network. The trams locations is based on static timetable and 
dynamic timetable updates (live delays) accessed via [opentransportdata.swiss API](https://opentransportdata.swiss). 
A CSV file with all currently running trams and their corresponding location, destination and delay information is 
written as an output file.   

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install all required packets.

```bash
pip install -r requirements.txt
```

Register as a user on [open data platform on mobility in Switzerland](https://opentransportdata.swiss)
and request for a __API token__ on the __"GTFS-RT Beta"__ API. Place your Token at the start of the 
***"dataAnalyser.py"***. 
Here you can also define your update interval. An interval faster then every 60s is not useful, since 
the resolution of the timetable is one minute and you will run into API request limitations.
```python
APIKEY = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
INTERVAL_S = 60.0
```
## Usage

Run once (or every week when new dataset is available) to get the static timetable from 
[open data platform on mobility in Switzerland](https://opentransportdata.swiss).

```bash
python prepare_weekly_run.py
```

Run continuously to load and apply dynamic timetable changes to static data and export tram location file.

```bash
python dataAnalyser.py
```

## Additional information
- An [live visualisation](https://tramtracker.ch/map) of this data can be viewed at [tramtracker.ch](https://tramtracker.ch).
- ***current_status.csv*** lists all trams and they corresponding position. The trip destination, 
tram number and delay (minutes) is also included. 
- The file ***stop_locations.csv*** contains the coordinates of all 204 VBZ tram stops.
- All date will be save on relative paths from the current working directory.

## Author
[Ueli Schön](https://github.com/schoenu)

## License
[MIT](license.txt)
