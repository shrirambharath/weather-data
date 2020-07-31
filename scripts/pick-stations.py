import argparse, datetime, os, json
import os.path

from os import path

CORE_TRACKING_ELEMENTS = {
	'AWND': { 'agg_type': 'average', 'interval': 7 },
	'PRCP': { 'agg_type': 'sum', 'interval': 90 },
	'SNOW': { 'agg_type': 'sum', 'interval': 90 },
	'SNWD': { 'agg_type': 'sum', 'interval': 90 },
	'TAVG': { 'agg_type': 'average', 'interval': 7 },
	'TMAX': { 'agg_type': 'average', 'interval': 7 },
	'TMIN': { 'agg_type': 'average', 'interval': 7 },
	'WT08': { 'agg_type': 'or_op', 'interval': 7 },
	'WT11': { 'agg_type': 'or_op', 'interval': 7 }
}

def get_selected_stations_filename(data_dir, append_date):	
	_selected_stations_file = '%s/ghcnd-selected-stations.txt' % (data_dir)
	if path.exists(_selected_stations_file):
		#rename the file
		_rename_path = '%s.%s' % (_selected_stations_file, append_date)
		if path.exists(_rename_path):
			os.remove(_rename_path)
		os.rename(_selected_stations_file, _rename_path)

	return _selected_stations_file



def filter_stations(station_details):
	selected_station_details = { }	
	for _station in sorted(station_details.keys()):
		(_lat, _lon, element_set, _start_year, _end_year, _name) = station_details[_station]
		# select stations that track one or more of teh core tracking elements. Discard other stations and elements
		if len(element_set & CORE_TRACKING_ELEMENTS.keys()) == 0:
			continue

		_m = { 'lat': _lat, 'lon': _lon, 'station': _station, 'elements': [x for x in sorted(element_set & CORE_TRACKING_ELEMENTS.keys())], \
			'start': _start_year, 'latest': _end_year, 'name': _name}
		selected_station_details[_station] = _m

	return selected_station_details



def pick_stations(data_dir, start_year):
	# formats can be found here - https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/readme.txt
	_station_file_path = '%s/ghcnd-stations.txt' % (data_dir)
	_station_name = { }
	for line in open(_station_file_path):
		# select stations that have a state (these are US/CA states/territories)
		_station = line[0:11].strip()
		_state = line[38:40].strip()
		_name = line[41:71].strip()
		if len(_state) > 0:
			_station_name[_station] = _name	


	_inventory_file_path = '%s/ghcnd-inventory.txt' % (data_dir)
	_today = datetime.datetime.today()
	_station_details = { }
	for line in open(_inventory_file_path):
		_station = line[0:11].strip()
		_lat = float(line[12:20].strip())
		_lon = float(line[21:30].strip())
		_element = line[31:35]
		_start_year = int(line[36:40].strip())
		_end_year = int(line[41:45].strip())

		# select stations in US/CA 
		if _station in _station_name and _start_year < start_year and _end_year == _today.year:
			_name = _station_name.get(_station, 'N/A')

			(_, _, element_set, _, _, _) = _station_details.get(_station, (None, None, set(), None, None, None))
			element_set.add(_element)
			_station_details[_station] = (_lat, _lon, element_set, _start_year, _end_year, _name)
	

	_append_date = _today.strftime("%Y%m%d")
	_selected_station_details= filter_stations(_station_details)	

	result_map = { 'selected_stations': _selected_station_details, 
					'selected_elements': CORE_TRACKING_ELEMENTS,
				 }
	_selected_stations_filename = get_selected_stations_filename(data_dir, _append_date)
	with open(_selected_stations_filename, 'w') as _selected_stations_handle:
		_selected_stations_handle.write(json.dumps(result_map))


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', dest='data_dir', required=True, help="Directory to find the files ghcnd-inventory.txt, ghcnd-stations.txt")
	parser.add_argument('-s', type=int, default=2000, dest='start_year', required=False, help="Year from which active data is being collected")
	args = parser.parse_args()

	data_dir = args.data_dir
	start_year = args.start_year
	print('Picked Dir: %s' % data_dir)
	print('Picked Year: %s' % start_year)
	pick_stations(data_dir, start_year)


main()
