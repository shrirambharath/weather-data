import argparse, datetime, os, json, statistics
import os.path, gzip, sys

from os import path

UNAVAILABLE_DATA_POINT = -9999

def generate_const_zero_list():
	last_val = '*'
	s = { '': last_val }
	for i in range(0,25):
		next_val = '%s,*' % last_val
		s[last_val] = next_val
		last_val = next_val
	return s
CONST_ZERO_LOOKUP = generate_const_zero_list()



def get_size(obj, seen=None):
	"""Recursively finds size of objects"""
	size = sys.getsizeof(obj)
	if seen is None:
		seen = set()
	obj_id = id(obj)
	if obj_id in seen:
		return 0
	# Important mark as seen *before* entering recursion to gracefully handle
	# self-referential objects
	seen.add(obj_id)
	if isinstance(obj, dict):
		size += sum([get_size(v, seen) for v in obj.values()])
		size += sum([get_size(k, seen) for k in obj.keys()])
	elif hasattr(obj, '__dict__'):
		size += get_size(obj.__dict__, seen)
	elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
		size += sum([get_size(i, seen) for i in obj])
	return size



def parse_line(line, selected_elements):
	try:
		cols = [x.strip() for x in line.decode('utf-8').split()]
		date = datetime.datetime.strptime(cols[0], '%Y-%m-%d')

		columns = { 
			'DATE': date,
			'WOY': date.isocalendar()[1],
			'DOW': date.weekday(),
			'STATION': cols[1],
		}
		index = 2
		for k in sorted(selected_elements.keys()):
			columns[k] = int(cols[index])
			index += 1
		return columns
	except Exception as e:
		return None


def retrieve_selected_details(data_dir):
	_file_path = '%s/ghcnd-selected-stations.txt' % (data_dir)
	with open(_file_path, 'r') as f:
		_selected_details = json.loads(f.read())
	return _selected_details


def interpret(element, data, selected_elements):
	aggregation_type = selected_elements[element]['agg_type']
	expected_data_pts = selected_elements[element]['interval']

	trimmed_data = [x for x in data if x != UNAVAILABLE_DATA_POINT]
	perc_data_points = float(len(data)) / float(expected_data_pts)

	has_anomaly = None
	anomaly_details = { }
	if aggregation_type == 'average' or aggregation_type == 'sum':
		if perc_data_points < 0.33:
			#less than a third of the expected points - too few points to interpret
			has_anomaly = False
		else:
			mean = statistics.mean(trimmed_data)
			stddev = statistics.pstdev(trimmed_data)

			_1sigma = [x for x in data if x != UNAVAILABLE_DATA_POINT and abs(x-mean) > stddev]
			_2sigma = [x for x in data if x != UNAVAILABLE_DATA_POINT and abs(x-mean) > (2*stddev)]
			_3sigma = [x for x in data if x != UNAVAILABLE_DATA_POINT and abs(x-mean) > (3*stddev)]
			if len(_2sigma) > 0 or len(_3sigma) > 0:
				anomaly_details = {
					'element': element,
					'points': data,
					'trimmed_points': trimmed_data,
					'mean': mean,
					'std_dev': stddev,
					'_1sigma': _1sigma,
					'_2sigma': _2sigma,
					'_3sigma': _3sigma,
					'agg_type': aggregation_type,
					'interval': expected_data_pts,
					'perc_points': perc_data_points
				}
				has_anomaly = True
			else:
				has_anomaly = False

	elif aggregation_type == 'or_op':
		#boolean here (1/-9999). So perc points has no bearing. if there is a '1' fewer than 20% of the time, the 1 is
		#treated as an anomaly
		if perc_data_points < 0.2:
			has_anomaly = True
			anomaly_details = {
				'element': element,
				'points': data,
				'trimmed_points': trimmed_data,
				'agg_type': aggregation_type,
				'interval': expected_data_pts,
				'perc_points': perc_data_points
			}
		else:
			has_anomaly = False

	else:
		raise Exception('Unknown aggregation_type: %s' % aggregation_type)

	return has_anomaly, anomaly_details


def pick_anomalies(data_dir, start_year, end_year, tracked_station_count):
	_selected_details = retrieve_selected_details(data_dir)
	selected_elements = _selected_details['selected_elements']	

	_tracked_stations = set()
	_woy_station_data = { }
	ignore_keys = set(['DATE','WOY','STATION','DOW'])

	for year in range(start_year, end_year+1):
		filepath = '%s/7day_avg/%d.txt.gz' % (data_dir, year)
		print('Reading %s' % filepath)
		line_count = 0

		with gzip.open(filepath, 'r') as f:
			for line in f:
				columns = parse_line(line, selected_elements)
				if columns == None:
					continue

				line_count += 1
				if line_count % 250000 == 0:
					print('Read %d lines' % (line_count))

				#track weekly on Sundays '6'
				if columns['DOW'] != 6:
					continue

				#respect the tracking limits
				if tracked_station_count <= 0 or \
					len(_tracked_stations) < tracked_station_count or \
					(len(_tracked_stations) == tracked_station_count and columns['STATION'] in _tracked_stations):

					station_map = _woy_station_data.get(columns['WOY'], { })
					data_map = station_map.get(columns['STATION'], { })
					for element in columns:

						if element in ignore_keys:
							continue

						l_str = data_map.get(element, '')
						if columns[element] == UNAVAILABLE_DATA_POINT and l_str in CONST_ZERO_LOOKUP:
							str_rep = CONST_ZERO_LOOKUP[l_str] #use pre-calculated all '0' string with additional '0'
						else:
							if len(l_str) == 0:
								str_rep = '%d' % (columns[element])
							else:
								str_rep = '%s,%d' % (l_str, columns[element])
						data_map[element] = str_rep

					station_map[columns['STATION']] = data_map
					_woy_station_data[columns['WOY']] = station_map


					#do something
					if tracked_station_count > 0:
						_tracked_stations.add(columns['STATION'])

	
	print('Scanning for anomalies')
	anomalies = []
	for woy in sorted(_woy_station_data.keys()):
		for station in sorted(_woy_station_data[woy].keys()):
			for element in sorted(_woy_station_data[woy][station].keys()):
				try:
					point_str = _woy_station_data[woy][station][element].replace('*','%d' % UNAVAILABLE_DATA_POINT)
					data = [int(x.strip()) for x in point_str.split(',')]
					if len([x for x in data if x != UNAVAILABLE_DATA_POINT]) == 0:
						continue

					has_anomaly, interpretation_details = \
						interpret(element, data, selected_elements)

					if has_anomaly:
						interpretation_details['WOY'] = woy
						interpretation_details['Station'] = station
						anomalies += [interpretation_details]
						break

				except ValueError as e:
					print('Issue with: ', _woy_station_data[woy][station][element], ', Exception: ', e)

	op_filepath = '%s/anomalies.txt' % data_dir
	with open(op_filepath, 'w') as f:
		for a in anomalies:
			f.write('%s\n' % json.dumps(a))





def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', dest='data_dir', required=True, help="Directory to find the 7-day avg directory & ghcnd-selected-stations.txt")
	parser.add_argument('-s', type=int, default=2000, dest='start_year', required=False, help="Year from which start")
	parser.add_argument('-e', type=int, required=True, dest='end_year', help="Year end")
	parser.add_argument('-n', type=int, default=-1, dest='num_stations', required=False, help="Limit the number of stations")	
	args = parser.parse_args()

	data = {
		'data_dir': args.data_dir,
		'start_year': args.start_year,
		'end_year': args.end_year,
		'tracked_station_count': args.num_stations,
	}

	print('Working w/ Dir: %s' % data['data_dir'])
	print('Working w/ Start Year: %d' % data['start_year'])
	print('Working w/ End Year: %d' % data['end_year'])
	print('Working w/ Num stations: %s' % ('Unlimited' if data['tracked_station_count'] < 0 else '%d' % data['tracked_station_count']))

	pick_anomalies(**data)

main()