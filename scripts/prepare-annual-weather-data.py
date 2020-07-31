import argparse, datetime, json
import tempfile, os, gzip

IN_MEMORY_DATE_LOOKAHEAD_DAYS = 10
TRAILING_DAY_COUNT = 90

def process_date(rolling_daily_data_map, process_datetime, selected_details, output_file_handles):
	print('Processing date:', process_datetime)
	station_details = selected_details['selected_stations']
	selected_elements = selected_details['selected_elements']

	op_row_cols = [str(process_datetime.date())]
	for station in sorted(station_details.keys()):
		op_row_cols += [station]

		for element in sorted(selected_elements.keys()):
			agg_details = selected_elements[element]
			_trailing_day_count = agg_details['interval']
			_aggregation_type = agg_details['agg_type']

			station_element_values = []
			for datediff in range(0, _trailing_day_count):
				pick_date = (process_datetime - datetime.timedelta(days=datediff)).date()

				try:
					value = rolling_daily_data_map[pick_date][station][element]
					if value != -9999: #unknown/invalid/not collected
						station_element_values += [value]
				except:
					continue

			if len(station_element_values) > 0:
				if _aggregation_type == 'average':
					op_row_cols += [str(round(sum(station_element_values)/len(station_element_values)))]
				elif _aggregation_type == 'sum':
					op_row_cols += [str(sum(station_element_values))]
				elif _aggregation_type == 'or_op':
					if sum(station_element_values) > 0:
						#quick test to see if there is atleast one '1'
						op_row_cols += ['1']
					else:
						op_row_cols += ['0']
				else:
					raise Exception('Unknown aggregation type: %s' % _aggregation_type)
				
			else:
				op_row_cols += ['-9999']

		op_row = ('%s\n' % ('\t'.join(op_row_cols))).encode('utf-8')
		output_file_handles[process_datetime.year].write(op_row)
		op_row_cols = [str(process_datetime.date())]



def add_data_to_memory(line_date, station, element, value, rolling_daily_data_map):
	date_map = rolling_daily_data_map.get(line_date, {})
	station_map = date_map.get(station, {})
	station_map[element] = value
	date_map[station] = station_map
	rolling_daily_data_map[line_date] = date_map

	return rolling_daily_data_map


def process_per_year_files(data_dir, start_year, num_years):
	_selected_details = retrieve_selected_details(data_dir)
	station_details = _selected_details['selected_stations']
	selected_elements = _selected_details['selected_elements']	

	rolling_daily_data_map = { }
	output_file_handles = { }

	for year in range(start_year, start_year+num_years):
		_year_file_path = '%s/%d.csv.gz' % (data_dir, year)

		with gzip.open(_year_file_path, 'rb') as f:
			print ("Reading from file: %s" % _year_file_path)
			line_count = 0

			for line in f:
				line_count += 1
				if line_count % 1000000 == 0:
					print ("Read %d lines" % line_count, ", Current read date: ", line_datetime.date() if line_datetime else 'N/A')

				line = line.decode('utf-8')
				cols = [x.strip() for x in line.split(',')]

				station = cols[0]
				if station not in station_details:
					continue

				element = cols[2]
				if element not in selected_elements:
					continue

				line_datetime = datetime.datetime.strptime(cols[1], '%Y%m%d')
				if len(rolling_daily_data_map) == 0:
					#no entries added yet. set the process datetime to be TRAILING_DAY_COUNT past last read line item
					process_datetime = line_datetime + datetime.timedelta(days=TRAILING_DAY_COUNT-1)
				else:
					min_date = min(rolling_daily_data_map.keys())
					min_datetime = datetime.datetime(min_date.year, min_date.month, min_date.day)
					process_datetime = min_datetime + datetime.timedelta(days=TRAILING_DAY_COUNT-1)

				if line_datetime >= process_datetime + datetime.timedelta(days=IN_MEMORY_DATE_LOOKAHEAD_DAYS):
					if process_datetime.year not in output_file_handles:
						if process_datetime.year-1 in output_file_handles:
							#close the previously opened handle
							output_file_handles[process_datetime.year-1].close()
							del output_file_handles[process_datetime.year-1]

						#create new handle
						output_file_handles[process_datetime.year] = prepare_output_handle(data_dir, process_datetime.year)

					process_date(rolling_daily_data_map, process_datetime, _selected_details, output_file_handles)
					min_date = min(rolling_daily_data_map.keys())
					del rolling_daily_data_map[min_date]

				value = int(cols[3])
				rolling_daily_data_map = add_data_to_memory(line_datetime.date(), station, element, value, rolling_daily_data_map)


	#process & flush what ever is left in memory as long as we can build 7-day averages
	while line_datetime >= process_datetime:
		if process_datetime.year not in output_file_handles:
			if process_datetime.year-1 in output_file_handles:
				#close the previously opened handle
				output_file_handles[process_datetime.year-1].close()
				del output_file_handles[process_datetime.year-1]

			#create new handle
			output_file_handles[process_datetime.year] = prepare_output_handle(data_dir, process_datetime.year)

		process_date(rolling_daily_data_map, process_datetime, _selected_details, output_file_handles)
		min_date = min(rolling_daily_data_map.keys())
		del rolling_daily_data_map[min_date]

		if len(rolling_daily_data_map) > 0:
			min_date = min(rolling_daily_data_map.keys())
			min_datetime = datetime.datetime(min_date.year, min_date.month, min_date.day)
			process_datetime = min_datetime + datetime.timedelta(days=TRAILING_DAY_COUNT-1)
		else:
			break #all done


	#close out any open file handles
	for handle in output_file_handles.values():
		handle.close()


def retrieve_selected_details(data_dir):
	_file_path = '%s/ghcnd-selected-stations.txt' % (data_dir)
	with open(_file_path, 'r') as f:
		_selected_details = json.loads(f.read())
	return _selected_details



def prepare_output_handle(data_dir, year):
	_dir_path = '%s/7day_avg' % data_dir
	try:
		os.mkdir(_dir_path)
	except FileExistsError as e:
		pass

	_file_path = '%s/7day_avg/%d.txt.gz' % (data_dir, year)
	try:
		os.remove(_file_path)
	except FileNotFoundError as e:
		pass

	return gzip.open(_file_path,'wb')



def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', dest='data_dir', required=True, help="Directory to find the selected stations file")
	parser.add_argument('-y', type=int, dest='start_year', required=True, help="First year to retrieve and prepare data for")
	parser.add_argument('-n', type=int, dest='num_years', required=True, help="Number of year to retrieve and prepare data for")
	args = parser.parse_args()

	data = {
		'data_dir': args.data_dir,
		'start_year': args.start_year,
		'num_years': args.num_years
	}

	process_per_year_files(**data)



main()
