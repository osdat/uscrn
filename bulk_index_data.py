import argparse
import os
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

parser = argparse.ArgumentParser(description='Process USCRN raw data files into elasticsearch')

parser.add_argument('-i', '--input_dir', required=True,
                    help='full path directory containing raw USCRN files (will look in sub-directories')
parser.add_argument('-s', '--station_file', required=True, help='file containing a list of stations to process')
args = parser.parse_args()

f = open(args.station_file)
stations = [line.strip('\n') for line in f]

es = Elasticsearch()

for station in stations:
    print('Start processing: {}'.format(station))
    for subdir, dirs, files in os.walk(args.input_dir):
        for file in files:
            if station in file:
                print('Reading: {}'.format(file))
                with open(os.path.join(subdir, file)) as f:
                    actions = []
                    for line in f:
                        data = line.split()
                        action = {
                            '_index': 'station_data',
                            '_type': 'uscrn',
                            '_source': {
                                'STATION_NAME': station,
                                'UTC_DATETIME': datetime.strptime(data[1]+data[2], '%Y%m%d%H%M'),
                                'LST_DATETIME': datetime.strptime(data[3]+data[4], '%Y%m%d%H%M'),
                                'WBANNO': data[0],
                                'CRX_VN': data[5],
                                'LONGITUDE': float(data[6]),
                                'LATITUDE': float(data[7]),
                                'AIR_TEMPERATURE': float(data[8]),
                                'PRECIPITATION': float(data[9]),
                                'SOLAR_RADIATION': float(data[10]),
                                'SR_FLAG': int(data[11]),
                                'SURFACE_TEMPERATURE': float(data[12]),
                                'ST_TYPE': data[13],
                                'ST_FLAG': int(data[14]),
                                'RELATIVE_HUMIDITY': float(data[15]),
                                'RH_FLAG': int(data[16]),
                                'SOIL_MOISTURE_5': float(data[17]),
                                'SOIL_TEMPERATURE_5': float(data[18]),
                                'WETNESS': int(data[19]),
                                'WET_FLAG': int(data[20]),
                                'WIND_1_5': float(data[21]),
                                'WIND_FLAG': int(data[22])
                            }
                        }
                        actions.append(action)
                    print('Writing to ES: {}'.format(file))
                    helpers.bulk(es, actions)
