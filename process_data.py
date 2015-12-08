import argparse
import os
import pandas as pd
from datetime import datetime

parser = argparse.ArgumentParser(description='Process USCRN raw data files into pandas dataframes and store the '
                                             'resulting frame in an HDFSTORE')
parser.add_argument('-i', '--input_dir', required=True,
                    help='full path directory containing raw USCRN files (will look in sub-directories')
parser.add_argument('-o', '--output_hdf', required=True, help='full path filename for HDFSTORE')
parser.add_argument('-s', '--station_file', required=True, help='file containing a list of stations to process')
args = parser.parse_args()

f = open(args.station_file)
stations = [line.strip('\n') for line in f]

colnames = ['WBANNO',
            'UTC_DATE',
            'UTC_TIME',
            'LST_DATE',
            'LST_TIME',
            'CRX_VN',
            'LONGITUDE',
            'LATITUDE',
            'AIR_TEMPERATURE',
            'PRECIPITATION',
            'SOLAR_RADIATION',
            'SR_FLAG',
            'SURFACE_TEMPERATURE',
            'ST_TYPE',
            'ST_FLAG',
            'RELATIVE_HUMIDITY',
            'RH_FLAG',
            'SOIL_MOISTURE_5',
            'SOIL_TEMPERATURE_5',
            'WETNESS',
            'WET_FLAG',
            'WIND_1_5',
            'WIND_FLAG']

for station in stations:
    print('Start processing: {}'.format(station))
    station_data = pd.DataFrame()
    for subdir, dirs, files in os.walk(args.input_dir):
        for file in files:
            if station in file:
                print('Reading: {}'.format(file))
                # TODO: need to define missing value for each column of data
                yearly_data = pd.read_csv(os.path.join(subdir, file),
                                          sep='\s*', header=None, names=colnames, engine='python',
                                          parse_dates={'UTC_DATETIME': ['UTC_DATE', 'UTC_TIME']},
                                          date_parser=lambda date, time: datetime.strptime(date+time, '%Y%m%d%H%M'),
                                          converters={'CRX_VN': str},
                                          index_col=0)
                station_data = pd.concat([station_data, yearly_data])
    print('Writing to disk: {}'.format(station))
    station_data.to_hdf(args.output_hdf, station, format='t', complib='blosc', complevel=9)
    print('Finished processing: {}'.format(station))

