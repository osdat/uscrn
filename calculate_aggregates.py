import numpy as np
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description="""
    Calculate aggregation summaries for 5-minute output from "process_data.py".

    Currently only hourly and daily aggregates are created.  Aggregation is
    performed naively, without taking into account quality flags or number
    of non-missing observations.  Daily aggregates are calculated using the
    station's local day, not UTC.

    The source of the 5-minute data also provides daily and hourly
    datasets here: https://www.ncdc.noaa.gov/crn/qcdatasets.html
    However, the methodology in this script will not necessarily yield
    compatible results.
    """)
parser.add_argument('-i', '--input_hdf', required=True,
                    help='full path filename for HDFSTORE output by "process_data.py')
parser.add_argument('-o', '--output_hdf', required=True,
                    help='full path filename for aggregated HDFSTORE')
parser.add_argument('-e', '--exclude_raw', action='store_true',
                    help='flag for excluding the raw 5-min data in output')
args = parser.parse_args()

inputstore = pd.HDFStore(args.input_hdf)
outputstore = pd.HDFStore(args.output_hdf)


# -------------------------------------------------------------------
# Specify which variables and summary functions to compute

# All variables to aggregate
ALL_MEASURE_VARS = [
    'AIR_TEMPERATURE',
    'PRECIPITATION',
    'SOLAR_RADIATION',
    'SURFACE_TEMPERATURE',
    'RELATIVE_HUMIDITY',
    'SOIL_MOISTURE_5',
    'SOIL_TEMPERATURE_5',
    'WETNESS',
    'WIND_1_5'
]

# Summary functions to compute by default
DEFAULT_SUMMARY_FUNCS = {
    'mean': np.mean,
    'min': np.min,
    'max': np.max
}

# Variables to compute sum for in addition to default summaries
SUM_VARS = ['PRECIPITATION']


# -------------------------------------------------------------------
# Calculate summaries and output to HDFStore

# Retrieve all station keys
stationkeys = inputstore.keys()

print('Calculating aggregates:')

for stationkey in stationkeys:
    rawdf = inputstore[stationkey]

    # Output raw 5-minute summary unless exclude_raw flag was set
    if not args.exclude_raw:
        rawkey = stationkey + '/res5min/raw'
        outputstore.put(rawkey, rawdf,
                        format='t', complib='blosc', complevel=9)

    # Augment rawdf with 'UTC_DTTM_HOUR' and 'LOCAL_DATE' grouping columns
    dfrm = rawdf.copy()
    # Shift times to denote start of 5-min interval for correct hour assignment
    utc_5min_start = dfrm.index.values - np.timedelta64(5, 'm')
    utc_hour_start = utc_5min_start.astype('<M8[h]')
    # Shift time to end of hour interval to follow interval ending convention
    dfrm['UTC_DTTM_HOUR'] = utc_hour_start + np.timedelta64(1, 'h')
    # Parse local date, again shifting to start of 5-min interval
    local_date_str = dfrm.LST_DATE.values.astype(str)
    local_date = pd.to_datetime(local_date_str, format='%Y%m%d')
    day_shift_mask = (dfrm.LST_TIME == 0).values.astype('<m8[D]')
    dfrm['LOCAL_DATE'] = local_date - day_shift_mask

    # Group by hour and day
    grouped_hour = dfrm.groupby('UTC_DTTM_HOUR')
    grouped_day = dfrm.groupby('LOCAL_DATE')

    # Define function to calculate and output hourly and daily results for
    # the given summary_name, summary_func, and measure_vars
    def calculate_and_output_summaries(summary_name, summary_func,
                                       measure_vars):
        hourdf = grouped_hour[measure_vars].agg(summary_func)
        hourkey = stationkey + '/res1hour/' + summary_name
        outputstore.put(hourkey, hourdf,
                        format='t', complib='blosc', complevel=9)
        daydf = grouped_day[measure_vars].agg(summary_func)
        daykey = stationkey + '/res1day/' + summary_name
        outputstore.put(daykey, daydf,
                        format='t', complib='blosc', complevel=9)

    # Calculate and output all default summary functions
    for summary_name, summary_func in DEFAULT_SUMMARY_FUNCS.items():
        calculate_and_output_summaries(summary_name, summary_func,
                                       ALL_MEASURE_VARS)
    # Calculate and output sum variables
    calculate_and_output_summaries('sum', np.sum, SUM_VARS)

    print('Finished processing:', stationkey)

outputstore.close()
inputstore.close()
print('Finished processing all stations!')

