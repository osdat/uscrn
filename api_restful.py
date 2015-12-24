import argparse
import numpy as np
import pandas as pd
from collections import OrderedDict
from datetime import datetime
from flask import Flask
from flask_restful import fields, marshal
from flask.ext.restful import Api, Resource, reqparse
from flask.ext.cors import CORS


def replace_nans(lst):
    """Given a list of numbers, replace NaN's with None"""
    return [None if np.isnan(v) else v for v in lst]

app = Flask(__name__)
CORS(app)
api = Api(app)

# load data
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--hdf_file', required=True,
                    help='HDFStore to use as api data source')
args = parser.parse_args()
store = pd.HDFStore(args.hdf_file)


class Stations(Resource):
    def get(self):
        # TODO: store.keys() is quite slow....research a faster method
        station_names = sorted([hdf_path.split('/')[1] for hdf_path in store.keys() if hdf_path.endswith('raw')])
        resource_fields = {'stations': fields.List(fields.String)}
        data = {'stations': station_names}

        return marshal(data, resource_fields)


class Station(Resource):
    def get(self, station_name):
        # TODO: also way too slow...plus my logic is a bit silly. Don't need to reread the dataframe for each variable
        timesteps = ['5min', '1hour', '1day']
        stats = ['raw', 'sum', 'min', 'mean', 'max']

        hdf_name = '/{}/res5min/raw'.format(station_name)
        variables = list(store[hdf_name].columns.values)

        data = {}
        for variable in variables:
            data[variable] = {}
            for timestep in timesteps:
                data[variable][timestep] = []
                for stat in stats:
                    hdf_name = '/{}/res{}/{}'.format(station_name, timestep, stat)
                    try:
                        if variable in store[hdf_name].columns:
                            data[variable][timestep].append(stat)
                    except:
                        continue

        return data


class StationQuery(Resource):
    def get(self, station_name, variable):

        # parse the request for required strings
        parser = reqparse.RequestParser()
        parser.add_argument('start')
        parser.add_argument('stop')
        parser.add_argument('timestep')
        parser.add_argument('stat')
        args = parser.parse_args()

        timestep = 'res' + args['timestep']
        stat = args['stat']

        # convert start/stop strings to datetime objects
        start = datetime.strptime(args['start'], '%Y%m%d%H%M')
        stop = datetime.strptime(args['stop'], '%Y%m%d%H%M')

        # construct dataframe id to read from hdfstore
        df_id = '/{}/{}/{}'.format(station_name, timestep, stat)

        # get the requested data and return dates/values in a list
        queried_df = store[df_id].loc[start:stop]
        queried_dates = queried_df.index.tolist()
        queried_values = queried_df[variable].tolist()
        queried_values_type = queried_df[variable].dtype

        # values field type will depend on variable requested
        if issubclass(queried_values_type.type, np.float):
            value_type = fields.Float
            # replace NaNs with None for marshalling
            queried_values = replace_nans(queried_values)
        elif issubclass(queried_values_type.type, np.integer):
            value_type = fields.Integer
            # replace NaNs with None for marshalling
            queried_values = replace_nans(queried_values)
        elif issubclass(queried_values_type.type, np.object):
            value_type = fields.String

        # control order of response with OrderedDict
        # control formatting of each resource field
        resource_fields = OrderedDict([('station_name', fields.String),
                                       ('variable', fields.String),
                                       ('stat', fields.String),
                                       ('timestep', fields.String),
                                       ('uri', fields.Url('station_data')),
                                       ('dates', fields.List(fields.DateTime(dt_format='iso8601'))),
                                       ('values', fields.List(value_type))])

        data = {'station_name': station_name,
                'variable': variable,
                'stat': stat,
                'timestep': timestep,
                'dates': queried_dates,
                'values': queried_values}

        return marshal(data, resource_fields)


api.add_resource(Stations, '/api/v1.0/stations')
api.add_resource(Station, '/api/v1.0/stations/<station_name>')
api.add_resource(StationQuery, '/api/v1.0/stations/<station_name>/<variable>', endpoint='station_data')

if __name__ == "__main__":
    app.run(port=80)
