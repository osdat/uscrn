import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--hdf_file', required=True)
args = parser.parse_args()

datastore = pd.HDFStore(args.hdf_file)
print(datastore)



