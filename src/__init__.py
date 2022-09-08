import argparse
import logging
import os
import utils.kathprfi_single_file as kathp
import pandas as pd


INTERNAL_CONFIG = "resources/config/privateConfig.kvp"

UNKNOWN_CORRELATOR_MODE_MESSAGE_FORMAT = "Unknown correlator value '%s' is not one of %s"
ADD_FILE_MESSAGE_FORMAT = 'Adding file {} : {}'
REMOVE_BAD_ANTENNA_MESSAGE = 'Removing bad antennas'
REMOVED_BAD_ANTENNA_MESSAGE = 'Bad antennas has been removed.'
GOOD_FLAGS_MESSAGE = 'Good flags has been returned'
START_UPDATE_ARRAY_MESSAGE = 'Start to update the master and counter array'
UPDATE_TIME_MESSAGE_FORMAT = '{} s has been taken to update file number {}'
CREATE_XRAY_MESSAGE = 'Creating Xarray Dataset'
SAVED_DATASET_MESSAGE = 'Dataset has been saved'
SELECTION_PROBLEM_MESSAGE_FORMAT = '{} selection has a problem'
CHANNEL_PROBLEM_MESSAGE = 'Channel/dump has a problem'
FILE_SAVED_MESSAGE = 'File has been saved'
SAVING_DATASET_MESSAGE = 'Saving dataset'
DESCRIPTION_MESSAGE = 'MEERKAT HISTORICAL PROBABILITY OF RADIO FREQUENCY INTERFERENCE FRAMEWORK'
FILE_NUMBER_READ_FORMAT = 'File number {} has been read'

#Error codes
FAILED_TO_INITILISE_ERROR_CODE = 1

def initialize_logs():
    """
    Initialize the log settings
    """
    logging.basicConfig(format='%(message)s', level=logging.INFO)


def create_parser():
    parser = argparse.ArgumentParser(description='This package produces two 5-D arrays, '
                                                 'which are the counter array and the master array.'
                                                 'The arrays provides statistics about measured'
                                                 'RFI from MeerKAT telescope.')
    parser.add_argument('-c', '--config', action='store', type=str,
                       help='A config file that does subselction of data')
    parser.add_argument('-b', '--bad', action='store',  type=str,
                        help='Path to save list of bad files')
    parser.add_argument('-g', '--good', action='store', type=str, default='\tmp',
                        help='Path to save bad files')
    parser.add_argument('-z', '--zarr', action='store', type=str, default='\tmp',
                        help='path to save output zarr file')
    return parser

# Initializing the log settings
initialize_logs()
logging.info(DESCRIPTION_MESSAGE)
parser = create_parser()
args = parser.parse_args()
path2config = os.path.abspath(args.config)
# Read in dictionary with keys and values from config file
config = kathp.config2dic(path2config)
internalConfig = kathp.config2dic(INTERNAL_CONFIG)
# Read in csv file with files to process
filename = config['filename']
data = pd.read_csv(filename)
correlator_mode = config['correlator_mode']
try:
    freq_chan = internalConfig[correlator_mode]
except KeyError:
    logging.error(UNKNOWN_CORRELATOR_MODE_MESSAGE_FORMAT.format(correlator_mode, internalConfig.keys()))
    exit(FAILED_TO_INITILISE_ERROR_CODE)


