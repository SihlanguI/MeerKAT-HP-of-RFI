#!/usr/bin/env python3
import argparse
import ast
import logging
import os
import six
import time as tme
import time
import numpy as np
import pandas as pd
import xarray as xr
import zarr
import kathprfi_single_file as kathp
import numba
from numba import prange

start_time = time.time()
def initialize_logs():
    """
    Initialize the log settings
    """
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    logging.basicConfig(filename = 'kathprfi_logs.log', format='%(message)s', level=logging.INFO)


def create_parser():
    parser = argparse.ArgumentParser(description='This package produces two 5-D arrays, '
            'which are the counter array and the master array.'
            'The arrays provides statistics about measured'
            'RFI from MeerKAT telescope.')
    #define the default values for the configuration
    DEFAULT_OUTPUT_DIR  = "/home/kvanqa/ALL_WORK/kvanqa/RFI_work/"
    DEFAULT_FILE_PATH = "/home/kvanqa/ALL_WORK/kvanqa/RFI_work/sci_Imaging_L_2024-04-01T00:00:00Z_2024-04-30T00:00:00Z.csv"

    parser.add_argument('-b', '--bad', action='store',  type=str,
                        help='Path to save list of bad files')
    parser.add_argument('-g', '--good', action='store', type=str, default='\tmp',
                        help='Path to save bad files')
    parser.add_argument('-z', '--zarr', action='store', type=str, default=DEFAULT_OUTPUT_DIR,
                        help='path to save output zarr file')
    parser.add_argument('--filename', action='store', type=str, default=DEFAULT_FILE_PATH,
                        help='Path to the CSV file')
    parser.add_argument('-p', '--pol', type=str, choices=['HH','HV','VH','VV'], help='polarization of interest')
    parser.add_argument('-s','--scan', type=str, default='track', help='observation scan')
    parser.add_argument('--corrprod', type=str, default='cross', help='add correlation product')
    parser.add_argument('--flag_type', type=str, choices=['cal_rfi', 'ingest_rfi','data_lost','cam'], default='cal_rfi', help='flag type of interest')

    return parser
   
def main():
     # Initializing the log settings
     
    initialize_logs()
    logging.info('MEERKAT HISTORICAL PROBABILITY OF RADIO FREQUENCY INTERFERENCE FRAMEWORK')
    # Configuration dictionary directly in the script
    parser = create_parser()
    args = parser.parse_args()
    pol = args.pol
    corrprod = args.corrprod
    scan = args.scan
    flag_type = args.flag_type
    filename_path = args.filename
    data = pd.read_csv(filename_path)
    filename = data['FullLink'].values
    # Read in csv file with files to process
    badfiles = []
    goodfiles = []
    #@jit(parallel=True)

    for i in range(len(filename)):
        # Initializing 5-D arrays
        master = np.zeros((24, 4096, 2016, 8, 24), dtype=np.uint16)
        counter = np.zeros((24, 4096, 2016, 8, 24), dtype=np.uint16)
        s = tme.time()
        logging.info('Adding file {} : {}'.format(i, filename[i]))
        try:
            pathvis = filename[i]
            vis = kathp.readfile(pathvis)
            logging.info('File number {} has been read'.format(i))
            #import pdb; pdb.set_trace()
            if len(vis.freqs) == 4096:

                logging.info('Removing bad antennas')
                clean_ants = kathp.remove_bad_ants(vis)
                logging.info('Bad antennas has been removed.')
                good_flags = kathp.selection(vis, pol=pol, corrprod=corrprod, scan=scan,
                                                clean_ants=clean_ants, flag_type=flag_type)
                logging.info('Good flags has been returned')
         
                if good_flags.shape[0] * good_flags.shape[1] * good_flags.shape[2] != 0:
                    # Updating the array
                    ntime = good_flags.shape[0]
                    time_step = 1
                    if ntime <= time_step:
                        time_step = ntime
                    nant = 64
                    Bl_idx = kathp.get_bl_idx(vis, nant)
                    elbins = np.linspace(10, 80, 8)
                    azbins = np.arange(0, 360, 15)
                    el, az = kathp.get_az_and_el(vis)
                    logging.info('Start to update the master and counter array')
                    for tm in six.moves.range(0, ntime, time_step):
                        time_slice = slice(tm, tm + time_step)
                        flag_chunk = good_flags[time_slice].astype(int)
                        # average flags from 32k to 4k mode.
                        if good_flags.shape[1] == 32768:
                            flag_chuck = kathp.NewFlagChunk(flag_chunk)
                        Time_idx = kathp.get_time_idx(vis)[time_slice]
                        El_idx = kathp.get_el_idx(el, elbins)[time_slice]
                        Az_idx = kathp.get_az_idx(az, azbins)[time_slice]
                        master, counter = kathp.update_arrays(Time_idx, Bl_idx, El_idx, Az_idx,
                                                                flag_chunk, master, counter)
                    logging.info('{} s has been taken to update file number {}'.format(i,
                                                                                        tme.time()
                                                                                        - s))
                    goodfiles.append(filename[i])
                    logging.info('Creating Xarray Dataset')
                    ds = xr.Dataset({'master': (('time', 'frequency', 'baseline', 'elevation',
                                                    'azimuth'), master),
                    'counter': (('time', 'frequency', 'baseline', 'elevation', 'azimuth'), counter)},
                    {'time': np.arange(24), 'frequency': vis.freqs, 'baseline': np.arange(2016),
                        'elevation': np.linspace(10, 80, 8), 'azimuth': np.arange(0, 360, 15)})
                    logging.info('Saving dataset')
                    #name, ext = os.path.splitext(args.zarr)
                    #flname = name+str(filename[i][46:56])+ext
                    #ds.to_zarr(str(flname[46:56])+ ".zarr",group='arr')
		    flname = os.path.join(os.getcwd(), f"{U}_{pol}_{filename[i][46:56]}.zarr")
                    ds.to_zarr(flname, group='arr')
                    logging.info('Dataset has been saved')
                else:
                    logging.info('{} selection has a problem'.format(filename[i]))
                    badfiles.append(filename[i])
                    pass
            else:
                logging.info('{} selection has a problem'.format(filename[i]))
                badfiles.append(filename[i])
                pass

            np.save(args.good,goodfiles)
            np.save(args.bad,badfiles)
            logging.info('File has been saved')
        

        except Exception as e:
            logging.info(e)
            continue


if __name__=="__main__":
    main()
    #print(f"Zarr path: {main().args.zarr}" )

#calculate the program's run Timetime
end_time = time.time()
print(f"program's runtime {(end_time - start_time)/60.}")

''' when running the script you simply parse the arguments in the following manner
ipython kathprfi_tester.py -- -z . --filename katfprfi_cvs/sci_Imaging_L_2024-01-01T00:00:00Z_2024-01-31T00:00:00Z.csv -p 'HH' -s 'track' --corrprod 'cross' --flag_type 'cal_rfi'

'''
                   
