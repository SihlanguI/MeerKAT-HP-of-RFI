#!/usr/bin/env python3
import logging
import os
import six
import time as tme

import numpy as np
import pandas as pd
import xarray as xr

import utils.kathprfi_single_file as kathp
from src import initialize_logs, create_parser, INTERNAL_CONFIG, UNKNOWN_CORRELATOR_MODE_MESSAGE_FORMAT, \
    ADD_FILE_MESSAGE_FORMAT, REMOVE_BAD_ANTENNA_MESSAGE, REMOVED_BAD_ANTENNA_MESSAGE, GOOD_FLAGS_MESSAGE, \
    START_UPDATE_ARRAY_MESSAGE, UPDATE_TIME_MESSAGE_FORMAT, CREATE_XRAY_MESSAGE, SAVED_DATASET_MESSAGE, \
    SELECTION_PROBLEM_MESSAGE_FORMAT, CHANNEL_PROBLEM_MESSAGE, FILE_SAVED_MESSAGE, SAVING_DATASET_MESSAGE, \
    DESCRIPTION_MESSAGE, config, internalConfig, data, FILE_NUMBER_READ_FORMAT, freq_chan, args


def main():

    # Get values from the dictionary
    name_col = config['name_col']
    corrpro = config['corrprod']
    scans = config['scan']
    flags = config['flag_type']
    pol = config['pol_to_use']
    dump_rate = int(config['dump_period'])
    correlator_mode = config['correlator_mode']

    f = data[name_col].values
    badfiles = []
    goodfiles = []
    for i in range(len(f)): #todo: if we are processing MS files, it would be faster to use the built in SQL to perform data selections
        # Initializing 5-D arrays
        master = np.zeros((24, 4096, 2016, 8, 24), dtype=np.uint16)
        counter = np.zeros((24, 4096, 2016, 8, 24), dtype=np.uint16)
        s = tme.time()
        logging.info(ADD_FILE_MESSAGE_FORMAT.format(i, f[i]))
        try:
            pathvis = f[i]
            vis = kathp.readfile(pathvis)
            logging.info(FILE_NUMBER_READ_FORMAT.format(i))
            
            if len(vis.freqs) == freq_chan and vis.dump_period > (dump_rate-1) and vis.dump_period <= dump_rate:
                logging.info(REMOVE_BAD_ANTENNA_MESSAGE)
                clean_ants = kathp.remove_bad_ants(vis)
                logging.info(REMOVED_BAD_ANTENNA_MESSAGE)
                good_flags = kathp.selection(vis, pol_to_use=pol, corrprod=corrpro, scan=scans,
                                             clean_ants=clean_ants, flag_type=flags)
                logging.info(GOOD_FLAGS_MESSAGE)
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
                    logging.info(START_UPDATE_ARRAY_MESSAGE)
                    for tm in six.moves.range(0, ntime, time_step):
                        time_slice = slice(tm, tm + time_step)
                        flag_chunk = good_flags[time_slice].astype(int)
                        # average flags from 32k to 4k mode.
                        if correlator_mode == '32k':
                            flag_chuck = kathp.NewFlagChunk(flag_chunk)
                        Time_idx = kathp.get_time_idx(vis)[time_slice]
                        El_idx = kathp.get_el_idx(el, elbins)[time_slice]
                        Az_idx = kathp.get_az_idx(az, azbins)[time_slice]
                        master, counter = kathp.update_arrays(Time_idx, Bl_idx, El_idx, Az_idx,
                                                              flag_chunk, master, counter)
                    logging.info(UPDATE_TIME_MESSAGE_FORMAT.format(i, tme.time() - s))
                    goodfiles.append(f[i])
                    logging.info(CREATE_XRAY_MESSAGE)
                    ds = xr.Dataset({'master': (('time', 'frequency', 'baseline', 'elevation',
                                                 'azimuth'), master),
                   'counter': (('time', 'frequency', 'baseline', 'elevation', 'azimuth'), counter)},
                   {'time': np.arange(24), 'frequency': vis.freqs, 'baseline': np.arange(2016),
                       'elevation': np.linspace(10, 80, 8), 'azimuth': np.arange(0, 360, 15)})
                    logging.info(SAVING_DATASET_MESSAGE)
                    name, ext = os.path.splitext(args.zarr)
                    flname = name+str(f[i][46:56])+ext
                    ds.to_zarr(flname, group='arr')
                    logging.info(SAVED_DATASET_MESSAGE)
                else:
                    logging.info(SELECTION_PROBLEM_MESSAGE_FORMAT.format(f[i]))
                    badfiles.append(f[i])
                    pass
            else:
                logging.info(CHANNEL_PROBLEM_MESSAGE)
                badfiles.append(f[i])
                pass
            np.save(args.good,goodfiles)
            np.save(args.bad,badfiles)
            logging.info(FILE_SAVED_MESSAGE)
            

        except Exception as e:
            logging.info(e)
            continue


if __name__=="__main__":
    main()
                   