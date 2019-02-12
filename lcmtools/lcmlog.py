#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from lcm import EventLog
import datetime


def log_metadata(lcm_log_path):
    """
    This function returns a Pandas DataFrame object of LCM Log Metadata
    :param lcm_log_path: Path to LCM Log File
    :return: Pandas Dataframe of metadata
    """
    lcmlog = EventLog(lcm_log_path, mode='r')
    df = []
    idx = 0

    for e in lcmlog:
        df.append((lcm_log_path, idx, e.eventnum, e.channel, e.timestamp))
        idx = lcmlog.tell()

    lcmlog.close()
    colnames = ['logfile', 'fpos', 'eventnum', 'channel', 'timestamp']

    return pd.DataFrame(df, columns=colnames)


def print_log_summary(df):
    """
    Print some summary for the lcm logfile given a pandasDataFrame
    :param df: pandas.DataFrame crated in log_metadata
    :return: None
    """
    df = pd.DataFrame(df)
    total_events = df.shape[0]
    print("\nLogfile: %s , %d total events\n" % (df['logfile'][0], total_events))

    first_time = datetime.datetime.fromtimestamp(df['timestamp'].min()/1E6)
    last_time = datetime.datetime.fromtimestamp(df['timestamp'].max()/1E6)
    duration = last_time - first_time
    print("First Time: %s" % first_time)
    print("Last Time: %s" % last_time)
    print("Duration: %s\n" % duration)

    msg_types = df['channel'].unique()
    for mtype in msg_types:
        mm = df.loc[df['channel'] == mtype]
        mt = mm['timestamp'] / 1E6
        print("%s: %d messages, %f mean dt, %f dt std_dev" % (mtype,
                                                              mm.shape[0],
                                                              mt.diff().mean(),
                                                              mt.diff().std()))


def merged_log_writer(out_file_path, sorted_df, overwrite=True):
    """
    Write out the merged log file using every row of the input dataframe
    :param out_file_path: Output path of file to write
    :param sorted_df: Pandas DataFrame of each file to be sorted.
    :param overwrite: Boolean variable if you're going to overwrite the file
        at out_file_path, default=True
    :return: None
    """
    log_map = {lf: EventLog(lf, 'r') for lf in sorted_df['logfile'].unique()}
    out_log = EventLog(out_file_path, 'w', overwrite=overwrite)
    for rowtuple in sorted_df.iterrows():
        row = rowtuple[1]
        log_map[row['logfile']].seek(row['fpos'])
        event = log_map[row['logfile']].next()

        out_log.write_event(row['timestamp'],
                            row['channel'],
                            event.data)

    out_log.close()

    for lfile in log_map.values():
        lfile.close()
