import csv
import os
import pandas as pd
import datetime
import logging
from flow_survey_templates import TMP

logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

'''
Extract data from flow monitoring data stored in the format of individual meters into three csv files that can be imported into infoWorks.
Procedures:
1. store the flow data for each meter into a csv file using the format:
    Month    Day    Year    Hour    Minute    Level (feet)    Velocity (ft/s)    Flow Rate (MGD)
    3    7    2013    17    30    1.50025    2.534    5.7895776
    3    7    2013    17    45    1.4465    2.561    5.5844208
2. place all the csv files in a folder
3. create a config.csv file to specify how the data should be exported in the following format:
    note that #profile need to be populated
    -----------------------
    UserSettings    U_LEVEL    U_CONDHEIGHT    U_VALUES    U_DATETIME    
    UserSettingsValues    ft AD    in    ft    mm-dd-yyyy hh:mm    
    G_START    G_TS    G_NPROFILES            
    3/1/2013 0:00    15m    2            
    #profile                    
    L_LINKID    L_INVERTLEVEL    L_CONDHEIGHT    L_GROUNDLEVEL    L_PTITLE    file
    SMH13039    753.39    30    762.13    22b sf    22b sf.csv
    SMH7975    729.073363    36    742.773363    10a sf    10a sf.csv
    SMH7526    701.35    36    712.76    11 sf    11 sf.csv
    SMH50541    684.78    36    696.38    8a sf    8a sf.csv
    SMH15110    802.27    30    807.84    26 sf    26 sf.csv
    SMH13023    757.152655    18    771.43    21b sf    21b sf.csv
    SMH600355    772.54    16    780.12    10b sf    10b sf.csv
    SMH16167    811.46    27    820.82    27 sf    27 sf.csv
    SMH7980B    730.87    30    742.986965    22a sf    22a sf.csv
    SMH12887    730.87    30    790.566117    21c sf    21c sf.csv
    SMH13016    771.77    12    779.95    21e sf    21e sf.csv
    SMH12706A    791.53    12    796.78    21d sf    21d sf.csv
    SMH7690    717.07    24    725.47    8b sf    8b sf.csv
    SMH8966    742.351077    18    749.741077    21a sf    21a sf.csv
    ------------
4. run the convert() function and you are set
'''

# TODO: make the config.csv to tell which file to extract the data from
"""
UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME
UserSettingsValues,ft AD,in,ft,mm-dd-yyyy hh:mm
G_START,G_TS,G_NPROFILES
03/01/2013 00:00,900,2
L_LINKID,L_INVERTLEVEL,L_CONDHEIGHT,L_GROUNDLEVEL,L_PTITLE
        8a,0,    0,0,8A                            
        8b,0,    0,0,8B                            
P_DATETIME,1,2
03/01/2013 00:00,1.500249,0.000000"""

SCHEMA = {}

SCHEMA['depth'] = {'header': {'header': 'FILECONT, TITLE'.split(','), 'values': None},
                   'user': {'header': 'UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME'.split(','),
                            'values': ['UserSettingsValues', 'ft AD', 'in', 'ft', 'mm-dd-yyyy hh:mm']},
                   'subevent': {'header': 'G_START,G_TS,G_NPROFILES'.split(','),
                                'values': None},
                   'profile': {'header': 'L_LINKID,L_INVERTLEVEL,L_CONDHEIGHT,L_GROUNDLEVEL,L_PTITLE'.split(','),
                               'values': None}}

SCHEMA['flow'] = {'header': {'header': 'FILECONT, TITLE'.split(','), 'values': None},
                  'user': {'header': 'UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME'.split(','),
                           'values': ['UserSettingsValues', 'ft AD', 'in', 'ft', 'mm-dd-yyyy hh:mm']},
                  'subevent': {'header': 'G_START,G_TS,G_NPROFILES'.split(','),
                               'values': None},
                  'profile': {'header': 'L_LINKID,L_CONDCAPACITY,L_PTITLE'.split(','),
                              'values': None}
                  }

SCHEMA['velocity'] = {
    'user': {'header': 'UserSettings,U_LEVEL,U_CONDHEIGHT,U_VALUES,U_DATETIME'.split(','),
             'values': ['UserSettingsValues', 'ft AD', 'in', 'ft', 'mm-dd-yyyy hh:mm']},
    'subevent': {'header': 'G_START,G_TS,G_NPROFILES'.split(','),
                 'values': None},
    'profile': {'header': 'L_LINKID,L_PTITLE'.split(','),
                'values': None}
}
SCHEMA['rainfall'] = {'header': {'header': 'FILECONT, TITLE'.split(','), 'values': None},
                      'user': {'header': 'UserSettings,U_ARD,U_EVAP,U_VALUES,U_DATETIME'.split(','),
                               'values': ['UserSettingsValues', 'ft AD', 'in', 'ft', 'mm-dd-yyyy hh:mm']},
                      'subevent': {'header': 'G_START,G_TS,G_NPROFILES,G_UCWI,G_ARD,G_EVAP,G_WI'.split(','),
                                   'values': None},
                      'profile': {'header': 'L_UCWI,L_ARD,L_ARF,L_EVAP,L_WI,L_PTITLE,L_ADD'.split(','),
                                  'values': None}
                      }


def convert_infoswmm_pipe_results(infoswmm_csv, output_folder):
    df = pd.read_csv(infoswmm_csv)
    df['timestamp'] = pd.to_datetime(df['Page'])
    pipes = df['ID'].unique()
    params = ['Flow', 'Depth', 'Velocity']
    results = {}
    for pipe in pipes:
        ts = df.loc[df['ID']==pipe, ]
        ts.index = ts['timestamp']

        for p in params:
            ts[pipe] = ts[p]
            if p in results:
                results[p] = results[p].join(ts.loc[:, [pipe]])
            else:
                results[p] = ts.loc[:, ['timestamp', pipe]]
    for p in results:
        results[p].to_csv(os.path.join(output_folder, '%s.csv' % p), index=False)


def csv_to_icm(input_csv, out_csv, csv_type, date_fld, ts_seconds, start_dt=None, end_dt=None):
    df = pd.read_csv(input_csv)
    df.index = pd.to_datetime(df[date_fld])
    if start_dt is None:
        start_dt = min(df.index)
    else:
        start_dt = pd.to_datetime(start_dt)
    if end_dt is None:
        end_dt = max(df.index)
    else:
        end_dt = pd.to_datetime(end_dt)
    # resample
    # TODO: up/down sampling
    df = df.loc[start_dt:end_dt, ].resample('%sS' % ts_seconds).pad()
    df = df.fillna(0)
    meter_list = [x for x in df.columns if x!=date_fld]
    data = {'start_dt': start_dt.strftime('%m-%d-%Y %H:%M'), 'time_step': ts_seconds, 'count': len(meter_list)}
    profile = []
    for i, fld in enumerate(meter_list):
        if csv_type == 'rainfall':
            profile.append('%s,,' % fld)
        else:
            profile.append(','.join([fld, '0', fld]))
    data['profile'] = '\n'.join(profile)
    if csv_type == 'rainfall':
        data['profile1'] = '\n'.join(['      0.00,0,0,     0.000,0,0,     0.000,    0,0,0']*len(meter_list))
        data['profile2'] = '\n'.join(['     1.000,     1.000']*len(meter_list))

    data['ts_header'] = ','.join([str(x) for x in range(1, len(meter_list) + 1)])
    ts_data = []
    df['timestamp'] = [x.strftime('%m-%d-%Y %H:%M') for x in df.index]
    for idx, r in df.iterrows():
        row = [r['timestamp']]
        for fld in meter_list:
            row.append(str(r[fld]))
        ts_data.append(','.join(row))
    data['ts_data'] = '\n'.join(ts_data)

    with open(out_csv, 'w') as o:
        o.write(TMP[csv_type].format(**data))




def read_csv_folder(folder_path, param_fields, fn, timestamp_fld):
    """
    each csv is a file for a single flow meter, and the file name is the flow meter
    combine them into 3 dataframes for depth, flow, vecloity, and the columns are the flow meter names
    """
    results = {}

    for f in os.listdir(folder_path):
        logging.info('processing %s' % f)
        a, b = os.path.splitext(f)
        if b.lower() == '.csv':
            logging.info('  --reading: %s' % f)
            fm = a
            df = pd.read_csv(os.path.join(folder_path, f))
            df = add_timestamp_to_df(df, fn, timestamp_fld)
            df.index = df[timestamp_fld]
            for p in param_fields:
                df[fm] = df[p]
                if p in results:
                    results[p] = results[p].join(df.loc[:, [fm]])
                else:
                    results[p] = df.loc[:, [timestamp_fld,fm]]

    return results


def add_timestamp_to_df(df, fn, timestamp_fld):
    df[timestamp_fld] = df.apply(lambda x: fn(x), axis=1)
    return df


def combine_csv_folder(folder_path, out_folder, header='Level (feet),Velocity (ft/s),Flow Rate (MGD)',
                       ts_format='%m-%d-%Y %H:%M', time_start=None, time_end=None, timestep=None,
                       time_fields='Year Month Day Hour Minute'):
    '''
    use time_start, time_end, timestep to use a predefined time step and range
    find all the *.csv files in the folder_path and combine the data in one table for depth, velocity and flow..
    each *.csv file are in the following format, user the header if the labels are different:
    Month    Day    Year    Hour    Minute    Level (feet)    Velocity (ft/s)    Flow Rate (MGD)
    3    7    2013    17    30    1.50025    2.534    5.7895776
    3    7    2013    17    45    1.4465    2.561    5.5844208
    3    7    2013    18    0    1.447    2.798    6.1039152
    3    7    2013    18    15    1.443416667    2.58    5.6104272
    3    7    2013    18    30    1.448666667    2.76    6.029928
    3    7    2013    18    45    1.448666667    2.845    6.2156448
    3    7    2013    19    0    1.452583333    2.687    5.8908384
    
    the 3 output *.csv are in the  out_folder in the following format, the header is the file name for each meter:
    timestampe    22b sf    10a sf    11 sf    8a sf    26 sf    21b sf    10b sf    27 sf    22a sf    21c sf    21e sf    21d sf    8b sf    21a sf
    3/14/2013 9:00    1.025833333    1.1725    0.464    1.211083333    0.668333333    0.611666667    0.214166667    0.796666667    0.67    0.200833333    0.158333333    0.216666667    0.256666667    0.453333333
    3/14/2013 9:15    1.0775    1.171833333    0.492916667    1.223583333    0.676666667    0.63    0.198333333    0.850833333    0.639166667    0.199166667    0.154166667    0.221666667    0.2    0.4575
    3/14/2013 9:30    1.1025    1.192    0.485916667    1.22    0.714166667    0.631666667    0.2    0.839166667    0.666666667    0.205833333    0.153333333    0.221666667    0.205833333    0.459166667
    3/14/2013 9:45    1.13    1.244833333    0.51225    1.223333333    0.719166667    0.631666667    0.2    0.708333333    0.6975    0.200833333    0.179166667    0.204166667    0.204166667    0.459166667
    3/14/2013 10:00    1.08    1.27475    0.501833333    1.2435    0.721666667    0.633333333    0.2    0.701666667    0.656666667    0.206083333    0.18    0.233333333    0.203333333    0.4625
    3/14/2013 10:15    1.15    1.282666667    0.49825    1.229416667    0.721666667    0.630833333    0.2    0.701666667    0.6625    0.211416667    0.1775    0.240833333    0.228333333    0.4625
    3/14/2013 10:30    1.155    1.251    0.495    1.236333333    0.721666667    0.610833333    0.214166667    0.6975    0.665833333    0.216666667    0.171666667    0.235    0.216666667    0.463333333
    3/14/2013 10:45    1.15    1.27775    0.475416667    1.259583333    0.719166667    0.611666667    0.2125    0.928333333    0.700833333    0.209166667    0.17    0.236666667    0.226666667    0.4625


    '''

    ts_list = set()  # Get a unique list of time stamps which will be used as the first column

    def read_csv(csv_path, time_fields):
        '''read each csv file into a dictionary
        { datetime.datetime(xx, xx, xx), {'Year': xx, 'Month': xx, ...}
        '''
        data = {}
        with open(csv_path) as f:
            reader = csv.reader(f)
            i = 0
            for l in reader:
                if i == 0:
                    h = l
                    continue
                i += 1
                r = dict(zip(h, l))
                t = [int(r.get(fld)) for fld in time_fields.split()]
                ts = datetime.datetime(*t)
                data[ts] = r
        return data

    logging.info('Combining csv files in: %s' % folder_path)
    data = {}
    for f in os.listdir(folder_path):
        if '.csv' in f:
            logging.info('  --reading: %s' % f)
            data[f] = read_csv(os.path.join(folder_path, f), time_fields)
            # get the unique list of time stamps
            for t in data[f]:
                ts_list.add(t)

    files = list(data.keys())  # a list of files
    ts_list = sorted(list(ts_list))  # sort the timestamp
    # using predefined timestep
    if time_start and time_end and timestep:
        ts_list = []
        t = time_start
        while t < time_end:
            ts_list.append(t)
            t = t + timestep
        if time_end in ts_list:
            pass
        else:
            ts_list.append(time_end)

    for param in header.split(','):
        f_out = param.replace(' ', '_').replace('(', '_').replace(')', '_').replace('/', '_')

        with open(os.path.join(out_folder, 'combined_%s.csv' % f_out), 'w') as o:
            writer = csv.writer(o, lineterminator='\n')
            writer.writerow(
                ['timestampe'] + files)  # [f.replace('.csv', '') for f in files]), to replace it with file name

            for ts in ts_list:
                row = [ts.strftime(ts_format)]  # here is the timestamp format
                for f in files:
                    d = data[f].get(ts)
                    if d:
                        v = d.get(param)

                    else:
                        v = None
                    row.append(v)
                writer.writerow(row)
        logging.info('  --File written: %s' % os.path.join(out_folder, 'combined_%s.csv' % f_out))

