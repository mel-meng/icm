import csv
import os
import pandas as pd
import datetime
import logging

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




def csv_to_icm_csv(config, input_csv, output_csv, profile, timestamp_fld, tparser=None, vfactor=1):
    '''
    convert csv file in the format:
    ------------------------------------
    timestampe    22b sf    10a sf    11 sf    8a sf    26 sf    21b sf    10b sf    27 sf    22a sf    21c sf    21e sf    21d sf    8b sf    21a sf
    3/14/2013 9:00    1.025833333    1.1725    0.464    1.211083333    0.668333333    0.611666667    0.214166667    0.796666667    0.67    0.200833333    0.158333333    0.216666667    0.256666667    0.453333333
    3/14/2013 9:15    1.0775    1.171833333    0.492916667    1.223583333    0.676666667    0.63    0.198333333    0.850833333    0.639166667    0.199166667    0.154166667    0.221666667    0.2    0.4575
    ------------------------------------
    
    to infoWorks format
    ---------------------
    UserSettings    U_LEVEL    U_CONDHEIGHT    U_VALUES    U_DATETIME                                        
    UserSettingsValues    ft AD    in    ft    mm-dd-yyyy hh:mm                                        
    G_START    G_TS    G_NPROFILES                                                
    3/1/2013 0:00    15m    14                                                
    L_LINKID    L_INVERTLEVEL    L_CONDHEIGHT    L_GROUNDLEVEL    L_PTITLE                                        
    SMH13039    753.39    30    762.13    22b sf                                        
    SMH7975    729.073363    36    742.773363    10a sf                                        
    SMH7526    701.35    36    712.76    11 sf                                        
    SMH50541    684.78    36    696.38    8a sf                                        
    SMH15110    802.27    30    807.84    26 sf                                        
    SMH13023    757.152655    18    771.43    21b sf                                        
    SMH600355    772.54    16    780.12    10b sf                                        
    SMH16167    811.46    27    820.82    27 sf                                        
    SMH7980B    730.87    30    742.986965    22a sf                                        
    SMH12887    730.87    30    790.566117    21c sf                                        
    SMH13016    771.77    12    779.95    21e sf                                        
    SMH12706A    791.53    12    796.78    21d sf                                        
    SMH7690    717.07    24    725.47    8b sf                                        
    SMH8966    742.351077    18    749.741077    21a sf                                        
    P_DATETIME    1    2    3    4    5    6    7    8    9    10    11    12    13    14
    3/17/2013 16:30    1.2025            1.519416667    0.674166667    0.6575    0.238333333    0.9225    0.695    0.250833333    0.321666667    0.208333333    0.251666667    0.464166667
    3/17/2013 16:45    1.238333333            1.51225    0.674166667    0.655    0.22    0.78    0.695    0.2475    0.325833333    0.211666667    0.251666667    0.469166667
    3/17/2013 17:00    1.205            1.504583333    0.666666667    0.634166667    0.235833333    0.7425    0.689166667    0.245833333    0.330833333    0.213333333    0.2425    0.490833333
    
-------------------------
    '''
    logging.info('writing infoWorks import to: %s' % output_csv)
    with open(output_csv, 'w') as o:
        writer = csv.writer(o, lineterminator='\n')
        # writing the user, and subevent sections
        if profile == 'rainfall':
            sections = ['header', 'user', 'subevent']
        else:
            sections = ['user', 'subevent']
        for section in sections:
            flds = SCHEMA[profile][section]['header']
            writer.writerow(flds)
            row = [config.get(fld) for fld in flds]
            writer.writerow(row)
        # writing the profile section
        flds = SCHEMA[profile]['profile']['header']
        writer.writerow(flds)
        profile_file_list = []
        # adding each profile
        for p in config['profiles']:
            row = [p.get(fld) for fld in flds]
            writer.writerow(row)
            profile_file_list.append(p['L_PTITLE'])  # get the order of the files
        # add the data
        flds = ['P_DATETIME'] + list(range(1, len(config['profiles']) + 1))
        writer.writerow(flds)
        df = pd.read_csv(input_csv)
        df['timestamp'] = pd.to_datetime(df[timestamp_fld]).apply(lambda x: x.strftime('%m-%d-%Y %H:%M')) #mm-dd-yyyy hh:mm
        for idx, r in df.iterrows():

            row = [r['timestamp']]

            for p in profile_file_list:
                v = r.get(p)
                if pd.isnull(v):
                    v = None
                else:
                    v = float(v) * vfactor


                row.append(v)
            writer.writerow(row)


def get_config(input_csv):
    '''
    read the input_csv into a dictionary
    input_csv format, the #profile is important, that's the flag for parsing profiles
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
    #profile and below are added as a list of profiles,
    
    data format:
    {'U_LEVEL': 'ft AD', 'UserSettings': 'UserSettingsValues', 
    'profiles': [{'L_LINKID': 'SMH13039', 'L_GROUNDLEVEL': '762.13', 'L_CONDHEIGHT': '30', 'file': '22b sf.csv', 'L_PTITLE': '22b sf', 'L_INVERTLEVEL': '753.39'},
                 {'L_LINKID': 'SMH7975', 'L_GROUNDLEVEL': '742.773363', 'L_CONDHEIGHT': '36', 'file': '10a sf.csv', 'L_PTITLE': '10a sf', 'L_INVERTLEVEL': '729.073363'}]
    }
    
    '''
    # get the configuration of the data

    with open(input_csv) as i:
        reader = csv.reader(i)
        data = {}
        profiles = []
        is_profile = False
        ltype = 'header'
        for l in reader:
            l = [x.strip() for x in l]
            if l[0] and l[0][0] == '#':
                # comment
                if l[0] == '#profile':
                    ltype = 'header'
                    is_profile = True
            else:
                if ltype == 'header':
                    flds = l
                    ltype = 'values'
                else:

                    if is_profile:
                        ltype = 'values'
                        profiles.append(dict(zip(flds, l)))
                    else:
                        for fld, v in zip(flds, l):
                            data[fld] = v
                        ltype = 'header'

        data['profiles'] = profiles
        data['G_NPROFILES'] = len(profiles)
        return data


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


def main():
    '''do all the conversion in one shot'''

    folder_path = 'data/src_csv'  # the individual meter files location
    out_folder = 'data/out_csv'  # the output csv file location
    time_start = datetime.datetime(2013, 3, 1)
    time_end = datetime.datetime(2013, 5, 9)
    timestep = datetime.timedelta(seconds=15 * 60)
    header = 'Level (feet),Velocity (ft/s),Flow Rate (MGD)'
    ts_format = '%m-%d-%Y %H:%M'
    # combine the csv files

    combine_csv_folder(folder_path, out_folder, header, ts_format, time_start, time_end, timestep)
    # do the conversion
    profiles = 'depth velocity flow'.split()
    inputs = ['combined_%s.csv' % x.replace(' ', '_').replace('(', '_').replace(')', '_').replace('/', '_') for x in
              header.split(',')]
    outputs = ['Observed %s Event.csv' % p.title() for p in profiles]

    for profile, input_csv, output_csv in zip(profiles, inputs, outputs):
        logging.info('converting %s' % profile)
        config_csv = 'data/config/%s_config.csv' % profile
        # get the config
        data = get_config(config_csv)
        input_csv = 'data/out_csv/%s' % input_csv
        output_csv = 'data/out_csv/%s' % output_csv
        csv_to_icm_csv(data, input_csv, output_csv, profile, tparser=None)


#     profile = 'flow'
#     #set up the workspace
#     config_csv = 'data/config/flow_config.csv'
#     #get the config
#     data =  get_config(config_csv)
#     input_csv = 'data/out_csv/Flow_Rate__MGD_.csv'
#     output_csv = 'data/out_csv/Observed %s Event.csv' % profile.title()
#     data2infoworks_csv(data, input_csv, output_csv,profile, tparser=None)
#     
#     profile = 'velocity'
#     #get the config
#     config_csv = 'data/config/velocity_config.csv'
#     data =  get_config(config_csv)
#     output_csv = 'data/out_csv/Observed %s Event.csv' % profile.title()
#     
#     input_csv = 'data/out_csv/Velocity__ft_s_.csv'
#     
#     data2infoworks_csv(data, input_csv, output_csv,profile, tparser=None)

def rainfall():
    profile = 'rainfall'
    folder_path = 'data/rain_src_csv'  # the individual meter files location
    out_folder = 'data/out_csv'  # the output csv file location
    time_start = datetime.datetime(2013, 3, 1)
    time_end = datetime.datetime(2013, 5, 9)
    timestep = datetime.timedelta(seconds=15 * 60)
    header = 'Depth'
    ts_format = '%m-%d-%Y %H:%M'
    # in the order of y m d h m
    time_fields = 'Year Month    Day    Hour    Min'
    # combine the csv files

    combine_csv_folder(folder_path, out_folder, header, ts_format, time_start, time_end, timestep, time_fields)
    # get the config
    config_csv = 'data/config/rainfall_config.csv'
    data = get_config(config_csv)
    output_csv = 'data/out_csv/Observed %s Event.csv' % profile.title()

    input_csv = 'data/out_csv/combined_Depth.csv'
    vfactor = 60 / 15  # convert to in/hr from inches
    csv_to_icm_csv(data, input_csv, output_csv, profile, None, vfactor)


if __name__ == '__main__':
    main()
    rainfall()
