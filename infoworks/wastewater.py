import csv
import logging
import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns


##CONVERT iw export waste water patterns into individual pattern files.
##
logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.INFO)
logging.info('starting logging')


def read_iw_wastewater(iw_csv):
    with open(iw_csv) as f:
        reader = csv.reader(f)

        profile = None
        i = 0
        p_line = ''  # previous line
        c_line = ''  # current line
        for l in reader:

            i += 1
            s = l[0]
            if s == 'PROFILE_NUMBER':
                c_line = 'p_number header'
                profile = {}
            elif s == 'CALIBRATION_WEEKDAY':
                c_line = 'weekday title'
            elif s == 'CALIBRATION_WEEKEND':
                c_line = 'weekend title'
            elif s == 'CALIBRATION_MONTHLY':
                c_line = 'monthly title'

            if p_line == 'p_number header':
                c_line = 'row'
                profile['summary'] = dict(zip(header, l))
                rows = []
            elif p_line == 'weekday title':
                c_line = 'weekday header'
                header = l
                rows = []
            elif p_line == 'weekend title':
                c_line = 'weekend header'
                header = l
                rows = []
            elif p_line == 'weekend header':
                c_line = 'row'

            elif p_line == 'weekday header':
                c_line = 'row'

            if 'header' in c_line:
                header = l
                row = []
            elif c_line == 'weekend title':
                profile['weekday'] = {'header': header, 'rows': rows}
                header = None
                rows = None

            elif c_line == 'row':
                rows.append(l)

            elif c_line == 'monthly title':
                profile['weekend'] = {'header': header, 'rows': rows}
                header = None
                rows = None
                yield profile
                profile = None
                p_line = ''
                c_line = ''

            p_line = c_line


def convert(iw_csv, out_folder):
    """
    convert the IW exported CSV into a folder with a summary of all profiles, and individual patterns
    :param iw_csv:
    :param out_folder:
    :return:
    """
    summary = []
    for p in read_iw_wastewater(iw_csv):
        summary.append(p['summary'])
        for pattern in ['weekday', 'weekend']:
            with open(os.path.join(out_folder, '%s_%s.csv' % (p['summary']['PROFILE_NUMBER'], pattern)), 'w') as o:
                writer = csv.writer(o, lineterminator='\n')
                writer.writerow(p[pattern]['header'])
                for row in p[pattern]['rows']:
                    writer.writerow(row)
    with open(os.path.join(out_folder, 'summary.csv'), 'w') as o:
        writer = csv.writer(o, lineterminator='\n')
        h = summary[0].keys()
        writer.writerow(h)
        for r in summary:
            row = [r[x] for x in h]
            writer.writerow(row)


def parse_timestamp(ts, fmt='%m/%d/%Y %H:%M:%S'):
    '''
    t: datetime
    dhr: hour of the day 0-24
    wkd: week day 1-7 (Monday=1)
    wkhr: hour of the week 0-7*24
    '''
    try:
        t = datetime.datetime.strptime(ts, fmt)
    except:
        t = ts
    d_hr = t.hour + t.minute / 60.0
    wkd = t.isoweekday()
    wk_hr = (wkd - 1) * 24 + d_hr

    return {'t': t, 'dhr': d_hr, 'whr': wk_hr, 'wkd': wkd}

def add_timeinfo(df):
    for k in ['dhr', 'whr', 'wkd']:
        df[k] = [parse_timestamp(x)[k] for x in df.index]
    return df


def wastewater_summary(pattern_folder, population_csv, summary_csv):
    """
    population csv: summary from the subcatchment table
    wastewater_profile	population_sum	population_count
    1	2100	15
    2	755.53	15

    :param pattern_folder:
    :param population_csv:
    :return:
    """
    f = os.path.join(pattern_folder, 'summary.csv')
    df_pattern = pd.read_csv(f)
    pop_df = pd.read_csv(population_csv)
    for fld in ['population', 'weekday_mean', 'weekend_mean', 'weekday_flow_gpd', 'weekend_flow_gpd' ]:
        df_pattern[fld] = 0
    for idx, r in df_pattern.iterrows():
        fm = r['PROFILE_DESCRIPTION']
        pn = r['PROFILE_NUMBER']
        flow = r['FLOW']
        f_wk = os.path.join(pattern_folder, '%s_weekday.csv' % pn)
        f_wkn = os.path.join(pattern_folder, '%s_weekend.csv' % pn)
        wk_flow = pd.read_csv(f_wk, parse_dates=True, index_col=0)['FLOW'].mean()
        wkn_flow = pd.read_csv(f_wkn, parse_dates=True, index_col=0)['FLOW'].mean()
        population = pop_df[pop_df.wastewater_profile==pn]
        if population.empty:
            #not found in the subcatchment summary
            population = -1
        else:
            population = population['population_sum'].values
        df_pattern.loc[idx, 'population'] = population
        df_pattern.loc[idx, 'weekday_flow_gpd'] = population*wk_flow*flow
        df_pattern.loc[idx, 'weekend_flow_gpd'] = population*wkn_flow * flow
        df_pattern.loc[idx, 'weekday_mean'] = wk_flow
        df_pattern.loc[idx, 'weekend_mean'] = wkn_flow


    df_pattern.to_csv(summary_csv, index=False)



def plot_pattern(pattern_folder, figure_folder):
    f = os.path.join(pattern_folder, 'summary.csv')
    df_pattern = pd.read_csv(f)
    for idx, r in df_pattern.iterrows():
        fm = r['PROFILE_DESCRIPTION']
        pn = r['PROFILE_NUMBER']
        flow = r['FLOW']

        f_wk = os.path.join(pattern_folder, '%s_weekday.csv' % pn)
        f_wkn = os.path.join(pattern_folder, '%s_weekend.csv' % pn)
        df_wk = pd.read_csv(f_wk, parse_dates=True, index_col=0)
        df_wk = add_timeinfo(df_wk)
        df_wkn = pd.read_csv(f_wkn, parse_dates=True, index_col=0)
        df_wkn = add_timeinfo(df_wkn)
        fig = plt.figure(figsize=(8,4))
        ax = fig.add_subplot(111)
        ax.plot(df_wk['dhr'], df_wk['FLOW'], label='Week(%.2f)' % df_wk['FLOW'].mean())
        ax.plot(df_wkn['dhr'], df_wkn['FLOW'], label='Weekend(%.2f)' % df_wkn['FLOW'].mean())
        ax.set_xlim(0,23)
        ax.set_xlabel('Time of Day (hr)')
        ax.set_ylabel('Daily Pattern Factor')
        ax.legend(loc='best')

        ax.set_title('%s Per Capita Flow= %s gal/day' % (fm, flow))
        f = os.path.join(figure_folder, '%s.png' % fm)
        fig.savefig(f, bbox_inches='tight')
        logging.info('figure saved at: %s' % f)
        plt.close()
