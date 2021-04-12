from typing import Dict, Any

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import csv
import logging
import seaborn as sns
import conveyance

# logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
#                     datefmt='%Y-%m-%d:%H:%M:%S',
#                     level=logging.DEBUG)
from pandas import DataFrame


def read_icm_cross_section_survey_section_array_csv(csv_path):
    """
    read the ICM csv export for cross section in the following format
ObjectTable	id	X	Y	Z	roughness_N	new_panel
FieldDescription	ID	X coordinate	Y coordinate	Bed level	Roughness Manning's n	New panel
UserUnits		m	m	m AD
hw_cross_section_survey_section_array	1093.1	546618.976	4804330.849	291.4439087	0.013
		546628.1402	4804327.693	291.1368103	0.013

    :param csv_path:
    :return: a dataframe
    """
    xs_name = None
    xs_list = {}
    rows = []
    with open(csv_path) as o:
        i = 0
        for l in csv.reader(o):
            if i == 0:
                header = l
            if i == 2:
                units = l
            if i >= 3:
                # X	Y	Z	roughness_N	new_panel
                for idx in [2, 3, 4, 5, 6]:
                    # try:
                    if True:
                        l[idx] = float(l[idx])
                    # except Exception:
                    #     l[idx] = 0
                    #


                if len(l[1]) > 0:
                    if xs_name is None:
                        # first cross section
                        xs_name = l[1]
                        logging.info('first xs: %s' % xs_name)
                    else:  # start of another xs
                        xs_list[xs_name] = pd.DataFrame(rows)
                        logging.info('xs saved: %s' % xs_name)
                        rows = []
                        xs_name = l[1]
                        logging.info('next xs: %s' % xs_name)
                rows.append(dict(zip(header, l)))
            i += 1
        # add the last one
        xs_list[xs_name] = pd.DataFrame(rows)
        logging.info('xs saved: %s' % xs_name)
    return xs_list


def icm_xs_add_offset(xs_list):
    for xs in xs_list.copy():
        df = xs_list[xs]
        for fld in df.columns:
            if fld in ['X', 'Y', 'Z', 'roughness_N', 'new_panel']:
                df[fld] = pd.to_numeric(df[fld], errors='roughness_N')
        df['length'] = df.loc[:, ['X', 'Y']].diff().apply(lambda x: np.sqrt(x['X'] ** 2 + x['Y'] ** 2), axis=1).fillna(
            0)
        df['offset'] = df['length'].cumsum()
        xs_list[xs] = df
    return xs_list


def plot_xs(df, xs_name, fig=None, axes=None):
    # TODO: add flood level alex
    sns.set()
    if fig:
        pass
    else:
        fig, axes = plt.subplots(2, sharex=True, gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.001})
    # sns.lineplot(x='offset', y='Z', data=df, ax=axes[1])
    axes[1].plot(df['offset'], df['Z'])
    axes[1].set_ylabel('Bed Elevation')
    axes[1].set_xlabel('Offset')
    zmax = max(df['Z'])
    zmin = min(df['Z'])
    for x in df.loc[df['new_panel'] == 1, 'offset'].values:
        axes[1].plot([x, x], [zmin, zmax], linestyle='--', color='grey')
    axes[0].step(df['offset'], df['roughness_N'], where='post')
    axes[0].set_ylabel('Roughness')
    fig.suptitle(xs_name)
    return fig


def conveyance_curve(df):
    xs = df['offset'].values
    ys = df['Z'].values
    ns = df['roughness_N'].values
    ps = df['new_panel'].values
    return conveyance.conveyance_curve(xs, ys, ns, ps)

def read_conveyance_from_log(log_txt):
    """

Geometry for conduit 130.1:
         h          a          b      db_dh          p      dp_dh          k      dk_dh       beta  d_beta_dh
 0.000E+00  0.000E+00  0.000E+00  3.069E+01  0.000E+00  4.026E+01  0.000E+00  0.000E+00  1.000E+00  0.000E+00
 4.454E-01  1.885E+00  5.859E+00  6.833E+00  5.978E+00  7.119E+00  5.821E+01  3.040E+02  1.000E+00  0.000E+00
 ...
Geometry for conduit 1349.1:
Section 1:
         h          a          b      db_dh          p      dp_dh          k      dk_dh       beta  d_beta_dh
 0.000E+00  5.000E-02  1.000E-01  1.861E+01  1.902E+00  1.693E+01  4.271E-01  1.192E-07  1.000E+00  0.000E+00
...
Section 2:
         h          a          b      db_dh          p      dp_dh          k      dk_dh       beta  d_beta_dh
 0.000E+00  5.000E-02  1.000E-01  1.861E+01  1.902E+00  1.693E+01  4.271E-01  1.192E-07  1.000E+00  0.000E+00
...
    """
    reaches = {}
    reading_rows = False
    with open(log_txt) as f:
        for l in f:
            if 'Geometry for conduit ' in l:
                if reaches:
                    reaches[reach] = sections
                    logging.info('done processing reach: %s' % reach)
                reach = l.split()[-1][:-1]
                logging.info('processing reach: %s' % reach)

                sections = {}
            elif 'Section ' in l:
                if sections:
                    sections[section] = rows
                    rows = []
                section = l.split()[-1][:-1]
            elif '         h          a          b      db_dh          p      dp_dh          k      dk_dh       beta  d_beta_dh' in l:
                # it is the header
                header = l.split()
                reading_rows = True
            else:
                if len(l.split()!=10):
                    reading_rows = False
                if reading_rows:
                    rows.append(l.split())
        #TODO: FINISH THIS





