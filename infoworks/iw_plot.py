import csv
import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import os
from matplotlib.dates import DayLocator, HourLocator, DateFormatter, drange, num2date
import pylab
#from plots import *

from matplotlib.ticker import FuncFormatter
logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)





def convert_iw_plot_csv(csv_path, out_path, freq='15Min', csv_h='dt_obs,ts_obs,obs_f,obs_d,dt_sim,ts_sim,sim_f,sim_d,dt_rain,ts_rain,rain'.split(','), out_h = 'rain sim_f obs_f sim_d obs_d t wkd d_hr2 wk_hr d_hr fm'.split()):
    '''
    infoworks export TS as a combined csv file for each meter.
    Here is the format with uniform time step
Page title is     "Flow Survey Location (Obs.) FM 22b     Model Location (Pred.) D/S  SMH13038.1     Rainfall Profile: 5"
Date    Time    Rain (Rainfall (in/hr))    >Catchment Group>Run Group>WWF C2>BV 2013 Rainfall (Flow (MGD))    Obs. (Flow (MGD))    >Catchment Group>Run Group>WWF C2>BV 2013 Rainfall (Depth (ft))    Obs. (Depth (ft))
3/1/2013    0:00:00    0    2.698188    0    0.894576    0
...
...
Page title is     "Flow Survey Location (Obs.) FM 22b     Model Location (Pred.) D/S  SMH13038.1     Rainfall Profile: 5"
Date    Time    Rain (Rainfall (in/hr))    >Catchment Group>Run Group>WWF C2>BV 2013 Rainfall (Flow (MGD))    Obs. (Flow (MGD))    >Catchment Group>Run Group>WWF C2>BV 2013 Rainfall (Depth (ft))    Obs. (Depth (ft))
3/1/2013    0:00:00    0    2.698188    0    0.894576    0
....

it can be non uniform time step too
Page title is     "Flow Survey Location (Obs.) CR_01     Model Location (Pred.) D/S  277515.1     Rainfall Profile: 3"                            
Date    Time    >2013 Model Update>DW Calibration>DWF 4/4>DWF (Flow (MGD))    >2013 Model Update>DW Calibration>DWF 4/4>DWF (Depth (ft))    Date    Time    Obs. (Flow (MGD))    Obs. (Depth (ft))    Date    Time    Rain (Rainfall (in/hr))
4/4/2013    0:00:00    7.129483    1.419176    4/4/2013    0:00:00    10.312993    1.665833    4/4/2013    0:00:00    0
4/4/2013    0:02:30    7.129486    1.419176    4/4/2013    0:05:00    9.508999    1.665833    4/4/2013    0:15:00    0


    the program convert it into:
rain    sim_f    obs_f    sim_d    obs_d    t    wkd    d_hr2    wk_hr    d_hr    fm
0    2.698188    0    0.894576    0    2013-03-01T00:00:00    5    0    96    0    FM 22b
0    2.697248    0    0.894457    0    2013-03-01T00:15:00    5    0.25    96.25    0.25    FM 22b
0    2.695889    0    0.894282    0    2013-03-01T00:30:00    5    0.5    96.5    0.5    FM 22b


    '''
    logging.info('Writing to: %s' % out_path)
    with open(out_path, 'w') as o:
        logging.info('reading infoworks csv: %s ' % csv_path)
        writer = csv.writer(o, lineterminator='\n')
        
        writer.writerow(out_h)
        i = 0
        
        results = {}
        with open(csv_path) as f:


            reader = csv.reader(f)

            for l in reader:
                i+=1

                if l[0]=='Page title is':
                    fm = l[1].split('(Obs.)')[-1].strip()
                    
                    
                    link = l[2]
                    rg = l[3]
                    
                    logging.info('line-%s: reading fm: %s, %s, %s' % (i, fm, link, rg))
                    results.setdefault(fm, {})

                elif l[0]=='Date':
                    h = 'dt ts rain sim_f obs_f sim_d obs_d'.split()
                    h = csv_h
                else:
                    r = dict(zip(h, l))
#                     logging.debug('process: %s' % r)

                    for k in ['obs', 'sim', 'rain']:
                        row = {}
                        row['fm'] = fm
                        row['rg'] = rg
                        row['link'] = link
                        row['line'] = i
                        results[fm].setdefault(k, [])
                        if not r.get('dt_%s' % k):
#                             logging.info('%s not found in row, skip' % k)
                            break
                        ts = '%s %s' % (r['dt_%s' % k], r['ts_%s' % k ])
                        t = datetime.datetime.strptime(ts, '%m/%d/%Y %H:%M:%S')
                        row['ts'] = t

                     

                        d_hr = t.hour + t.minute/60.0
                        wkd = t.isoweekday()
                        wk_hr = (wkd - 1)*24 + d_hr
                        row['wkd'] = wkd
                        row['wk_hr'] = wk_hr
                        
                        
                        if k!='rain':
                            
                            for param in ['d', 'v', 'f']:
                                v = r.get('%s_%s' % (k, param))
                                try:
                                    v = float(v)
                                    row[param] = v
                                except TypeError as e:
                                    pass
#                                     logging.error(e)
                                except ValueError:
#                                     logging.error(e)
                                    pass
                                
                           
                        else:
                            v = r.get('rain')
                            try:
                                v = float(v)
                                row['rain'] = v
                            except TypeError as e:
                                    logging.error(e)
                            except ValueError:
                                logging.error(e)
                            
                        results[fm][k].append(row)
    
    data = {}
    for f in results:
        data.setdefault(f, {})
        for k in ['obs', 'sim', 'rain']:
            fm = results[f][k]
            
            data[f].setdefault(k, {})
            idx = [x['ts'] for x in fm]
           
            
            df = pd.DataFrame(fm)
            
            df = df.set_index('ts')
            
            data[f][k] = df.resample(freq, how='mean')
            
    return data

