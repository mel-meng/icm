import unittest
import iw_plot as ip
import logging
import os
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)
logging.info('starting logging')
import matplotlib.gridspec as gridspec


class TestRdii(unittest.TestCase):

    def setUp(self):

        workspace = './workspace'
        self.workspace = workspace
        



    def tearDown(self):
        pass

    def testConvert(self):
        csv_path = os.path.join(self.workspace, 'dwf/0404.csv')
        out_path = os.path.join(self.workspace, 'dwf/0404_out.csv')
        
        results = ip.convert_iw_plot_csv(csv_path, out_path,freq='15Min', csv_h='dt_obs,ts_obs,obs_f,obs_d,dt_sim,ts_sim,sim_f,sim_d,dt_rain,ts_rain,rain'.split(','), out_h = 'rain sim_f obs_f sim_d obs_d t wkd d_hr2 wk_hr d_hr fm'.split())
      
        obs = results['CR_02']['obs']
        sim = results['CR_02']['sim']
        rdii = obs - sim
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(obs.index, obs['f'], label='obs')
        ax.plot(sim.index, sim['f'], label='sim')
        ax.plot(rdii.index, rdii['f'], label = 'rdii')
        ax.legend(loc='lower right', fontsize='small', numpoints=1)
        plt.show()
        