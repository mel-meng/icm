import unittest
import logging
import os
from flow_survey import *

logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)


class TestIntegrity(unittest.TestCase):
    def setUp(self):
        pass

    def test_combine_csv_folder(self):
        folder_path = 'data/src_csv'
        out_folder = 'data/out_csv'
        combine_csv_folder(folder_path, out_folder)
        logging.info('check %s' % out_folder)

    def test_add_timestamp_to_df(self):
        df = pd.read_csv('data/src_csv/8a sf.csv')

        def fn(x):
            return datetime.datetime(int(x['Year']), int(x['Month']), int(x['Day']), int(x['Hour']), int(x['Minute']))

        timestamp_fld = 'timestampe'
        df = add_timestamp_to_df(df, fn, timestamp_fld)
        print(df['timestampe'].describe())
        assert(len(df['timestampe'].values)==5129)
        assert (pd.to_datetime(max(df['timestampe'].values)) == datetime.datetime(2013, 5, 7, 10, 30))

    def test_read_csv_folder(self):
        def fn(x):
            return datetime.datetime(int(x['Year']), int(x['Month']), int(x['Day']), int(x['Hour']), int(x['Minute']))
        folder_path = 'data/src_csv'
        out_folder = 'data/out_csv'
        param_fields = ['Level (feet)', 'Velocity (ft/s)', 'Flow Rate (MGD)']
        timestamp_fld = 'timestamp'
        results = read_csv_folder(folder_path, param_fields, fn, timestamp_fld)

        for k in results:
            results[k].to_csv(os.path.join(out_folder, '%s.csv' % k.split()[0]),index=False)
        assert False

    def test_csv_to_icm_csv(self):
        config = get_config('data/config/flow_config.csv')
        input_csv = 'data/out_csv/Flow.csv'
        output_csv = 'data/out_csv/icm_flow.csv'
        profile = 'flow'
        csv_to_icm_csv(config, input_csv, output_csv, profile, timestamp_fld='timestamp',tparser=None, vfactor=1)
        assert False


    def test_get_config(self):
        config_csv = 'data/config/depth_config.csv'
        data = get_config(config_csv)
        logging.info(data)
        check_data = {'UserSettings': 'UserSettingsValues', 'U_LEVEL': 'ft AD', 'U_CONDHEIGHT': 'in', 'U_VALUES': 'ft', 'U_DATETIME': 'mm-dd-yyyy hh:mm', '': '', 'G_START': '3/1/2013 0:00', 'G_TS': '900', 'G_NPROFILES': 14, 'profiles': [{'L_LINKID': 'FM 22b', 'L_INVERTLEVEL': '753.39', 'L_CONDHEIGHT': '30', 'L_GROUNDLEVEL': '762.13', 'L_PTITLE': '22b sf', 'file': '22b sf.csv'}, {'L_LINKID': 'FM 10a', 'L_INVERTLEVEL': '729.073363', 'L_CONDHEIGHT': '36', 'L_GROUNDLEVEL': '742.773363', 'L_PTITLE': '10a sf', 'file': '10a sf.csv'}, {'L_LINKID': 'FM 11', 'L_INVERTLEVEL': '701.35', 'L_CONDHEIGHT': '36', 'L_GROUNDLEVEL': '712.76', 'L_PTITLE': '11 sf', 'file': '11 sf.csv'}, {'L_LINKID': 'FM 8a', 'L_INVERTLEVEL': '684.78', 'L_CONDHEIGHT': '36', 'L_GROUNDLEVEL': '696.38', 'L_PTITLE': '8a sf', 'file': '8a sf.csv'}, {'L_LINKID': 'FM 26', 'L_INVERTLEVEL': '802.27', 'L_CONDHEIGHT': '30', 'L_GROUNDLEVEL': '807.84', 'L_PTITLE': '26 sf', 'file': '26 sf.csv'}, {'L_LINKID': 'FM 21b', 'L_INVERTLEVEL': '757.152655', 'L_CONDHEIGHT': '18', 'L_GROUNDLEVEL': '771.43', 'L_PTITLE': '21b sf', 'file': '21b sf.csv'}, {'L_LINKID': 'FM 10b', 'L_INVERTLEVEL': '772.54', 'L_CONDHEIGHT': '16', 'L_GROUNDLEVEL': '780.12', 'L_PTITLE': '10b sf', 'file': '10b sf.csv'}, {'L_LINKID': 'FM 27', 'L_INVERTLEVEL': '811.46', 'L_CONDHEIGHT': '27', 'L_GROUNDLEVEL': '820.82', 'L_PTITLE': '27 sf', 'file': '27 sf.csv'}, {'L_LINKID': 'FM 22a', 'L_INVERTLEVEL': '730.87', 'L_CONDHEIGHT': '30', 'L_GROUNDLEVEL': '742.986965', 'L_PTITLE': '22a sf', 'file': '22a sf.csv'}, {'L_LINKID': 'FM 21c', 'L_INVERTLEVEL': '780.75', 'L_CONDHEIGHT': '15', 'L_GROUNDLEVEL': '790.025513', 'L_PTITLE': '21c sf', 'file': '21c sf.csv'}, {'L_LINKID': 'FM 21e', 'L_INVERTLEVEL': '771.77', 'L_CONDHEIGHT': '12', 'L_GROUNDLEVEL': '779.95', 'L_PTITLE': '21e sf', 'file': '21e sf.csv'}, {'L_LINKID': 'FM 8b', 'L_INVERTLEVEL': '717.07', 'L_CONDHEIGHT': '24', 'L_GROUNDLEVEL': '725.47', 'L_PTITLE': '8b sf', 'file': '8b sf.csv'}, {'L_LINKID': 'FM 21a', 'L_INVERTLEVEL': '742.351077', 'L_CONDHEIGHT': '18', 'L_GROUNDLEVEL': '749.741077', 'L_PTITLE': '21a sf', 'file': '21a sf.csv'}, {'L_LINKID': 'FM 21d', 'L_INVERTLEVEL': '791.53', 'L_CONDHEIGHT': '12', 'L_GROUNDLEVEL': '796.78', 'L_PTITLE': '21d sf', 'file': '21d sf.csv'}]}
        assert(data == check_data)

    def test_convert_infoswmm_pipe_results(self):
        infoswmm_csv = 'data/infoswmm2icm/infoswmmm.csv'
        infoswmm_csv = r"data\infoswmm2icm\infoswmm.csv"
        output_folder = 'data/infoswmm2icm'
        convert_infoswmm_pipe_results(infoswmm_csv, output_folder)
        config = get_config('data/infoswmm2icm/flow_config.csv')
        input_csv = r"data\infoswmm2icm\Flow.csv"
        output_csv = r"data\infoswmm2icm\icm_Flow.csv"
        profile = 'flow'
        csv_to_icm_csv(config, input_csv, output_csv, profile, timestamp_fld='timestamp', tparser=None, vfactor=1)


    def test_main(self):
        main()

    def test_rainfall(self):
        rainfall()
