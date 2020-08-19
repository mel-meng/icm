import unittest
import logging
import os
from flow_survey import *
import filecmp

logger = logging.getLogger()
FORMAT = '[%(levelname)s]%(filename)s %(lineno)-8s %(message)s'
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)

def cmp(f1, f2):
    return filecmp.cmp(f1, f2)

class TestIntegrity(unittest.TestCase):
    def setUp(self):
        pass

    def test_csv_to_flow(self):
        input_csv = 'data/flow_survey/input/Flow.csv'
        out_csv = 'data/flow_survey/output/flow_icm.csv'
        check_csv = 'data/flow_survey/output/flow_icm_check.csv'
        date_fld = 'timestamp'
        ts_seconds = 15*60
        csv_to_icm(input_csv, out_csv, 'flow', date_fld, ts_seconds, start_dt=None, end_dt=None)
        assert(cmp(out_csv, check_csv))

    def test_csv_to_depth(self):
        input_csv = 'data/flow_survey/input/Depth.csv'
        out_csv = 'data/flow_survey/output/depth_icm.csv'
        check_csv = 'data/flow_survey/output/depth_icm_check.csv'
        date_fld = 'timestamp'
        ts_seconds = 15*60
        csv_to_icm(input_csv, out_csv, 'depth', date_fld, ts_seconds, start_dt=None, end_dt=None)
        assert(cmp(out_csv, check_csv))
    def test_csv_to_velocity(self):
        input_csv = 'data/flow_survey/input/Velocity.csv'
        out_csv = 'data/flow_survey/output/velocity_icm.csv'
        check_csv = 'data/flow_survey/output/velocity_icm_check.csv'
        date_fld = 'timestamp'
        ts_seconds = 15*60
        csv_to_icm(input_csv, out_csv, 'velocity', date_fld, ts_seconds, start_dt=None, end_dt=None)
        assert(cmp(out_csv, check_csv))
    def test_csv_to_rainfall(self):
        input_csv = 'data/flow_survey/input/Rainfall.csv'
        out_csv = 'data/flow_survey/output/rainfall_icm.csv'
        check_csv = 'data/flow_survey/output/rainfall_icm_check.csv'
        date_fld = 'timestamp'
        ts_seconds = 15*60
        csv_to_icm(input_csv, out_csv, 'rainfall', date_fld, ts_seconds, start_dt=None, end_dt=None)
        assert(cmp(out_csv, check_csv))

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
