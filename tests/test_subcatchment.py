import infoworks as iw
import os

def test_subcatchment(ws):
    iw_csv = os.path.join(ws, 'input/subcatchment/subcatchment.csv')
    output_folder = os.path.join(ws, 'output/subcatchment')
    out_csv = os.path.join(output_folder, 'wastewater_summary.csv')
    iw.subcatchment.wastewater_summary(iw_csv, out_csv)


if __name__ == '__main__':
    ws = './workspace'
    test_subcatchment(ws)