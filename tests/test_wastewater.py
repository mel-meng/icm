import infoworks as iw
import os

def test_wastewater(ws):
    iw_csv = os.path.join(ws, 'input/wastewater/wastewater.csv')
    output_folder = os.path.join(ws, 'output/wastewater')
    iw.wastewater.convert(iw_csv, output_folder)

    figure_folder = os.path.join(ws, 'output/wastewater/figure')

    iw.wastewater.plot_pattern(output_folder, figure_folder)

def test_wastewater_summary(ws):
    pattern_folder = os.path.join(ws, 'output/wastewater')
    population_csv = os.path.join(ws, 'output/subcatchment/wastewater_summary.csv')
    summary_csv = os.path.join(ws, 'output/subcatchment/wastewater_summary2.csv')
    iw.wastewater.wastewater_summary(pattern_folder, population_csv, summary_csv)

if __name__ == '__main__':
    ws = './workspace'
    # test_wastewater(ws)
    test_wastewater_summary(ws)