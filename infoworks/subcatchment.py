import pandas as pd

def wastewater_summary(iw_csv, out_csv):
    df = pd.read_csv(iw_csv, skiprows=[1,2])
    g = df.loc[:, ['wastewater_profile', 'population']].groupby('wastewater_profile').agg(['sum', 'count'])
    g.columns = ['population_%s' % x for x in g.columns.droplevel()]
    g.to_csv(out_csv)


