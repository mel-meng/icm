import logging
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def conveyance_at_level(xs, ys, ns, ps, level):
    """
    given water level, cut the cross section with only lines touching the water
    :param xs: list of offset
    :param ys: list of z
    :param ns: list of manning's n
    :param ps: list of panel markers, 0 no, 1 yes
    :param level: water level
    :return: the wetted perimeter line, for lines that is under the ground, the n is set as 0, None if level is lower than the xs
    """
    # make a copy of all the lines
    panels = []
    line = []
    x = list(xs)
    y = list(ys)
    p = list(ps)  # panel markers
    n = list(ns)
    results = []
    if level <= np.min(ys):  # water level below the cross section
        return pd.DataFrame(results)

    # add panel marker to the first point
    p[0] = 1
    panel = x[0]

    for i in range(1, len(x)):
        logging.debug('processing line segment(starting from 1):%s' % i)
        pt1 = (x[i - 1], y[i - 1], n[i - 1], p[i - 1])
        pt2 = (x[i], y[i], n[i], p[i])
        if pt1[3] == 1:  # panel marker
            panel = pt1[0]

        line_result = calculate_line_segment(level, pt1, pt2, panel)

        if line_result:
            wp, ws, rn, area, panel = line_result
            wp_len = get_length([wp[0][0], wp[1][0]], [wp[0][1], wp[1][1]])
            ws_len = get_length([ws[0][0], ws[1][0]], [ws[0][1], ws[1][1]])
            results.append(dict(
                zip(['wp', 'ws', 'wp_len', 'ws_len', 'n', 'area', 'panel'], [wp, ws, wp_len, ws_len, rn, area, panel])))

        else:
            logging.info('water below the line')
    return pd.DataFrame(results)


def conveyance_curve(xs, ys, ns, ps):
    level_list = sorted(set(list(ys)))
    con_list = []
    for i, level in enumerate(level_list):
        results = conveyance_at_level(xs, ys, ns, ps, level)
        if i == 0:
            if (results is None) or results.empty:
                summary = {'level': level}
                for v in ['k', 'wp', 'ws', 'area']:
                    summary[v] = 0
                con_list.append(summary)
        else:
            if not results.empty:

                data = get_panel(results)
                summary = {'level': level}
                for v in ['k', 'wp', 'ws', 'area']:
                    summary[v] = data[v].sum()
                con_list.append(summary)
    df = pd.DataFrame(con_list)

    df['depth'] = df['level'] - min(ys)
    return df


def get_panel(results):
    rows = []
    for panel in results['panel'].unique():
        data = results.loc[results['panel'] == panel]
        area = data['area'].sum()
        wp = data['wp_len'].sum()
        ws = data['ws_len'].sum()
        n_average = (data['n'] * data['wp_len']).sum() / float(wp)
        k = 1.49 / n_average * area * np.power(area / wp, 2 / 3.0)
        rows.append({'area': area, 'wp': wp, 'n': n_average, 'ws': ws, 'k': k, 'panel': panel})
    df = pd.DataFrame(rows)
    return df


def calculate_line_segment(level, pt1, pt2, panel):
    """
    Given a line segment from the cross secton and the water level, caluclate the parameters
    :param level: water level
    :param pt1: first point
    :param pt2: second point
    :param panel: the panel this line segment is in
    :return: (wp, ws, n, area, panel)
    """

    x0, y0, n0, p0 = pt1
    x1, y1, n1, p1 = pt2
    if p0 == 1:
        panel = x0  # panel is defined as x0
    level_status = ''  # compare level and the line segment
    if level < min(y0, y1):
        level_status = 'below'
    elif level > max(y0, y1):
        level_status = 'above'
    elif min(y0, y1) <= level <= max(y0, y1):
        level_status = 'cross'
    # special case
    # TODO: make sure my assumptions are right
    if y1 == y0:  # flat line, should work the same
        # if y0 == level, should it count as wp or not?
        pass
    if x0 == x1:  # vertical line, should be the same
        pass

    if level_status == 'below':
        return None
    elif level_status == 'cross':
        if x0 == x1:
            pt = [x0, level]
        else:
            if y0 == y1:  # flat
                pt = [x1, y1]
            else:
                pt = line_intersection([[x0, y0], [x1, y1]], [[x0, level], [x1, level]])
        if y0 > y1:
            if y1 == level:
                return None
            wp = [(pt[0], pt[1]), (x1, y1)]  # wetted perimeter
            ws = [(pt[0], level), (x1, level)]  # water surface
            n = n0
            area = (x1 - pt[0]) * (level - y1) / 2.0
            return wp, ws, n, area, panel
        else:
            if y0 == level and (y0 != y1):
                return None
            wp = [(x0, y0), (pt[0], pt[1])]  # wetted perimeter
            ws = [(x0, level), (pt[0], level)]  # water surface
            n = n0
            area = (pt[0] - x0) * (level - y0) / 2.0
            return wp, ws, n, area, panel
    elif level_status == 'above':
        wp = [(x0, y0), (x1, y1)]  # wetted perimeter
        ws = [(x0, level), (x1, level)]  # water surface
        n = n0
        area = ((level - y0) + (level - y1)) * (x1 - x0) / 2.0
        return wp, ws, n, area, panel


def get_length(x, y):
    """
    get polyline length
    :param x: a list of x of the polyline
    :param y: a list of y of the polyline
    :return:
    """
    # TODO: for W shaped xs, the water is separated in two channels, and need to remove the water surface length.
    points = np.array([[x, y] for x, y in zip(x, y)])
    d = np.diff(points, axis=0)
    return np.sqrt((d ** 2).sum(axis=1)).sum()


def line_intersection(line1, line2):
    """
    get intersection of two lines
    :param line1: x1, y1
    :param line2:  x2, y2
    :return: x, y of the intersection
    """
    # https://stackoverflow.com/questions/20677795/how-do-i-compute-the-intersection-point-of-two-lines/20679579
    xdiff = (line1[0][0] - line1[1][0], line2[0][0] - line2[1][0])
    ydiff = (line1[0][1] - line1[1][1], line2[0][1] - line2[1][1])

    def det(a, b):
        return a[0] * b[1] - a[1] * b[0]

    div = det(xdiff, ydiff)
    if div == 0:
        raise Exception('lines do not intersect')

    d = (det(*line1), det(*line2))
    x = det(d, xdiff) / div
    y = det(d, ydiff) / div
    return x, y


def plot_conveyance_curve(df, title='conveyance curve', xlabel='Conveyance', ylabel='Depth(ft)'):
    sns.set()

    df['dk'] = df['k'].diff()
    df['dlevel'] = df['level'].diff()
    df['dkdlevel'] = df['dk'] / df['dlevel']
    fig = plt.plot(df['dk'], df['level'], marker='o')
    fig = plt.plot(df['dkdlevel'], df['level'], marker='o')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    return fig


def plot_conveyance(df, xs_name):
    if not ('offset' in df.columns):
        df['length'] = df.loc[:, ['X', 'Y']].diff().apply(lambda x: np.sqrt(x['X'] ** 2 + x['Y'] ** 2), axis=1).fillna(
            0)
        df['offset'] = df['length'].cumsum()
    xs = df['offset'].values
    ys = df['Z'].values
    ns = df['roughness_N'].values
    ps = df['new_panel'].values
    df_convey = conveyance_curve(xs, ys, ns, ps)

    rows = []

    sns.set()

    fig, axes = plt.subplots(2, 2, sharey='row', sharex='col',
                             gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.01, 'wspace': 0.01}, figsize=(15, 12))
    dk_ax = axes[0][1]

    shay = dk_ax.get_shared_y_axes()
    shay.remove(dk_ax)
    dk_ax.yaxis.set_label_position("right")
    # dk_ax.yaxis.tick_right()

    curve_ax = axes[1, 1]
    curve_ax.yaxis.set_label_position("right")
    # curve_ax.yaxis.tick_right()

    # sns.lineplot(x='offset', y='Z', data=df, ax=axes[1])
    # cross section
    axes[1][0].plot(df['offset'], df['Z'], marker='o')
    axes[1][0].set_ylabel('Bed Elevation')
    axes[1][0].set_xlabel('Offset')
    zmax = max(df['Z'])
    zmin = min(df['Z'])
    # panel marker
    for x in df.loc[df['new_panel'] == 1, 'offset'].values:
        axes[1][0].plot([x, x], [zmin, zmax], linestyle='--', color='grey')
    axes[0][0].step(df['offset'], df['roughness_N'], where='post')
    axes[0][0].set_ylabel('Roughness')
    # conveyance curve
    axes[1][1].plot(df_convey['k'], df_convey['level'], marker='o')
    axes[1][1].set_xlabel('conveyance')
    axes[1][1].set_ylabel('Water Level', labelpad=10)
    # plot conveyance changes
    df_convey['dk'] = df_convey['k'].diff() / df_convey['level'].diff()
    df_convey['dk'] = df_convey['dk'].fillna(0)

    for idx, r in df_convey.iterrows():
        c = r['k']
        l = r['level']
        dk = r['dk']

        if dk < 0:
            axes[0][1].plot([c, c], [0, dk], 'r-')
            axes[1][0].plot([min(df['offset'].values), max(df['offset'].values)], [l, l], 'r--')
            axes[1][1].plot([min(df_convey['k'].values), max(df_convey['k'].values)], [l, l], 'r--')
            axes[1][1].plot([c, c], [min(df_convey['level'].values), max(df_convey['level'].values)], 'g--')
        else:
            axes[0][1].plot([c, c], [0, dk], 'b-')

    axes[0][1].set_ylabel('dk/dlevel')  # , labelpad=20)
    # a = min(df_convey['dk'].values)
    # b = max(df_convey['dk'].values)
    # c = (b - a)/10
    # axes[0][1].yaxis.set_ticks(np.arange(a, b, c))
    # axes[0][1].tick_params(colors='r')

    # sf = (np.max(df_convey['k'].values) - np.min(df_convey['k'].values))/(np.max(dk.values) - np.min(dk.values))
    # dk = dk*sf
    # dk = dk - np.min(dk.values)
    #
    # axes[1][1].plot(dk, df_convey['level'], color='red', marker='x', linestyle='--')
    fig.suptitle(xs_name)
    plt.tight_layout()
    plt.setp([a.get_xticklabels() for a in [axes[0][0], axes[0][1], axes[1][1]]], visible=True)
    return fig
