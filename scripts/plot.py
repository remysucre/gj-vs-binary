#!/usr/bin/env python3

import re

import plotly.express as px

# import matplotlib
# matplotlib.rcParams['pdf.fonttype'] = 42
# matplotlib.rcParams['ps.fonttype'] = 42
# matplotlib.rcParams['savefig.bbox'] = 'tight'


def parse_gj(filename):
    print(f'parsing {filename}')
    pats = dict(
        query=r'running query \w+: IMDBQ(\d+)',
        dd_total=r'DUCKDB total time: ([\d\.]*)',
        dd_filter=r'DUCKDB filter time: ([\d\.]*)',
        dd_join=r'DUCKDB join time: ([\d\.]*)',
        build=r'Total building takes ([\d\.]*)',
        join=r'Total joining takes ([\d\.]*)',
        total=r'Total takes ([\d\.]*)',
    )
    regexp = ".*?".join(pats.values())

    with open(filename) as f:
        text = f.read()

    data = []
    for m in re.finditer(regexp, text, re.DOTALL):
        # print(m.groups())
        groups = map(float, m.groups())
        data.append(dict(zip(pats.keys(), groups)))

    return data


def plot(gjs):

    # get an arbitrary data list to plot duckdb stuff
    # data = list(gjs.values())[0]
    data = list(gjs.values())[0]

    for name, gj in gjs.items():
        for i, d in enumerate(gj):
            assert d['query'] == data[i]['query']
            data[i][name] = d['total']

    for d in data:
        d['query'] = str(int(d['query']))
        del d['total']

    print(data[:5])

    # sort
    def sort_key(x): return x['dd_join']
    data.sort(key=sort_key)

    ys = ['dd_total', 'dd_filter', 'dd_join', 'build'] + list(gjs.keys())

    fig = px.line(
        data, x='query', y=ys,
        # barmode='group',
    )
    fig.update_layout(
        title='IMDB Query Times',
        yaxis_title='Time (s)',
    )
    fig.write_html('plot.html')


if __name__ == '__main__':
    import sys

    gjs = {
        filename: parse_gj(filename)
        for filename in sys.argv[1:]
    }

    plot(gjs)
