#!/usr/bin/env python3

import re
import statistics
import matplotlib.pyplot as plt
import numpy as np

import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
matplotlib.rcParams['savefig.bbox'] = 'tight'


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
        print(m.groups())
        groups = map(float, m.groups())
        data.append(dict(zip(pats.keys(), groups)))

    return data


def plot(gjs):

    # get an arbitrary data list to plot duckdb stuff
    data = list(gjs.values())[0]

    # sort
    def sort_key(x): return x['dd_join']
    data.sort(key=sort_key)
    for data in gjs.values():
        data.sort(key=sort_key)

    ind = np.arange(len(data))

    fig, ax = plt.subplots()

    ax.plot([q['dd_total'] for q in data], label='duckdb total', color='black')
    ax.plot([q['dd_filter'] for q in data], label='duckdb filter',
            color='black', linestyle='--')
    ax.plot([q['dd_join'] for q in data], label='duckdb join',
            color='black', linestyle=':')
    ax.plot([q['build'] for q in data], label='build',
            color='blue', linestyle='--')

    for name, gj in gjs.items():
        ax.plot([q['total'] for q in gj], label=name)

    ax.set_xlabel('IMDB Query')
    ax.set_xticks(ind)
    ax.set_xticklabels([int(q['query']) for q in data])
    ax.set_ylabel('Run time')
    ax.legend()


if __name__ == '__main__':
    import sys

    gjs = {
        filename: parse_gj(filename)
        for filename in sys.argv[1:]
    }

    plot(gjs)
    plt.savefig(f"plot.png")
    plt.savefig(f"plot.pdf")
    plt.show()
