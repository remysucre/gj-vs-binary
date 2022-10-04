#!/usr/bin/env python3

import json
import statistics
import itertools
import numpy as np

import matplotlib.pyplot as plt
plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'


def plot(data):

    # maps each sf to a map query -> time
    ddb = {}
    fj = {}
    gj = {}

    for record in data['duckdb']:
        sf = record['sf']
        q = record['query']
        if ddb.get(q) is None:
            ddb[q] = {}
        ddb[q][sf] = record['time']

    for record in data['gj']:
        xj = fj if record['algo'] == 'fj' else gj
        sf = record['sf']
        q = record['query']
        if xj.get(q) is None:
            xj[q] = {}
        xj[q][sf] = record['time']

    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    lines = {
        'q1': '-',
        'q2': '--',
        'q3': '-.',
        'q4': ':',
        'q5': (5, (10, 3)),
    }

    # '#377eb8', '#ff7f00', '#4daf4a', '#f781bf', '#a65628', '#984ea3', '#999999', '#e41a1c', '#dede00'
    colors = {
        'q1': '#377eb8',
        'q2': '#ff7f00',
        'q3': '#4daf4a',
        'q4': '#f781bf',
        'q5': '#a65628',
    }

    # for each query make a line plot of the query times over sf
    for q in ddb.keys():
        ddb_q = ddb[q]
        gj_q = gj[q]
        fj_q = fj[q]

        x = ddb_q.values()
        y = fj_q.values()
        ax.plot(x, y, linestyle=lines[q], color='black', label=q)
        # ax.scatter(ddb_q[0.1], fj_q[0.1], color=colors[q], marker="o", s=12)

        y = gj_q.values()
        ax.plot(x, y, linestyle=lines[q], alpha=0.35, color='black')
        # ax.scatter(ddb_q[0.1], gj_q[0.1], color=colors[q], marker="s", s=12)

        lims = [
            np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
            np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
        ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('Binary Join time (s)')
    ax.set_ylabel('Free Join / Generic Join time (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)
    ax.legend(loc='upper left')  # , fontsize=8)

    plt.show()


if __name__ == '__main__':
    import sys

    with open(sys.argv[1]) as f:
        data = json.load(f)

    plot(data)
