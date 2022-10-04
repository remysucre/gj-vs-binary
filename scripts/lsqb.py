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

    # for each query make a line plot of the query times over sf
    for q in ddb.keys():
        ddb_q = ddb[q]
        gj_q = gj[q]
        fj_q = fj[q]

        print(gj_q)

        ax.plot(ddb_q.values(), fj_q.values(),
                linestyle=lines[q], label='FJ {}'.format(q))

        # ax.plot(ddb_q.values(), gj_q.values(),
        #         linestyle=lines[q], label='GJ {}'.format(q))

        lims = [
            np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
            np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
        ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.show()


if __name__ == '__main__':
    import sys

    with open(sys.argv[1]) as f:
        data = json.load(f)

    plot(data)
