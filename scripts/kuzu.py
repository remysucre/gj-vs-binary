#!/usr/bin/env python3

import json
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'


def plot(data):

    ddb = {}
    fj = {}
    gj = {}
    kz = {}

    for record in data['duckdb']:
        sf = record['sf']
        q = record['query']
        if ddb.get(q) is None:
            ddb[q] = {}
        ddb[q][sf] = record['time']

    for record in data['gj']:
        xj = fj 
        if record['algo'] == 'gj':
            xj = gj
        if record['algo'] == 'kz':
            xj = kz
        sf = record['sf']
        q = record['query']
        if xj.get(q) is None:
            xj[q] = {}
        xj[q][sf] = record['time'][0]

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

    colors = {
        'q1': '#377eb8',
        'q2': '#ff7f00',
        'q3': '#4daf4a',
        'q4': '#f781bf',
        'q5': '#a65628',
    }

    for q in ddb.keys():
        ddb_q = ddb[q]
        gj_q = gj[q]
        fj_q = fj[q]
        kz_q = kz[q]

        x = ddb_q.values()
        y = fj_q.values()
        ax.plot(x, y, linestyle=lines[q], color='black', label=q)

        print('maximum speed up over binary join for query {}: {}'.format(
            q, max([x / y for x, y in zip(x, y)])))

        z = gj_q.values()
        ax.plot(x, z, linestyle=lines[q], alpha=0.4, color='black')

        w = kz_q.values()

        ax.plot(list(x)[0:len(w)], w, linestyle=lines[q], alpha=0.15, color='black')

        print('maximum speed up over generic join for query {}: {}'.format(
            q, max([z / y for z, y in zip(z, y)])))

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

    # save plot as pdf
    plt.savefig('lsqb.pdf', format='pdf')
    # plt.show()


if __name__ == '__main__':
    import sys

    with open(sys.argv[1]) as f:
        data = json.load(f)

    plot(data)
