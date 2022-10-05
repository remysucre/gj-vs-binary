#!/usr/bin/env python3

import json
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'

colors = ['#377eb8', '#ff7f00', '#4daf4a', '#f781bf', '#a65628']


def plot(good_data, bad_data):

    ddb_good = good_data['duckdb']
    ddb_bad = bad_data['duckdb']
    gj_good = good_data['gj']
    gj_bad = bad_data['gj']

    ddb_good = {d['query']: d['time'] for d in ddb_good}
    ddb_bad = {d['query']: d['time'] for d in ddb_bad}
    fj_good = {d['query']: d['time'] for d in gj_good if d['optimize'] == 1}
    fj_bad = {d['query']: d['time'] for d in gj_bad if d['optimize'] == 1}
    gj_good = {d['query']: d['time'] for d in gj_good if d['optimize'] == 0}
    gj_bad = {d['query']: d['time'] for d in gj_bad if d['optimize'] == 0}

    ddb_slowdown = [ddb_bad[q] / ddb_good[q] for q in ddb_good]
    fj_slowdown = [fj_bad[q][0] / fj_good[q][0] for q in fj_good]
    gj_slowdown = [gj_bad[q][0] / gj_good[q][0] for q in gj_good]

    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    ax.scatter(ddb_bad.values(), fj_bad.values(), s=5,
               color='black', label='Free Join')
    ax.scatter(ddb_bad.values(), gj_bad.values(), s=5,
               color='silver', label='Generic Join')

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

    plt.legend(loc='upper left')
    plt.savefig('robust.pdf', format='pdf')


if __name__ == '__main__':
    import sys

    with open(sys.argv[1]) as f:
        good_data = json.load(f)
    with open(sys.argv[2])as f:
        bad_data = json.load(f)

    plot(good_data, bad_data)
