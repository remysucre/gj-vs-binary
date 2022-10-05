#!/usr/bin/env python3

import json
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'

colors = ['#377eb8', '#ff7f00', '#4daf4a', '#f781bf', '#a65628']


def plote2e(data):

    ddb = data['duckdb']
    ddb = {d['query']: d['time'] for d in ddb}

    gj = data['gj']
    gj = {d['query']: d['time'] for d in gj}

    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    x = ddb.values()
    y = gj.values()
    ax.scatter(x, y, s=5, label='occurence merging')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('time w/ basic merging (s)')
    ax.set_ylabel('time w/ further merging (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('e2e.pdf', format='pdf')


def plot(data):

    ddb = data['duckdb']
    ddb = {d['query']: d['time'] for d in ddb}

    gj = data['gj']

    ablate_opt = {}
    ablate_trie = {}
    ablate_vec = {}

    for i, record in enumerate(gj):
        j = i % 10
        match j:
            case 0 | 1 | 2:
                if ablate_opt.get(j) is None:
                    ablate_opt[j] = {}
                ablate_opt[j][record['query']] = record['time'][0]
            case 3 | 4 | 5:
                if ablate_trie.get(j) is None:
                    ablate_trie[j] = {}
                ablate_trie[j][record['query']] = record['time'][0]
            case 6 | 7 | 8 | 9:
                if ablate_vec.get(j) is None:
                    ablate_vec[j] = {}
                ablate_vec[j][record['query']] = record['time'][0]

    # e2e
    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    x = ddb.values()
    y = ablate_opt[1].values()
    ax.scatter(x, y, s=5, label='occurence merging')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('time w/ basic merging (s)')
    ax.set_ylabel('time w/ further merging (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('e2e.pdf', format='pdf')

    # merging ablation
    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    y0 = ablate_opt[0].values()
    y1 = ablate_opt[1].values()
    y2 = ablate_opt[2].values()
    ax.scatter(y0, y2, s=5, color='silver', label='occurence merging')
    ax.scatter(y0, y1, s=5, color='black', label='variable merging')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('time w/ basic merging (s)')
    ax.set_ylabel('time w/ further merging (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('merge.pdf', format='pdf')

    # trie ablation
    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    y0 = ablate_trie[3].values()
    y1 = ablate_trie[4].values()
    y2 = ablate_trie[5].values()
    ax.scatter(y0, y1, s=5, color='silver', label='SLT')
    ax.scatter(y0, y2, s=5, color='black', label='COLT')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('time w/ simple trie (s)')
    ax.set_ylabel('time w/ lazy tries (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('trie.pdf', format='pdf')

    # vector ablation
    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    y0 = ablate_vec[6].values()
    y1 = ablate_vec[7].values()
    y2 = ablate_vec[8].values()
    y3 = ablate_vec[9].values()

    ax.scatter(y0, y1, s=5, color='lightgray', label='batch 10x')
    ax.scatter(y0, y2, s=5, color='lightgray', label='batch 100x')
    ax.scatter(y0, y3, s=5, color='black', label='batch 1000x')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('time w/ simple trie (s)')
    ax.set_ylabel('time w/ lazy tries (s)')
    ax.set_xlim(lims)
    ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('vec.pdf', format='pdf')

    # indexes = {d['query']: i for i, d in enumerate(ddb)}
    # gj = sorted(data['gj'], key=lambda x: indexes[x['query']])

    # ddb = {}
    # fj = {}
    # gj = {}

    # for record in data['duckdb']:
    #     sf = record['sf']
    #     q = record['query']
    #     if ddb.get(q) is None:
    #         ddb[q] = {}
    #     ddb[q][sf] = record['time']

    # for record in data['gj']:
    #     xj = fj if record['algo'] == 'fj' else gj
    #     sf = record['sf']
    #     q = record['query']
    #     if xj.get(q) is None:
    #         xj[q] = {}
    #     xj[q][sf] = record['time']

    # fig, ax = plt.subplots()
    # plt.xscale('log')
    # plt.yscale('log')

    # lines = {
    #     'q1': '-',
    #     'q2': '--',
    #     'q3': '-.',
    #     'q4': ':',
    #     'q5': (5, (10, 3)),
    # }

    # colors = {
    #     'q1': '#377eb8',
    #     'q2': '#ff7f00',
    #     'q3': '#4daf4a',
    #     'q4': '#f781bf',
    #     'q5': '#a65628',
    # }

    # for q in ddb.keys():
    #     ddb_q = ddb[q]
    #     gj_q = gj[q]
    #     fj_q = fj[q]

    #     x = ddb_q.values()
    #     y = fj_q.values()
    #     ax.plot(x, y, linestyle=lines[q], color='black', label=q)

    #     y = gj_q.values()
    #     ax.plot(x, y, linestyle=lines[q], alpha=0.35, color='black')

    #     lims = [
    #         np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
    #         np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    #     ]

    # ax.plot(lims, lims, color='gray', linewidth=0.5)
    # ax.set_aspect('equal')
    # ax.set_xlabel('Binary Join time (s)')
    # ax.set_ylabel('Free Join / Generic Join time (s)')
    # ax.set_xlim(lims)
    # ax.set_ylim(lims)
    # ax.legend(loc='upper left')  # , fontsize=8)

    # # save plot as pdf
    # plt.savefig('lsqb.pdf', format='pdf')
    # # plt.show()


if __name__ == '__main__':
    import sys

    with open(sys.argv[1]) as f:
        data = json.load(f)

    # plote2e(data)
    plot(data)
