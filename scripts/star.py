#!/usr/bin/env python3

import json
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'


def plot(good_data, bad_data):

    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

duckdb": [
    {
      "query": "s100",
      "time": 0.000755
    },
    {
      "query": "s300",
      "time": 0.002428
    },
    {
      "query": "s1000",
      "time": 0.017643
    },
    {
      "query": "s3000",
      "time": 0.092013
    },
    {
      "query": "s10000",
      "time": 0.658043
    },
    {
      "query": "s30000",
      "time": 6.264033
    }
  ]

    x = 
    y = [0.000049083, 0.000066666, 0.000104, 0.000186458, 0.000343791, 0.000739166]  

    ax.scatter(ddb_slowdown, fj_slowdown, s=5,
               color='black', label='Free Join')

    lims = [
        np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
        np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    ]

    ax.plot(lims, lims, color='gray', linewidth=0.5)
    ax.set_aspect('equal')
    ax.set_xlabel('Binary Join slowdown')
    ax.set_ylabel('Free Join / Generic Join slowdown')
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
