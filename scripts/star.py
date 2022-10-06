#!/usr/bin/env python3

import json
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['pdf.fonttype'] = 42
plt.rcParams['ps.fonttype'] = 42
plt.rcParams['savefig.bbox'] = 'tight'


def plot():

    fig, ax = plt.subplots()
    plt.xscale('log')
    plt.yscale('log')

    n = [100, 300, 1000, 3000, 10000, 30000]
    x = [0.000755, 0.002428, 0.017643, 0.092013, 0.658043, 6.264033]
    y = [0.000049083, 0.000066666, 0.000104,
         0.000186458, 0.000343791, 0.000739166]

    ax.plot(n, y, marker='o', ms=4, color='black', label='Free Join')
    ax.plot(n, x, marker='o', ms=4, color='silver', label='Binary Join')

    # lims = [
    #     np.min([ax.get_xlim(), ax.get_ylim()]),  # min of both axes
    #     np.max([ax.get_xlim(), ax.get_ylim()]),  # max of both axes
    # ]

    # ax.plot(lims, lims, color='gray', linewidth=0.5)
    # ax.set_aspect('equal')
    ax.set_xlabel('input size')
    ax.set_ylabel('Free Join / Binary Join time (s)')
    # ax.set_xlim(lims)
    # ax.set_ylim(lims)

    plt.legend(loc='upper left')
    plt.savefig('star.pdf', format='pdf')


if __name__ == '__main__':
    import sys

    plot()
