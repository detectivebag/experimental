"""
below code is copied from:
https://github.com/joelgrus/stupid-itertools-tricks-pydata
"""

import random
from functools import partial
from itertools import accumulate

import matplotlib.pyplot as plt
from explorex.utils.basic import *
from matplotlib import animation


def k_meanses(points, k):
    initial_means = random.sample(points, k)
    return iterate(partial(new_means, points),
                   initial_means)


def no_repeat(prev, curr):
    if prev == curr:
        raise StopIteration
    else:
        return curr


def until_convergence(it):
    return accumulate(it, no_repeat)


def new_means(points, old_means):
    k = len(old_means)
    assignments = [closest_index(point, old_means)
                   for point in points]
    clusters = [[point
                 for point, c in zip(points, assignments)
                 if c == j] for j in range(k)]
    return [cluster_mean(cluster) for cluster in clusters]


def closest_index(point, means):
    return min(enumerate(means),
               key=lambda pair: squared_distance(point, pair[1]))[0]


def squared_distance(p, q):
    return sum((p_i - q_i) ** 2 for p_i, q_i in zip(p, q))


def cluster_mean(points):
    num_points = len(points)
    dim = len(points[0]) if points else 0
    sum_points = [sum(point[j] for point in points)
                  for j in range(dim)]
    return [s / num_points for s in sum_points]


def run_kmeans_animation(seed=0, k=5, data=None, output_filename=None):
    output_filename = output_filename or 'kmeans2.gif'
    random.seed(seed)
    data = data or [(random.choice([0, 1, 2, 4, 5]) + random.random(),
                     random.normalvariate(0, 1)) for _ in range(500)]
    meanses = [mean for mean in until_convergence(k_meanses(data, k))]

    # colors = random.sample(list(matplotlib.colors.cnames), k)
    colors = ['r', 'g', 'b', 'c', 'm']

    def animation_frame(nframe):
        means = meanses[nframe]
        plt.cla()
        assignments = [closest_index(point, means)
                       for point in data]
        clusters = [[point
                     for point, c in zip(data, assignments)
                     if c == j] for j in range(k)]

        for cluster, color, mean in zip(clusters, colors, means):
            x, y = zip(*cluster)
            plt.scatter(x, y, color=color)
            plt.plot(*mean, color=color, marker='*', markersize=20)

    fig = plt.figure(figsize=(5, 4))
    anim = animation.FuncAnimation(fig, animation_frame, frames=len(meanses))
    anim.save(output_filename, writer='imagemagick', fps=4)
