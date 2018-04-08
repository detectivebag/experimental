"""
original reference:
https://github.com/echen original reference is in Ruby
"""

from random import randint, random

import numpy as np
from scipy.stats import beta


def chinese_restaurant_process(num_customers, alpha):
    if num_customers <= 0:
        return []

    table_assignments = [1]  # first customer sits at table 1
    next_open_table = 2  # index of the next empty table

    # Now generate table assignments for the rest of the customers.
    for i in range(1, num_customers + 1):
        if random() < alpha / (alpha + i):
            # Customer sits at new table.
            table_assignments.append(next_open_table)
            next_open_table += 1
        else:
            # Customer sits at an existing table.
            # He chooses which table to sit at by giving equal weight to each
            # customer already sitting at a table.
            which_table = table_assignments[randint(0, len(table_assignments) - 1)]
            table_assignments.append(which_table)

    return table_assignments


def polya_urn_model(base_color_distribution, num_balls, alpha):
    if num_balls <= 0:
        return []

    balls_in_urn = []
    for i in range(1, num_balls + 1):
        if random() < alpha / (alpha + len(balls_in_urn)):
            # Draw a new color, put a ball of this color in the urn.
            new_color = base_color_distribution()
            balls_in_urn.append(new_color)
        else:
            # Draw a ball from the urn, add another ball of the same color.
            ball = balls_in_urn[randint(0, len(balls_in_urn) - 1)]
            balls_in_urn.append(ball)

    return balls_in_urn


def stick_breaking(num_weights, alpha):
    betas = beta.rvs(1, alpha, size=num_weights)
    remaining_stick_lengths = [1, np.cumprod(1 - betas)][1:num_weights]
    weights = remaining_stick_lengths * betas
    return weights


if __name__ == "__main__":
    # experimental one
    print("===============exp1=================")
    print(chinese_restaurant_process(10, 1))
    print("====================================")
    print()

    # experimental two
    print("===============exp2=================")
    unit_uniform = lambda: random()
    print(polya_urn_model(unit_uniform, num_balls=10, alpha=1))
    print("====================================")
    print()

    # experimental three
    print("===============exp3=================")
    print(stick_breaking(10, 1))
    print("====================================")
    print()
