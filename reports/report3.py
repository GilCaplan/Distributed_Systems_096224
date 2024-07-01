import random
import numpy as np


def simulation():
    time, curr_state = 0, 0
    while curr_state < 3:
        if curr_state in [0, 1]:
            curr_state += 1
            time += 1
        else:
            if random.random() < min([1, float((curr_state-1) / 10)]):
                curr_state = 0
                time += 1
            else:
                time += 1
                curr_state += 1
    return time


score = []
for _ in range(1000):
    score.append(simulation())
print(np.mean(score))