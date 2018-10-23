import matplotlib.pyplot as plt
import math
import numpy as np


def sigmoid(x):
    return 1 / (1 + math.exp(-0.21*x))
x_vals = np.linspace(-20, 20, 100)
y = [sigmoid(x) for x in x_vals]

plt.plot(x_vals, y)
plt.show()