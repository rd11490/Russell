import random
import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit

class Team:
    def __init__(self, twoRt, twoPct, threePct):
        self.twoRt = twoRt
        self.twoPct = twoPct
        self.threeRt = 1-self.twoRt
        self.threePct = threePct

    def determineShot(self):
        rand = random.random()
        if rand < self.twoRt:
            shotChance = random.random()
            if shotChance <= self.twoPct:
                return 2
        else:
            shotChance = random.random()
            if shotChance <= self.twoPct:
                return 3
        return 0

    def calculateScore(self, pace):
        score = 0
        for s in range(pace):
            score += self.determineShot()
        return score


def playGame(team1, team2, pace):
    score1 = team1.calculateScore(pace)
    score2 = team2.calculateScore(pace)

    return score1 - score2


def simulateGame(pace):
    celtics = Team(.6061, .515, .3939, .359)
    cavs = Team(.6009, .528, .3991, .384)
    results = []
    for i in range(1000):
        results.append(playGame(celtics, cavs, pace))
    return results


def simulateGames():
    results = {}
    for p in [80, 90, 100, 110, 120]:
        print("Pace: {}".format(p))
        results[p] = simulateGame(p)
    return results


def visualize():
    results = simulateGames()
    keys = results.keys()
    plt.figure()
    for i, k in enumerate(keys):
        values = results[k]
        plt.subplot(5, 1, i+1)
        hist, bin_edges = np.histogram(values, bins=range(-50, 50))
        test = curve_fit(xdata=bin_edges[:-1],ydata=hist)
        plt.plot(bin_edges[:-1], hist, label=str(k))
        plt.xlim(min(bin_edges), max(bin_edges))
        plt.legend()
    plt.xlim(-50, 50)
    plt.show()


visualize()

"""
Celtics:
985	2742	.359	2183	4236	.515
Shots = 6978
3rt = .3939
3ptc = .359
2rt = .6061
2pct = .515
Cavs:
1067	2779	.384	2208	4184	.528
shots = 6963
3rt = .3991
3pct = .384
2rt = .6009
2rt = .528

"""
