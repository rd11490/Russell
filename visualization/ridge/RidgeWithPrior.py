import math
import numpy as np
from sklearn.linear_model import RidgeCV


class RidgeRegression:
    def __init__(self):
        pass

    def fit(self, X, Y, prior=None, lmbda=0.001, epochs=1000, min_err=-math.inf):
        if prior is None:
            B = np.zeros(shape=(X.shape[1], 1))
            prior = B.copy()
        else:
            B = prior.copy()

        i = 0
        err_mag = 0
        n = len(Y)
        while i < epochs and err_mag > min_err:
            pred = self.predict(X, B)
            err = pred-Y
            err_mag = np.linalg.norm(err)
            if err_mag > min_err:
                ols = X.T.dot(err)/n
                reg = np.linalg.norm(B - prior)
                B -= (ols - (2 * lmbda/n)*reg)
            i += 1
        return B

    def predict(self, X, B):
        return X.dot(B)


def ridge_with_prior(X, Y, weights=None, prior = None, lmbdas=None):
    if lmbdas is None:
        lmbdas = [0.1]
    if prior is None:
        prior = np.zeros(shape=(X.shape[1], 1))
    if weights is None:
        weights= np.ones(shape=(len(Y)))

    alphas = [lambda_to_alpha(l, X.shape[0]) for l in lmbdas]
    clf = RidgeCV(alphas=alphas, cv=5, fit_intercept=True, normalize=False)
    Yn = Y - X.dot(prior)
    model = clf.fit(X, Yn, sample_weight=weights)
    return model

def lambda_to_alpha(lambda_value, samples):
    return (lambda_value * samples) / 2.0