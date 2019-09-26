# 2x1 + 3x2 -7x3 + X4 = Y

import numpy as np
import random
from ridge.RidgeWithPrior import *


rows = 10
X = np.random.rand(rows,4)

def calc_funct(row):
    return 2*row[0] + 3*row[1] -7*row[2] + row[3] + (2*random.random()-1)

Y = np.apply_along_axis(calc_funct, 1, X).reshape(rows, 1)
prior = np.array([2.0, 3.0, -7.0, 1.0]).reshape(4, 1)

model = RidgeRegression()
B = model.fit(X=X, Y=Y, prior = prior, epochs=1000, lmbda=0.1)
print('With Prior')
print(B)
print(np.linalg.norm(Y-model.predict(X, B)))
print('\n')


B2 = model.fit(X=X, Y=Y, prior = None, epochs=1000, lmbda=0.1)
print('With Out Prior')
print(B2)
print(np.linalg.norm(Y-model.predict(X, B2)))
print('\n')



Yn = Y-X.dot(prior)

B3 = model.fit(X=X, Y=Yn, prior = None, epochs=1000, lmbda=0.1)
print('With Out Prior')
print(B3)
print('Add Back in Prior')
print(B3 + prior)
print(np.linalg.norm(Y-model.predict(X, B3 + prior)))

print('\n')


model = ridge_with_prior(X=X, Y=Y, lmbdas=[0.1])
B4 = model.coef_.T
print('With Out Prior')
print(B4)

print(np.linalg.norm(Y-X.dot(B4)))
print('\n')

model = ridge_with_prior(X=X, Y=Y, prior=prior, lmbdas=[0.1])
B5o = model.coef_.T
print('With Out Prior')
print(B5o)
print('Add Back in Prior')
B5 = B5o + prior
print(B5)
print(np.linalg.norm(Y-X.dot(B5)))
