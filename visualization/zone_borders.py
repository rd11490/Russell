import ShotZones
import numpy as np
import math
import matplotlib.pyplot as plt

def check_size(arr, i, j):
    if i == 499 or j == 549 or i == 0 or j == 0:
        return True
    cnt = 0
    if arr[i, j-1] == 1:
        cnt += 1
    if arr[i, j+1] == 1:
        cnt += 1
    if arr[i-1, j] == 1:
        cnt += 1
    if arr[i+1, j] == 1:
        cnt += 1

    return cnt < 4



# zones = ShotZones.buildShotZones()
#
# for z in zones:
#     arr = np.zeros((500, 550))
#     print(z)
#     pts = len(zones[z]['X'])
#     for i in range(0, pts):
#         arr[zones[z]['X'][i]+250, zones[z]['Y'][i]+50] = 1
#
#     boundary = []
#     for i in range(0, 500):
#         for j in range(0, 550):
#             if arr[i, j] == 1:
#                 if check_size(arr, i, j):
#                     boundary.append([i-250, j-50])
#     print(boundary)



# pts = []
# for i in range(-220, 1):
#     y = math.sqrt(238.5 ** 2 - i ** 2)
#     pts.append([i, y])
#
# for i in range(-220, 1):
#     y = math.sqrt(238.5 ** 2 - i ** 2)
#     pts.append([i, y])
#
# pts.append([0, 238])
# pts.append([0, 422.5])
# pts.append([-250, 422.5])
# pts.append([-250, 92.5])
# pts.append([-220, 92.5])
#
# print(pts)
#
# x = [v[0] for v in pts]
# y = [v[1] for v in pts]
#
# plt.plot(x, y)
# plt.show()

pts = []
for j in range(-48, 71):
    i = math.sqrt(180**2 - j**2)
    pts.append([i, j])


pts.append([220, 92.5])
pts.append([220, -47.5])

print(pts)

x = [v[0] for v in pts]
y = [v[1] for v in pts]


plt.plot(x, y)
plt.show()