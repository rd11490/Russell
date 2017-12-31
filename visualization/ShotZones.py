import math
import matplotlib.colors as colors


def buildShotZones():
    """
    { Zone: { X: [], Y: [] } }

    :return: Dictionary of locations
    """
    zones = {}
    for x in range(-250, 250):
        for y in range(-50, 500):
            zone = findZone(x, y)
            if zone in zones:
                zones[zone]["X"].append(x)
                zones[zone]["Y"].append(y)
            else:
                point = {}
                point["X"] = [x]
                point["Y"] = [y]
                zones[zone] = point

    return zones


def findZone(x, y):
    if (isRestricted(x, y)):
        return "RestrictedArea"
    elif (y < 92 and (x < -220 or x > 220)):
        return findBaselineZone(x)
    else:
        return findLongZone(x, y)


def isRestricted(x, y):
    return distance(x, y) <= 40


def findBaselineZone(x):
    if (x < -220):
        return "LeftCorner"
    elif (x > 220):
        return "RightCorner"
    else:
        return "NONE"


def findLongZone(x, y):
    dist = distance(x, y)
    angle = theta(x, y)

    """
    LeftBaseline11FT, Left11FT, Right11FT, RightBaseline11FT,
      LeftBaseline18FT, Left18FT, Right18FT, RightBaseline18FT,
      LeftBaseline23FT, Left23FT, Right23FT, RightBaseline23FT,
      Left27FT, Right27FT,
      LeftLong3, RightLong3
    """

    if 40 < dist <= 110 and 270 > angle > 157:
        return "LeftBaseline11FT"
    elif 40 < dist <= 110 and 157 > angle > 90:
        return "Left11FT"
    elif 41 < dist <= 110 and 90 > angle > 23:  # 41 is a hack to get the image to look correct
        return "Right11FT"
    elif 41 < dist <= 110 and 23 > angle > -90:  # 41 is a hack to get the image to look correct
        return "RightBaseline11FT"

    elif 180 >= dist > 110 and 270 > angle > 157:
        return "LeftBaseline18FT"
    elif 180 >= dist > 110 and 157 > angle > 90:
        return "Left18FT"
    elif 180 >= dist > 110 and 90 > angle > 23:
        return "Right18FT"
    elif 180 >= dist > 110 and 23 > angle > -90:
        return "RightBaseline18FT"

    elif 237.5 >= dist > 180 and 270 > angle > 157:
        return "LeftBaseline23FT"
    elif 237.5 >= dist > 180 and 157 > angle > 90:
        return "Left23FT"
    elif 237.5 >= dist > 180 and 90 > angle > 23:
        return "Right23FT"
    elif 237.5 >= dist > 180 and 23 > angle > -90:
        return "RightBaseline23FT"

    elif 280 >= dist > 237.5 and angle > 90:
        return "Left27FT"
    elif 280 >= dist > 237.5 and angle < 90:
        return "Right27FT"
    elif dist > 281 and angle > 90:
        return "LeftLong3"
    elif dist > 281 and angle < 90:
        return "RightLong3"

    else:
        return "NONE"


def theta(x, y):
    angle = math.degrees(math.atan2(y, x))
    if x < 0 and angle < 0:
        return 360 + angle
    else:
        return angle


def distance(x, y):
    return math.sqrt((x ** 2) + (y ** 2))
