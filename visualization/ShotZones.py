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
    elif (y < 93):
        return findBaselineZone(x)
    else:
        return findLongZone(x, y)


def isRestricted(x, y):
    return distance(x, y) <= 40


def findBaselineZone(x):
    if (x < -220):
        return "LeftCorner"
    elif (x > -220 and x < -178):
        return "LeftLongMidBaseLine"
    elif (x > -178 and x < -137):
        return "LeftMidBaseLine"
    elif (x > -137 and x < -80):
        return "LeftShortBaseLine"
    elif (x > -80 and x < 0):
        return "LeftPaint"
    elif (x > 220):
        return "RightCorner"
    elif (x < 220 and x > 178):
        return "RightLongBaseLine"
    elif (x < 178 and x > 137):
        return "RightMidBaseLine"
    elif (x < 137 and x > 80):
        return "RightShortBaseLine"
    elif (x < 80 and x > 0):
        return "RightPaint"
    else:
        return "NONE"


def findLongZone(x, y):
    dist = distance(x, y)
    angle = theta(x, y)

    if dist < 165.5 and 180 > angle > 120:
        return "Short2Right"
    elif dist < 165.5 and 120 > angle > 90:
        return "Short2CenterRight"
    elif dist < 165.5 and 90 > angle > 60:
        return "Short2CenterLeft"
    elif dist < 165.5 and 60 > angle > 0:
        return "Short2Left"

    elif 201.5 > dist > 165.5 and 180 > angle > 120:
        return "Mid2Right"
    elif 201.5 > dist > 165.5 and 120 > angle > 90:
        return "Mid2CenterRight"
    elif 201.5 > dist > 165.5 and 90 > angle > 60:
        return "Mid2CenterLeft"
    elif 201.5 > dist > 165.5 and 60 > angle > 0:
        return "Mid2Left"

    elif 237.5 > dist > 201.5 and 180 > angle > 120:
        return "Long2Right"
    elif 237.5 > dist > 201.5 and 120 > angle > 90:
        return "Long2CenterRight"
    elif 237.5 > dist > 201.5 and 90 > angle > 60:
        return "Long2CenterLeft"
    elif 237.5 > dist > 201.5 and 60 > angle > 0:
        return "Long2Left"

    elif 280 > dist > 237.5 and 180 > angle > 120:
        return "Mid3Right"
    elif 280 > dist > 237.5 and 120 > angle > 90:
        return "Mid3CenterRight"
    elif 280 > dist > 237.5 and 90 > angle > 60:
        return "Mid3CenterLeft"
    elif 280 > dist > 237.5 and 60 > angle > 0:
        return "Mid3Left"

    elif dist > 280 and angle > 90:
        return "Long3Right"
    elif dist > 280 and angle < 90:
        return "Long3Left"
    else:
        return "NONE"


def theta(x, y):
    return math.degrees(math.atan2(y, x))


def distance(x, y):
    return math.sqrt((x**2 ) + (y**2))
