from matplotlib.patches import Circle, Rectangle, Arc
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


# Function to draw the basketball court lines
def draw_full_court(ax=None, color="gray", lw=1, zorder=0):
    if ax is None:
        ax = plt.gca()

    # Creates the out of bounds lines around the court
    outer = Rectangle((0, 0), width=94, height=50, color=color,
                      zorder=zorder, fill=False, lw=lw)

    # The left and right basketball hoops
    l_hoop = Circle((5.35, 25), radius=.75, lw=lw, fill=False,
                    color=color, zorder=zorder)
    r_hoop = Circle((88.65, 25), radius=.75, lw=lw, fill=False,
                    color=color, zorder=zorder)

    # Left and right backboards
    l_backboard = Rectangle((4, 22), 0, 6, lw=lw, color=color,
                            zorder=zorder)
    r_backboard = Rectangle((90, 22), 0, 6, lw=lw, color=color,
                            zorder=zorder)

    # Left and right paint areas
    l_outer_box = Rectangle((0, 17), 19, 16, lw=lw, fill=False,
                            color=color, zorder=zorder)
    l_inner_box = Rectangle((0, 19), 19, 12, lw=lw, fill=False,
                            color=color, zorder=zorder)
    r_outer_box = Rectangle((75, 17), 19, 16, lw=lw, fill=False,
                            color=color, zorder=zorder)

    r_inner_box = Rectangle((75, 19), 19, 12, lw=lw, fill=False,
                            color=color, zorder=zorder)

    # Left and right free throw circles
    l_free_throw = Circle((19, 25), radius=6, lw=lw, fill=False,
                          color=color, zorder=zorder)
    r_free_throw = Circle((75, 25), radius=6, lw=lw, fill=False,
                          color=color, zorder=zorder)

    # Left and right corner 3-PT lines
    # a represents the top lines
    # b represents the bottom lines
    l_corner_a = Rectangle((0, 3), 14, 0, lw=lw, color=color,
                           zorder=zorder)
    l_corner_b = Rectangle((0, 47), 14, 0, lw=lw, color=color,
                           zorder=zorder)
    r_corner_a = Rectangle((80, 3), 14, 0, lw=lw, color=color,
                           zorder=zorder)
    r_corner_b = Rectangle((80, 47), 14, 0, lw=lw, color=color,
                           zorder=zorder)

    # Left and right 3-PT line arcs
    l_arc = Arc((5, 25), 47.5, 47.5, theta1=292, theta2=68, lw=lw,
                color=color, zorder=zorder)
    r_arc = Arc((89, 25), 47.5, 47.5, theta1=112, theta2=248, lw=lw,
                color=color, zorder=zorder)

    # half_court
    # ax.axvline(470)
    half_court = Rectangle((47, 0), 0, 50, lw=lw, color=color,
                           zorder=zorder)

    hc_big_circle = Circle((47, 25), radius=6, lw=lw, fill=False,
                           color=color, zorder=zorder)
    hc_sm_circle = Circle((47, 25), radius=2, lw=lw, fill=False,
                          color=color, zorder=zorder)

    court_elements = [l_hoop, l_backboard, l_outer_box, outer,
                      l_inner_box, l_free_throw, l_corner_a,
                      l_corner_b, l_arc, r_hoop, r_backboard,
                      r_outer_box, r_inner_box, r_free_throw,
                      r_corner_a, r_corner_b, r_arc, half_court,
                      hc_big_circle, hc_sm_circle]

    # Add the court elements onto the axes
    for element in court_elements:
        ax.add_patch(element)

    return ax


def draw_shot_chart_court(ax=None, color='black', lw=2, outer_lines=False):
    # If an axes object isn't provided to plot onto, just get current one
    if ax is None:
        ax = plt.gca()

    # Create the various parts of an NBA basketball court

    # Create the basketball hoop
    # Diameter of a hoop is 18" so it has a radius of 9", which is a value
    # 7.5 in our coordinate system
    hoop = Circle((0, 0), radius=7.5, linewidth=lw, color=color, fill=False)

    # Create backboard
    backboard = Rectangle((-30, -7.5), 60, -1, linewidth=lw, color=color)

    # The paint
    # Create the outer box 0f the paint, width=16ft, height=19ft
    outer_box = Rectangle((-80, -47.5), 160, 190, linewidth=lw, color=color,
                          fill=False)
    # Create the inner box of the paint, widt=12ft, height=19ft
    inner_box = Rectangle((-60, -47.5), 120, 190, linewidth=lw, color=color,
                          fill=False)

    # Create free throw top arc
    top_free_throw = Arc((0, 142.5), 120, 120, theta1=0, theta2=180,
                         linewidth=lw, color=color, fill=False)
    # Create free throw bottom arc
    bottom_free_throw = Arc((0, 142.5), 120, 120, theta1=180, theta2=0,
                            linewidth=lw, color=color, linestyle='dashed')
    # Restricted Zone, it is an arc with 4ft radius from center of the hoop
    restricted = Arc((0, 0), 80, 80, theta1=0, theta2=180, linewidth=lw,
                     color=color)

    # Three point line
    # Create the side 3pt lines, they are 14ft long before they begin to arc
    corner_three_a = Rectangle((-220, -47.5), 0, 140, linewidth=lw,
                               color=color)
    corner_three_b = Rectangle((220, -47.5), 0, 140, linewidth=lw, color=color)
    # 3pt arc - center of arc will be the hoop, arc is 23'9" away from hoop
    # I just played around with the theta values until they lined up with the
    # threes
    three_arc = Arc((0, 0), 475, 475, theta1=22, theta2=158, linewidth=lw,
                    color=color)

    # Center Court
    center_outer_arc = Arc((0, 422.5), 120, 120, theta1=180, theta2=0,
                           linewidth=lw, color=color)
    center_inner_arc = Arc((0, 422.5), 40, 40, theta1=180, theta2=0,
                           linewidth=lw, color=color)

    # List of the court elements to be plotted onto the axes
    court_elements = [hoop, backboard, outer_box, inner_box, top_free_throw,
                      bottom_free_throw, restricted, corner_three_a,
                      corner_three_b, three_arc, center_outer_arc,
                      center_inner_arc]

    if outer_lines:
        # Draw the half court line, baseline and side out bound lines
        outer_lines = Rectangle((-250, -47.5), 500, 470, linewidth=lw,
                                color=color, fill=False)
        court_elements.append(outer_lines)

    # Add the court elements onto the axes
    for element in court_elements:
        ax.add_patch(element)

    return ax


def draw_shot_chart_court_with_zones(ax=None, color='black', color_zone='dimgray', lw=3, zone_width=2, outer_lines=False):
    # If an axes object isn't provided to plot onto, just get current one
    alpha = .15
    alphaZone = 1
    if ax is None:
        ax = plt.gca()

    # Create the various parts of an NBA basketball court

    # Create the basketball hoop
    # Diameter of a hoop is 18" so it has a radius of 9", which is a value
    # 7.5 in our coordinate system
    # hoop = Circle((0, 0), radius=7.5, linewidth=lw, color=color, fill=False, alpha=alpha)

    # Create backboard
    # backboard = Rectangle((-30, -7.5), 60, -1, linewidth=lw, color=color, alpha=alpha)

    # The paint
    # Create the outer box 0f the paint, width=16ft, height=19ft
    outer_box = Rectangle((-80, -47.5), 160, 190, linewidth=lw, color=color,
                          fill=False, alpha=alpha)
    # Create the inner box of the paint, widt=12ft, height=19ft
    inner_box = Rectangle((-60, -47.5), 120, 190, linewidth=lw, color=color,
                          fill=False, alpha=alpha)

    # Create free throw top arc
    top_free_throw = Arc((0, 142.5), 120, 120, theta1=0, theta2=180,
                         linewidth=lw, color=color, fill=False, alpha=alpha)
    # Create free throw bottom arc
    bottom_free_throw = Arc((0, 142.5), 120, 120, theta1=180, theta2=0,
                            linewidth=lw, color=color, linestyle='dashed', alpha=alpha)
    # Restricted Zone, it is an arc with 4ft radius from center of the hoop
    restricted = Arc((0, 0), 80, 80, theta1=0, theta2=180, linewidth=1,
                     color=color, alpha=alpha)

    # Three point line
    # Create the side 3pt lines, they are 14ft long before they begin to arc
    corner_three_a = Rectangle((-220, -47.5), 0, 140, linewidth=lw, color=color, alpha=1)
    corner_three_b = Rectangle((220, -47.5), 0, 140, linewidth=lw, color=color, alpha=1)
    # 3pt arc - center of arc will be the hoop, arc is 23'9" away from hoop
    # I just played around with the theta values until they lined up with the
    # threes

    three_arc = Arc((0, 0), 475, 475, theta1=22, theta2=158, linewidth=lw,
                    color=color, alpha=1)

    # Center Court
    center_outer_arc = Arc((0, 422.5), 120, 120, theta1=180, theta2=0,
                           linewidth=lw, color=color, alpha=alpha)
    center_inner_arc = Arc((0, 422.5), 40, 40, theta1=180, theta2=0,
                           linewidth=lw, color=color, alpha=alpha)

    # Draw Grid

    restrictedZone = Circle((0, 0), 40, linewidth=zone_width, color=color_zone, fill=False, alpha=alphaZone)
    restrictedZone3 = Circle((0, 0), 110, linewidth=zone_width, color=color_zone, fill=False, alpha=alphaZone)
    restrictedZone4 = Circle((0, 0), 180, linewidth=zone_width, color=color_zone, fill=False, alpha=alphaZone)

    # zone3 = Arc((0, 0), 475, 475, theta1=22, theta2=158, linewidth=zone_width, color=color_zone, alpha=alphaZone)
    zone3_p3 = Arc((0, 0), 560, 560, theta1=0, theta2=180, linewidth=zone_width, color=color_zone, alpha=alphaZone)
    courtSplitb = Rectangle((0, -47.5), 0, 7.5, linewidth=zone_width, color=color_zone, fill=False, alpha=alphaZone)
    courtSplitt = Rectangle((0, 40), 0, 430, linewidth=zone_width, color=color_zone, fill=False, alpha=alphaZone)

    # leftCorner = Rectangle((-220, -47.5), 0, 140, linewidth=zone_width, color=color_zone, alpha=alphaZone)
    # rightCorner = Rectangle((220, -47.5), 0, 140, linewidth=zone_width, color=color_zone, alpha=alphaZone)

    # Restricted Zone, it is an arc with 4ft radius from center of the hoop

    grid = [courtSplitb, courtSplitt, zone3_p3, restrictedZone, restrictedZone3,
            restrictedZone4]  # leftCorner, rightCorner,

    # List of the court elements to be plotted onto the axes
    court_elements = [outer_box, inner_box, top_free_throw,
                      bottom_free_throw, restricted, corner_three_a,
                      corner_three_b, three_arc, center_outer_arc,
                      center_inner_arc] + grid  # hoop, backboard,

    if outer_lines:
        # Draw the half court line, baseline and side out bound lines
        outer_lines = Rectangle((-250, -47.5), 500, 470, linewidth=lw,
                                color=color, fill=False)
        court_elements.append(outer_lines)

    # Add the court elements onto the axes
    for element in court_elements:
        ax.add_patch(element)

    angle1corner = mlines.Line2D(xdata=[220, 250], ydata=[92.5, 92.5], linewidth=zone_width, color=color_zone,
                                 alpha=alphaZone)
    angle1 = mlines.Line2D(xdata=[37, 220], ydata=[15, 92.5], linewidth=zone_width, color=color_zone, alpha=alphaZone)
    angle2corner = mlines.Line2D(xdata=[-220, -250], ydata=[92.5, 92.5], linewidth=zone_width, color=color_zone,
                                 alpha=alphaZone)
    angle2 = mlines.Line2D(xdata=[-37, -220], ydata=[15, 92.5], linewidth=zone_width, color=color_zone, alpha=alphaZone)

    ax.add_line(angle1)
    ax.add_line(angle1corner)
    ax.add_line(angle2)
    ax.add_line(angle2corner)

    return ax
