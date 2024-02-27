
#################################################################################
# Name: infosys_dynamic_cube.py
# Purpose: This Script will a 3D plot of NxNxN cobe, N is provided as an input value
#
# Arguments:
#       Input: N represents the no.of cubes to be present in each dimension
# Output: 3-D NxNxN plotted cube
# Last Modified By: Infosys Team
# Last Modified Date: Feb 25, 2024
# Version: 1.0
# Version Comment: Final version
#################################################################################

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np

def plot_dynamic_cube(N):
   

    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    # Create grid representing cube positions
    x, y, z = np.mgrid[0:N, 0:N, 0:N]

    # Create a boolean array representing filled voxels
    filled = np.ones_like(x, dtype=bool)

    # Plot smaller cubes
    ax.voxels(filled, color='white', edgecolor='k', linewidth=1.5, alpha=0.6)

    # Cube Outline
    max_val = N
    for axis in 'xyz':
        getattr(ax, f'set_{axis}lim')([0, max_val])
        getattr(ax, f'set_{axis}ticks')([0, max_val])

    # Labels
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('Z')

    # Angles and distance
    ax.view_init(elev=30, azim=45)
    ax.dist = 15

    print(f"Input N = {N}")
    print(f"Output:")
    print(f"Here's a {N} x {N} x {N} cube plot with {N**3} cubes inside")

    plt.title(f"{N} x {N} x {N} Cube")
    plt.show()


try:

    N= int(input("Provide N value for NxNxN cube:"))
    if not isinstance(N, int):
        raise TypeError("N must be an integer")
    if N <= 0:
        raise ValueError("N must be a positive integer")    
    plot_dynamic_cube(int(N))
except (TypeError, ValueError) as e:
        print("N must be a positive integer")


