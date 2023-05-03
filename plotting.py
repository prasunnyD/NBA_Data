from players import Player
import numpy as np
import matplotlib.pyplot as plt


a_edwards = Player('Anthony Edwards','Minnesota')
edwards_boxscore = a_edwards.player_career_boxscore()
edwards_boxscore.plot.scatter(x='PTS',y='FG_PCT')
plt.show()