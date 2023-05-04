from players import Player
import numpy as np
import matplotlib.pyplot as plt
from model import *


a_edwards = Player('Anthony Edwards','Minnesota')
min_df = a_edwards.player_points()
x_train,x_test,y_train,y_test = build_TrainTest(min_df,'PTS')
model,y_pred = RunLinearModel(x_train,y_train,x_test,y_test)
plt.scatter(y_pred ,y_test)
plt.xlabel('actual points')
plt.ylabel('predicted points')
plt.show()