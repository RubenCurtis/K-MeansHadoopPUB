import matplotlib.pyplot as plt
from collections import defaultdict

#This program should not be run individually, it will be called by its required programs
#It is assumed that the data required is in .txt format in the same folder as this program

#filelocation works but optimisation required

filelocation = "/home/user/git/PROJECT/Code/FrequencyDistribution/src/main/python/plot.txt"

#Open the file
with open(filelocation, "r") as file:
    lines = file.readlines()

x_axis = []
y_axis = []

uniquex = defaultdict(float)

#Iterate through the lines to create the x and y axis
#x = key
#y = value
for line in lines:
    items = line.strip().split("\t")
    x = items[0]
    y = items[1]
    uniquex[x] += float(y)

x_axis = list(set(uniquex.keys()))
y_axis = [uniquex[x] for x in x_axis]

#Create and show the graph using the data
plt.bar(x_axis, y_axis, label="Count")

plt.title("Frequency Distribution")
plt.xlabel("Value")
plt.ylabel("Count")
plt.xticks(fontsize=4)

plt.legend()

plt.savefig("plot.png")

plt.show()

#could probably do with a if __name__ == "__main__" here...
