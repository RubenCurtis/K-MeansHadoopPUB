import sys
import matplotlib.pyplot as plt

def Plot(file):
    try:
        with open(file, "r") as f:
            data = f.read().splitlines()
    except FileNotFoundError:
        print("File does not exist or cannot be found, check driver configuration.")
    
    x_axis = []
    y_axis = []
    
    for line in data:
        items = line.strip().split(",")
        if len(items)>= 10:
            try:
                x = float(items[7])#Median Income
                y = float(items[9])#Median House Value
                x_axis.append(x)
                y_axis.append(y)
            except ValueError:
                    print("Error in line, ignoring values in line: " + line)
    
    plt.scatter(x_axis, y_axis)
    plt.xlabel("Median income difference")
    plt.ylabel("Median house value difference")
    plt.title("K-Means, a visualisation")
    
    plt.savefig("KMeans.png")
    
    plt.show()

file = "/home/user/git/PROJECT/Code/kmeans/src/main/resources/Output/final_centroids"
Plot(file)
        