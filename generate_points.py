import matplotlib.pyplot as plt
# import seaborn as sns; sns.set()  # for plot styling
import numpy as np
from sklearn.datasets import make_blobs
from sklearn.cluster import KMeans
import sys, getopt
from datetime import datetime


def checkInput(argv):
  clusters = 7
  dimension = 3
  pointNumber = 1000
  
  try:
    opts, args = getopt.getopt(argv,"k:d:n:h",["clusters=","point_num=", "point_dimension="])
  except getopt.GetoptError:
      print ('test.py -i <inputfile> -o <outputfile>')
      sys.exit(2)
  
  for opt, arg in opts:
      if opt == '-h':
         print ('generate_point.py -k <clusters> -n <sample> -d <dimension>')
         print ('or')
         print ('generate_point.py --clusters <clusters> --point_num <sample> --point_dimension <dimension>')
         print ('k default is 7')
         print ('n default is 1000')
         print ('d default is 3')
         sys.exit()
      elif opt in ("-k", "--clusters"):
        clusters = int(arg)
      elif opt in ("-n", "--point_num"):
        pointNumber = int(arg)
      elif opt in ("-d", "--point_dimension"):
        dimension = int(arg)
  return (clusters, dimension, pointNumber)

# it generates points suitable for K-means clustering
# the returner dataset is separable
def generatePoints(clusters, dimension, pointNumber):
  X, y_true = make_blobs(n_samples=pointNumber, centers=clusters, cluster_std=STD, n_features=dimension)
  return X

def savePoints(fileName, points):
    with open(fileName, "w") as file: 
      for point in points:
        file.write(" ".join(str(round(i, 3)) for i in point))
        file.write('\n')

def plotPoints(points, centers):
  if len(points[0]) == 3:
    ax = plt.axes(projection='3d')
    ax.scatter3D(points[:, 0],points[:, 1],points[:, 2], cmap='viridis');
    ax.scatter3D(centers[:, 0],centers[:, 1],centers[:, 2], cmap='viridis');
    
  elif len(points[0]) == 2:
    plt.scatter(points[:, 0], points[:, 1], s=50);
  else:
    print('Point dimensions bigger than 3')
    return
  plt.show()

def generateRandomCluster(points):
  pass

# it performs the K-means alg provided by sklearn.cluster
def computeDesiredOutput(points, clusters):
  kmeans = KMeans(n_clusters=int(clusters))
  kmeans.fit(points)
  y_kmeans = kmeans.predict(points)
  centers = kmeans.cluster_centers_
  return centers

def generateTimestamp():
   # datetime object containing current date and time
  now = datetime.now()
  
  # dd/mm/YY H:M:S
  timestamp = now.strftime("%d/%m/%Y %H:%M:%S")
  return timestamp
if __name__ == "__main__":
  # generate timestamp to link different files
  timestamp = generateTimestamp()
  
  inputPath = './inputs/'
  desiredOutputPath = './des_output/'
  
  STD = 0.8 # standard deviation between points in a cluster

  clusters, dimension, pointNumber = checkInput(sys.argv[1:])

  InputFileName = 'input_d=%d_n=%d_k=%d.txt' % (dimension, pointNumber, clusters)
  ClusterFileName = 'cluster_d=%d_k=%d.txt' % (dimension, clusters)
  
  # dataset genaration
  points = generatePoints(clusters, dimension, pointNumber)
  
  #initialCluster = generateRandomCluster(points)

  # compute the desired output for a given dataset
  centers = computeDesiredOutput(points, clusters)

  savePoints(inputPath + timestamp + InputFileName, points)
  savePoints(desiredOutputPath + timestamp + ClusterFileName, centers)
  plotPoints(points, centers)



  
