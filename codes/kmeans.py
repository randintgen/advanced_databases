from pyspark import SparkContext, SparkConf
import datetime

def get_labels(points,centers):
        index = 0
        mindst = float("+inf")
        tempdst = 0
        for i in range(len(centers)):
                tempdst = ((points[0]-centers[i][0])**2 + (points[1]-centers[i][1])**2)
                if(mindst>tempdst):
                        mindst = tempdst
                        index = i

        return index

conf = SparkConf().setAppName('kMeans')
sc = SparkContext(conf=conf)

# read dataset,split it by ',' keep only x and y of startpoint , then filter for zero coordinates
dataset = sc.textFile("hdfs://master:9000/user/user0023/inputs/yellow_tripdata_1m.csv",100).map(lambda line: line.split(',')).map(lambda line: (float(line[3]),float(line[4]))).filter(lambda line: line[0]!=0 and line[1]!=0)

max_iterations = 3
iterations = 0
# first centroid are the first five elements of the dataset
centroids = dataset.take(5)

while iterations < max_iterations:
        iterations = iterations+1
        points_and_labels = dataset.map(lambda p: (get_labels(p,centroids),(p,1)))
        points_and_labels = points_and_labels.reduceByKey(lambda a,b: ((a[0][0]+b[0][0],a[0][1]+b[0][1]),a[1]+b[1]))
        centroids = points_and_labels.mapValues(lambda v: (v[0][0]/v[1],v[0][1]/v[1])).collectAsMap()

print "Id Centroid"
print " "
for i in range(len(centroids)):
        print i,centroids[i]

