from pyspark import SparkContext, SparkConf
import datetime

def time_to_sec(l1,l2):
        t1=datetime.datetime.strptime(l1, '%Y-%m-%d %H:%M:%S')
        t2=datetime.datetime.strptime(l2, '%Y-%m-%d %H:%M:%S')
        return (t2-t1).total_seconds()

def sec_to_min(t):
        t=float(t)
        return t/60

def hour_of_day(l):
        t=l.split(" ")[1]
        h,m,s=[int(i) for i in t.split(":")]
        return h

conf = SparkConf().setAppName('AvgDuration')
sc = SparkContext(conf=conf)

# read dataset,split it by ',' and keep only starttime and endtime
dataset = sc.textFile("hdfs://master:9000/user/user0023/inputs/yellow_tripdata_1m.csv",100).map(lambda line: line.split(',')).map(lambda line: (line[1],line[2]))

# compute duration in seconds and hour_of_day
dataset = dataset.map(lambda line: (hour_of_day(line[0]),(time_to_sec(line[0],line[1]))))

# transform seconds in minutes
dataset = dataset.map(lambda line: (line[0],sec_to_min(line[1])))
dataset = dataset.mapValues(lambda v: (v,1)).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).mapValues(lambda v: v[0]/v[1])
dataset = dataset.sortByKey()
copy = dataset
dataset = dataset.collect()
print(dataset)

copy.coalesce(1,True).saveAsTextFile("hdfs://master:9000/user/user0023/out/ex1a.txt")
