from pyspark import SparkContext, SparkConf
from operator import itemgetter

def sorts(list1):
	line = list(list1[1])
	line.sort(key=itemgetter(0))
	return (list1[0],line)

def compute(list1,list2):
        i=0
        for j in range(0,len(list1)):
                for k in range(0,len(list1[j])):
                        if k==1:
                                i+=(list1[j][1])*(list2[j][1])
        return i

conf = SparkConf().setAppName('FourthExercise')
sc = SparkContext(conf=conf)

# read matrices A,B
arraya = sc.textFile("hdfs://master:9000/user/user0023/inputs/A.csv",10).map(lambda line: line.split(',')).map(lambda line: (int(line[0]),[(int(line[1]),int(line[2]))]))
arrayb = sc.textFile("hdfs://master:9000/user/user0023/inputs/B.csv",10).map(lambda line: line.split(',')).map(lambda line: (int(line[1]),[(int(line[0]),int(line[2]))]))

arraya=arraya.reduceByKey(lambda a,b:a+b)
arrayb=arrayb.reduceByKey(lambda a,b:a+b)

arraya = arraya.map(lambda line: sorts(line))
arrayb = arrayb.map(lambda line: sorts(line))

res = arraya.cartesian(arrayb)
res = res.map(lambda x: ((x[0][0],x[1][0]),compute(x[0][1],x[1][1])))

# save as textfile in hdfs
res.coalesce(1,True).saveAsTextFile("hdfs://master:9000/user/user0023/ex4.txt")             
