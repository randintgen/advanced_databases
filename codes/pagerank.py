from pyspark import SparkContext, SparkConf

def compute(outbound,rank):
        Lpj = len(outbound)
        for page in outbound:
                yield(page,rank/Lpj)

conf = SparkConf().setAppName('Pagerank')
sc = SparkContext(conf=conf)

# read dataset,split it by tab and cast to integers 
dataset = sc.textFile("hdfs://master:9000/user/user0023/inputs/web-Google.txt").filter(lambda line: not(line.startswith("#"))).map(lambda line: line.split()).map(lambda line: (int(line[0]),int(line[1])))

# edges is in form (pageid,list of outbound pageid's)
edges = dataset.groupByKey()

# rank is in form (pageid,rank)
rank = edges.map(lambda x: (x[0],0.5))

d = 0.85
max_iterations = 5
iterations = 0
N = rank.count()
#N = sc.broadcast(N1)
constant = ((1-d)/N)

while iterations < max_iterations:
        iterations = iterations + 1
        partial = edges.join(rank).flatMap(lambda x: compute(x[1][0],x[1][1]))
        rank = partial.reduceByKey(lambda a,b : a+b).mapValues(lambda x: constant+(x*d))

rank.coalesce(1,True).saveAsTextFile("hdfs://master:9000/user/user0023/out/ex3.txt")
print(rank.first())

