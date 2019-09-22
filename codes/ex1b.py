from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('Maximum_Amount_Paid')
sc = SparkContext(conf=conf)

# read dataset_tripdata,split it by ',' and keep only id and money paid
dataset = sc.textFile("hdfs://master:9000/user/user0023/inputs/yellow_tripdata_1m.csv",100).map(lambda line: line.split(',')).map(lambda line: (line[0],line[7]))

# read dataset_tripvendors,split it by ',' 
dataset2 = sc.textFile("hdfs://master:9000/user/user0023/inputs/yellow_tripvendors_1m.csv",100).map(lambda line: line.split(',')).map(lambda line: (line[0],line[1]))

# join datasets using key , keep only id and money paid , keep only max money paid for each key
dataset=dataset.join(dataset2).map(lambda line: (int(line[1][1]),float(line[1][0]))).reduceByKey(max)

# save as one file
dataset.coalesce(1,True).saveAsTextFile("hdfs://master:9000/user/user0023/out/ex1b.txt")

