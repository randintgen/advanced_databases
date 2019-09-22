# Advanced topics in databases
This project was developped as part of the 9th semester course, Advanced Topics in databases (of the National Technical University of Athens) taught by Ionnis Konstantinou (http://www.cslab.ntua.gr/~ikons/) in the winter semester of 2018. At this project I had the opportunity to implement algorithms in map reduce context using Apache Spark and Hadoop (HDFS) on a distributed cluster. 

## Exercise 1 - Data Analytics 
The first exercise was all about data analytics . We worked on the new york yellow cab dataset (https://data.cityofnewyork.us/Transportation/2015-Yellow-Taxi-Trip-Data) and we had to implement two different actions on this dataset . At first we had to calculate the average trip duration for each hour of day (/codes/ex1a.py). Then we calculated the maximum amount of money paid to each different taxi company for one single trip (/codes/ex2a.py) . At this step we actually found that the dataset had some wrong data as the cost was too big ( around 500000 $ for a single ride) . As a result , we cleared the dataset as part of preprocessing by removing rides that costed more than 5000 $ . 

## Exercise 2 - K-Means implementation
The second exercise was about machine learning on map reduce context . To be more precise we used the exercise's 1 dataset (nyc yellow cab) and we implemented the K-Means algorithm (https://en.wikipedia.org/wiki/K-means_clustering) using pyspark (/codes/kmeans.py) in order to find the k centers ((x,y) coordinates) from which the taxis started their routes . Such information could be really useful as taxi drivers can maximize their profits by going by these centers . Once again we encountered a problem with this dataset as there were missing values in the (x,y) coordinates . These values lead one of the centers to be located in the Null Island ((0,0) coordinates , https://en.wikipedia.org/wiki/Null_Island) . In order to fix this issue we preprocessed the dataset in a way that we cleared the entries with missing values .

## Exercise 3 - PageRank algorithm 
The third exercise was about implementing the PageRank algorithm (https://en.wikipedia.org/wiki/PageRank) using pyspark (/codes/pagerank.py). At this exercise we used as our input data a subtotal of the actual Google Web Graph which can be found here --> https://snap.stanford.edu/data/web-Google.html . This exercise actually was really tempting as it was the first "difficult" problem I faced and SOLVED.

## Exercise 4  - Matrix-Matrix Multiplication (MMM kernel)
The fourth (and last) exercise was about the famous Matrix-Matrix multiplication kernel , extracted from linear algebra . This kernel is highly parallel as each element of the product matrix can be calculated independently . This exercise was an actual nightmare as it topped our ram usage due to the fact that every line of the first matrix and every row of the second matrix has to sent to the same reducer.An example of calculating one element of the product matrix can be seen below :

<img src="https://github.com/filmnoirprod/advanced_databases/blob/master/mmm.png" width="600" height="200">
