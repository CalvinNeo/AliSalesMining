# coding: utf8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark import SparkConf

sc = SparkContext("local[4]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("127.0.0.1", 9999)

words = lines.flatMap(lambda line: line.split(","))

pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate