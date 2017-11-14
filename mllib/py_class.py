#coding: utf8

import pyspark
import pyspark.mllib as mllib
# from pyspark.mllib import regression, classification
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.classification import NaiveBayes
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.linalg import DenseVector, Vectors
from pyspark import SparkContext
from pyspark import SparkConf
import math

class LineFile(object):
    def __init__(self, fn):
        self.f = open(fn, "r")

    def __del__(self):
        self.f.close()

    def __iter__(self):
        return self

    def next(self):
        while True:
            l = self.f.readline()
            if not l:
                raise StopIteration()
            else:
                l = l.strip(" ").strip("\n")
                if l == "":
                    continue
                else:
                    return l

def split_dataset(dataset, split = [0.9, 0.0, 0.1]):
    return dataset.randomSplit(split)

def do_nb():
    sc = SparkContext("local[*]", "NB")
    fi = LineFile("./data.txt")
    rawdata = []
    for line in fi:
        item = map(lambda x: str(x), line.split(","))
        rawdata.append((int(item[0]), map(float, item[2:])))

    def make_labeled(record):
        return LabeledPoint(record[0], Vectors.dense(record[1]))

    dataset = sc.parallelize(rawdata).map(make_labeled)
    [trset, vlset, tsset] = split_dataset(dataset)

    model = NaiveBayes.train(trset, 1.0)

    predictionAndLabel = tsset.map(lambda p : (model.predict(p.features), p.label))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / tsset.count()

    print accuracy

    for x in predictionAndLabel.collect():
        print x

def do_lr():
    sc = SparkContext("local[*]", "LR")
    fi = LineFile("./data.txt")
    rawdata = []
    for line in fi:
        item = map(lambda x: str(x), line.split(","))
        rawdata.append((int(item[0]), map(float, item[2:])))

    def make_labeled(record):
        return LabeledPoint(record[0], Vectors.dense(record[1]))

    dataset = sc.parallelize(rawdata).map(make_labeled)
    [trset, vlset, tsset] = split_dataset(dataset)

    model = DecisionTree.trainClassifier(trset, numClasses=3, categoricalFeaturesInfo={}, impurity='gini', maxDepth=5, maxBins=32)

    labelsAndPoint = tsset.map(lambda lp:lp.label).zip(model.predict(tsset.map(lambda x:x.features)))

    accuracy = 1.0 * labelsAndPoint.filter(lambda (x, v): x == v).count() / tsset.count()

    print accuracy
    for x in labelsAndPoint.collect():
        print x

if __name__ == '__main__':
    do_lr()