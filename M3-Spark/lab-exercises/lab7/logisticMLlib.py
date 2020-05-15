import sys
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint

def parseLine(line):
	"""
	parses each line as spark mllib LabeledPoint object
	"""
	values = [float(val) for val in line.split(' ')]
	return LabeledPoint(values[0], values[1:])

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr,"usage command : file <ip_file> <iteration>"
        exit(-1)

    sc = SparkContext(appName="logistic_lab_7")
    iteration = int(sys.argv[2])
    rdd = sc.textFile(sys.argv[1])
    rdd1 = rdd.map(parseLine)
    
    for m in range(iteration):
     	model = LogisticRegressionWithSGD.train(rdd1,iteration)
    print("final weights :" + str(model.weights))
    sc.stop()
