import sys
from operator import add
from pyspark import SparkContext

#print('--> length of arguments : ',len(sys.argv))
if len(sys.argv) != 2:
    print >> sys.stderr, "Usage: wordcount <input_file> <output_file>"
    exit(-1)
sc = SparkContext(appName="PythonFrequentWordCount")
lines = sc.textFile(sys.argv[1])
counts = lines.flatMap(lambda line: line.strip().lower().split(' ')).filter(lambda word : ''.join(e for e in word.lower() if e.isalnum())).map(lambda word: (word, 1)).reduceByKey(add)
#counts.saveAsTextFile(sys.argv[2])
output = counts.map(lambda (k,v): (v,k)).sortByKey(False).take(1)
for (count, word) in output:
	print "%i: %s" % (count, word)
sc.stop()
