from pyspark import SparkContext
import sys
if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: logmining <file> <outputStore>"
        exit(-1)
sc = SparkContext(appName="logmining")
text_file = sc.textFile(sys.argv[1])
errors = text_file.filter(lambda line: "error" in line)
errors.cache()
files = []
count = errors.count()
print "Total number of errors : ",count
files.append('Total errors : '+str(count))
browser_error = errors.filter(lambda line: "Mozilla" in line).count()
print "Mozilla error count : ",browser_error
files.append('Mozilla errors : '+str(browser_error))
browserCompatibilityError = errors.filter(lambda line: "compatible" in line).count()
print "Mozilla Compatibility Error count : ",browserCompatibilityError
files.append('Mozilla Compatibility Errors : '+str(browserCompatibilityError))
browserCompatibilityIphoneError = errors.filter(lambda line: "iPhone" in line).count()
print "Mozilla Compatibility Error that are iPhone related are : ",browserCompatibilityIphoneError
files.append('Mozilla Compatibility Errors for iPhone : '+str(browserCompatibilityIphoneError))
finalErrors = errors.filter(lambda line: 'iPhone' in line)
finalErrors.saveAsTextFile(sys.argv[2])
obj=open("errorcounts.txt",'w')
for item in files:
	obj.write("%s\n" % item)
sc.stop()
