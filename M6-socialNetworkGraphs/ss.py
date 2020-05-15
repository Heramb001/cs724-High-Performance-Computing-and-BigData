from pyspark import SparkContext
import sys

oput=[]

def mapper(line):   
        parts=str(line).split()
        vertex=int(parts[0])
        neighbors=parts[1].split(',')
        neighbors=[int(i) for i in neighbors]
        nrange=len(neighbors)
        for i in range(len(neighbors)):
                key=(vertex,neighbors[i])
                val=(key,[-1])
                oput.append(val)
        for i in range(nrange-1):
                for j in range(i+1, len(neighbors)):
                        key=(neighbors[i],neighbors[j])
                        val=(key,[vertex])
                        oput.append(val)
        
        return oput

def reducer(value):
        numTrgls = 0 
        vertices=value[0]
        neighbors=value[1]
        v1=vertices[0]
        v2=vertices[1]
        nrange=len(neighbors)
        if -1 in neighbors:
                for i in range(nrange):
                        if (neighbors[i]<v1 and neighbors[i]<v2):
                                oput.append((neighbors[i],v1,v2))
        numTrgls = len(oput)
        return oput


if __name__ == "__main__":
        if len(sys.argv)!=3:
                print >> sys.stderr, "COMMAND: pyfile <input> <input>"
                exit(-1)
        sc=SparkContext(appName="triangle")
        rdd1=sc.textFile(sys.argv[1])
        mapopt=rdd1.flatMap(mapper)
        reduceropt = mapopt.reduceByKey(lambda x,y:x+y).sortByKey(False).flatMap(reducer)
        reduceropt.saveAsTextFile(sys.argv[2])
        sc.stop()