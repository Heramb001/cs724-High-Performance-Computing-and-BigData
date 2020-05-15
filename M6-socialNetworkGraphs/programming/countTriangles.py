from pyspark import SparkContext
import sys

#opt = []

def mapfunc(line):
	opt = []
	adj = str(line).split()
	vertex = int(adj[0])
	neighbours = adj[1].split(',')
	neighbours = [int(i) for i in neighbours]
	# indicate edge between vertices
	for i in range(len(neighbours)):
		key = (vertex, neighbours[i])
		value = (key, [-1])
		opt.append(value)
	# vertex is connected to both vertices in the pair
	for i in range(len(neighbours)-1):
		for j in range((i+1),len(neighbours)):
			key = (neighbours[i],neighbours[j])
			value = (key,[vertex])
			opt.append(value) 
	return opt

def reducefunc(value):
	opt=[]
	vertices = value[0]
	neighbours = list(set(value[1]))
	v1 = vertices[0]
	v2 = vertices[1]
	if -1 in neighbours:
		neighbours.remove(-1)
		for i in range(len(neighbours)):
			if (neighbours[i]<v1 and neighbours[i]<v2):
				opt.append((neighbours[i],v1,v2))
	return opt

if __name__ == "__main__":
    if len(sys.argv)!=3:
        print >> sys.stderr, "COMMAND: pyfile <input> <output>"
        exit(-1)
    sc=SparkContext(appName="count-triangle")
    rdd1=sc.textFile(sys.argv[1])
    print('Read text File : success')
    op=rdd1.flatMap(mapfunc).reduceByKey(lambda x,y:x+y).flatMap(reducefunc)
    print('Calculated number of triangles from the given adjacency list : success')
    print(op.collect())
    op.saveAsTextFile(sys.argv[2])
    print('Saved triangles to text file : success ')
	sc.stop()
