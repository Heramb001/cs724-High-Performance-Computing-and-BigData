import re
import sys
from operator import add
from pyspark import SparkContext

# create contribution graph
def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

sc = SparkContext(appName="PythonPageRank")

if len(sys.argv) != 5:
        print >> sys.stderr, "Usage: pagerank.py <input_file 1> <input_file 2> <output_file> <number of iterations>"
        exit(-1)

lines = sc.textFile(sys.argv[1], 1) #--- Reading the graph
pages = sc.textFile(sys.argv[2], 1) #--- reading the urls data

print("--> Creating RDDS..")
links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
pagelinks = pages.map(lambda urls: parseNeighbors(urls)).cache()

#--- assigning initital Rank as 1 for each node.
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
print("\n\n***setting up initial rank to 1.0***")
# for (page, rank) in ranks.collect():
#     print("\n%s has initial rank: %s" % (page, rank))
temp_join = links.join(ranks)
contribs = temp_join.flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

print("\n***calculating contirbutions for each page from neightbours by rank/number of links going out of the page\n***")

temp_aggreg = contribs.reduceByKey(add)
print("\n*** adding all the contirbutions****\n")

print("\n*** recalculating ranks using damping factor and guessing iniatial PR as 0****\n")
temp_ranks = temp_aggreg.mapValues(lambda rank: rank * 0.85 + 0.15)

#---- reverse the mapping to find max and minimum --- (x,y) ---> (y,x)
rnksPg = temp_ranks.map(lambda x:(x[1],x[0]))
#--- get the maximum and minimum values of each iteration
min_rank = rnksPg.min()
max_rank = rnksPg.max()
print('--> initial Lowest rank is for %s with rank value : %s , the URL is : %s'%(min_rank[1],min_rank[0],pagelinks.lookup(min_rank[1])))
print('--> initial Highest rank is for %s with rank value : %s , the URL is : %s'%(max_rank[1],max_rank[0],pagelinks.lookup(max_rank[1])))


#--- Now calculating the pagerank for given number of iterations
for iteration in range(int(sys.argv[4])):
    print ("\n*** Iteration %d***" %iteration)
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    #---- reverse the mapping to find max and minimum --- (x,y) ---> (y,x)
    ranksRev = ranks.map(lambda x:(x[1],x[0]))
    #--- get the maximum and minimum values of each iteration
    min_rank = ranksRev.min()
    max_rank = ranksRev.max()
    print('--> Lowest rank is for %s with rank value : %s , the URL is : %s'%(min_rank[1],min_rank[0],pagelinks.lookup(min_rank[1])))
    print('--> Highest rank is for %s with rank value : %s , the URL is : %s'%(max_rank[1],max_rank[0],pagelinks.lookup(max_rank[1])))

ranks.repartition(1).saveAsTextFile(sys.argv[3])
sc.stop()
