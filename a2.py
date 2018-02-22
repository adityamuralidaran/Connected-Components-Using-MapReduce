""" 
First Name: Aditya Subramanian
Last Name: Muralidaran
UBIT Name: adityasu
"""

from pyspark import SparkConf, SparkContext
import sys

def getLGraph(line):
	v = [ int(x) for x in line.split(" ") ]
 	return [(v[0], v[1]), (v[1], v[0])]

def getSGraph(LGraph):
	v = [int(x) for x in LGraph]
	if(v[1] <= v[0]):
		return (v[0],v[1])
	else:
		return (v[1],v[0])

def smallStarOp(keyval):
	if (int(keyval[1][0]) != int(keyval[1][1])):
		return (int(keyval[1][0]), int(keyval[1][1]))
	else:
		return (int(keyval[0]), int(keyval[1][0]))


if __name__ == "__main__":
 	conf = SparkConf().setAppName("ConnectedComponents")
 	sc = SparkContext(conf = conf)
	lines = sc.textFile(sys.argv[1])

	Neig = lines.flatMap(getLGraph)
	Neig.persist()

	# Large Star Operations
	while (True):
		m = Neig.reduceByKey(lambda a,b: min(a,b)).map(lambda v: (int(v[0]),min(int(v[0]),int(v[1]))))
		LGraphEdges = Neig.map(lambda e: e if int(e[0]) < int(e[1]) else None).filter(lambda x: x is not None)
		newEdges = LGraphEdges.join(m).map(lambda e: (e[1][0], e[1][1])).distinct()
		# Small star operation
		SstarGraph = newEdges.map(getSGraph)
		SstarGraph.persist()
		m1 = SstarGraph.reduceByKey(lambda a,b: min(a,b)).map(lambda v: (int(v[0]),min(int(v[0]),int(v[1]))))
		SnewEdges = SstarGraph.join(m1).map(smallStarOp).distinct()
		SnewEdgeslist = SstarGraph.flatMap(lambda e: [(int(e[0]),int(e[1])), (int(e[1]),int(e[0]))])
		if(SnewEdgeslist.subtract(Neig).isEmpty() and Neig.subtract(SnewEdgeslist).isEmpty()):
			break
		else:
			Neig = SnewEdges.flatMap(lambda e: [(int(e[0]),int(e[1])), (int(e[1]),int(e[0]))])
			Neig.persist()

	SstarGraph = SnewEdges.map(getSGraph)
	SstarGraph.persist()

	# Each node of a connected component is labeled using the root of its connected component.
	vertexLabel = SstarGraph.flatMap(lambda v: [str(v[0]) + " " + str(v[1]), str(v[1]) + " " + str(v[1])]).distinct()
	vertexLabel.saveAsTextFile(sys.argv[2])
	
	sc.stop()