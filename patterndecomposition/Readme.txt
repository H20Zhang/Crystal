The pattern decomposition aims at optimzing Objective 3 in the paper. 

This package provides decomposition.cpp which decomposes the pattern graph with/without the statistics of the cliques. 

************************************** input specification ****************************************
Input the pattern graph to pattern.txt.  
	first line : 
		n m b
			n: # of nodes, m: # of edges. Note that nodes are labeled with [1,n]. 
			b: a boolean value of 0 or 1, specifying that statistics are used (1) or not (0)
	followed by m lines, each line 
		x y 
			describes an undirected edge (x,y)

	followed by a line, if b=1,  
		k M
			k: the largest clique with statistics, k should be at least = the largest clique size in the pattern graph
			M: memory size
	followed by k lines, the i-th line specifies 
		the total number of Clique_i in the target graph. Specifically, the first line is the # of nodes in the target graph and the second line is the # of edges in the target graph.

*********************Example input 1***********************
4 4 0
1 2
1 3
2 4
3 4

*********************Example input 2***********************
4 4 1
1 2
1 3
2 4
3 4

2 20
100
200

************Please refer to patternpool.txt for further examples***********