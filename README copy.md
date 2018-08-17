##README

test
##Source File
Source Code is in **Subgraph** folder, which can be open by IDEA.

In it you can find three packages:

- sAlg
    - Square
    - Chordal Square
    - House 
        - HouseS: for small data graph
        - House:for large data graph
    - Chordal Roof
    - Three Triangle
    - Solar Square
    - Near5Clique
    - QuadTriangle
    - SolarSquarePlus
- sData: data structure used in sAlg
- sPreprocess: Preprocessing Related
    - sCliqueGeneration
        - classes used to generate the crystal
    - sTool
        - preprocessing the graph

Each of the patterns comes with two java class, one for counting, one for outputting(class with name ended with O). More detail please read the code.

##Test File
Files related to testing is at Top level.

- **Data**: small datasets for testing the algorithm, in which each dataset is attached with a *-pattern-size.txt recording the pattern's size in each graph for each pattern.
- Preprocess.sh: Preprocessing the Data for CountingTest.sh and WritingTest.sh.
- CountingTest.sh: Testing the code related to counting in **Crystal**
- WritingTest.sh: Testing the code related to writing in **Crystal**
- pattern.eps: give a visulization about what pattern looks like

Please run Preprocess.sh first, before running the other two shell script. More detail please read the .sh file, and change some of the variables accordingly in the script file.

##Parameter 
- mapreduce.job.reduces: better using the same number as vCore in yarn
- mapreduce.reduce.memory.mb: the default is 4000MB, if graph bigger than UK2002 need to be processed, this might need to increase.
- mapreduce.map.memory.mb: the default is 4000MB, if graph bigger than UK2002 need to be processed, this might need to increase.
- test.memory: same as mapreduce.map.memory.mb. But only available in sAlg.
- bloomHash: controlling hash function used in Bloom filter
- bloomBit: controlling numbers of bit for representing a record in Bloom filter
- test.p
    - test.p (for classes in sIntersection): controlling the number of the round for the algorithm, the lower the better (if memory permit)
    - test.p (for classes in sPartition)L controlling the number of partitions for the algorithm, the lower the better (if memory permit)
- test.isOutput: specify whether the result should be outputted in *O.class.
- test.isEnumerating: whether just counting or enumerating.

##Format
- InputFormat: Edge List
- OutputFormat: file format specified in the paper. 

#

    
    //Below three configuration is commented for maximum compatibility for the different version of Hadoop, if possible, they should be uncommented to improve the performance and compression ratio.
    
    //These configurations can be found in the source code.
    
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
    
    FileOutputFormat.setCompressOutput(job, true);
    
    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    
    


###Core-Crystal Decomposition


The pattern decomposition aims at optimzing Objective 3 in the paper. 

This package provides decomposition.cpp which decomposes the pattern graph with/without the statistics of the cliques. 

input specification 
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
		the total number of Clique_i in the target graph. Specifically, the first line is the # of nodes in the target graph and the second line is the # of edges in the target 

Example input 1

4 4 0 
1 2
1 3
2 4
3 

Example input 2
4 4 1
1 2
1 3
2 4
3 4

2 20
100
200

Please refer to patternpool.txt for further example


### if you have found any bug or something you don't understand feel free to contact me at [hzhang@se.cuhk.edu.hk](hzhang@se.cuhk.edu.hk)
