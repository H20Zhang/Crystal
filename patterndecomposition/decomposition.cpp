#include <vector>
#include <set>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <limits>

using namespace std;
const int MAX = 10000;

// 0: if the nodes in the input pattern graph are counted from 0; 1: if that is counted from 1.
const int base = 0; 
int n; 

//to transform a graph's id to nodes [1..n]
//multi=1: input; multi = -1: output;
inline int inputTransform(int &x, int multi){ 
	if (multi == -1) return (n - (x-1)) % n + base; 
	x = (n - (x - base)) % n + 1;
	return 0; 
}

//in the following code
//all graph edges and nodes are counted from 1.
//all other arrays are counted from 0.


struct EdgeType{
	int x;
	int y;
	int id; 
	void assign(int a, int b){
		x = a; 
		y = b;
	}

	bool operator < (EdgeType & e){
		if (x < e.x) return true; 
		if (x == e.x && y < e.y) return true; 
		return false; 
	}

	void output(){
		printf("(%d %d)\n", inputTransform(x, -1), inputTransform(y, -1));
	}
};

inline int hashEdge(int a, int b, int n) {return (a * (n + 1) + b);}

struct CliqueType{
	int size; 
	int* nodes;
	std::set<int> nodemap;
	

	void buildmap(){
		for (int i = 0; i < size; i++) nodemap.insert(nodes[i]);
	}
	
	bool fullyconnect(int x, int n, std::set<int> & edgeset){
		//returns true if all nodes in this clique are connected to x. the underlying graph has n nodes and edges hashed in edgeset.
		for (int i = 0;i < size; i++){
			if (edgeset.count(hashEdge(nodes[i], x, n)) == 0) return false; 
		}
		return true; 
	}
	void output(){
		for (int i = 0; i < size; i++) cout << inputTransform(nodes[i], -1) << " "; 
		cout << endl; 	
	}
};


class GraphType{
public:
	int n, m;
	EdgeType* edge;
	std::set<int> edgeset; 
	int* nodeid; // nodeid[i] denote the starting index of node i in the sorted edge list.
	std::map<int,double> cost; // the total of the instances of this clique in the target graph.
	bool useStatistic;

	int mvc; //minimum # of nodes in a vertex cover;
	int mcc; //minimum # of connected components in over all minimum vertex cover
	int* vc; //the vertex cover of min vc;
	int* tvc; //the vertex cover of current vc;

	int* edgecover; // each edge has an id. edgecover[id] records how many nodes have covered the edge_id. 
	int uncoverededge;

	int* mvcvisit; // for connectivity check. records how many ccs the mvc has.
	int* stack; // for connectivity check.
	

	void removes(){
		delete[] vc;
		delete[] tvc;
		delete[] edgecover;
		delete[] edge;
		delete[] mvcvisit;
		delete[] stack; 
	}

	void outputGraph(){
		cout << "*******Graph Info*******" << endl; 
		cout << n << " " << m << endl; 

		for (int i = 1; i <= m; i++){
	    	cout << (edge[i].x) << " " << (edge[i].y) << " " << (edge[i].id) << endl;	
	    }

	    for (int i = 1; i <= n; i++){
	    	cout << nodeid[i] << endl;	
	    }
	}
	
	void init(){ //assume all edges [1..m] are sorted, construct auxiliate structures
		mvc = MAX;
		mcc = MAX;
		vc = new int[n + 2];
		tvc = new int[n + 2];
		mvcvisit = new int[n + 2];
		stack = new int[n + 2];

		//pivot edges
    	edge[0].assign(-1, -1);
    	edge[m + 1].assign(MAX, MAX);  

    	//index the edges
		nodeid = new int[n + 2];
		//pivot node
		nodeid[0] = 0;
		nodeid[n + 1] = m + 1; 
		for (int i = 1; i <=n; i++) nodeid[i]=-1;
    	
    	for (int i = 1; i <= m; i++){
    		if (edge[i].x != edge[i - 1].x) nodeid[edge[i].x] = i;
    	}
    	for (int i = n; i>0; i--) if (nodeid[i] == -1) nodeid[i] = nodeid[i + 1];

    	// identify undirected edges 
    	int distinctedge = 0;
    	for (int i = 1; i <= m; i++){
	    	if (edge[i].x < edge[i].y) edge[i].id = ++distinctedge;
	    	else { // this part is a raw implementation since we assume that the pattern has < 1000 edges. 
	    		for (int j = 1; j < i; j++) {
	    			if (edge[j].x == edge[i].y && edge[j].y == edge[i].x){
	    				edge[i].id = edge[j].id;
	    				break;
	    			}
	    		}
	    	}
    	}


    	for (int i = 1; i <= m; i++){
			edgeset.insert(hashEdge(edge[i].x, edge[i].y, n));
		}

		//undirected graph in the linked list presentation, edges are doubled.
		edgecover = new int[m + n + 1]; 
		for (int i = 0; i <= m / 2; i++) edgecover[i] = 0; 
		uncoverededge = m / 2;		
	}

	void findMinVertexCover(int curnode, int usednodes){
		if (uncoverededge == 0) {
			int cc = 0;
			if (usednodes <= mvc){ // condition 1 satisfied
				// compute the connectivity of the induced graph on tvc[0..usednodes-1] using a traversal.
				int stacktop = 0; 
				// if i is not in tvc, mvcvisit[i] = -1; 
				// if i is not visited tvc node, mvcvisit[i] = 0;
				// if i is a visited tvc node, mvcvisit[i] = 1;
				for (int i = 0; i <= n; i++) {mvcvisit[i] = -1;}
				for (int i = 0; i < usednodes; i++) mvcvisit[tvc[i]] = 0;
				for (int i = 0; i < usednodes; i++){
					if (mvcvisit[tvc[i]] == 0) { 
						cc++; //meet an unvisited tvc node, cc += 1, then traverse from tvc[i].
						stack[1] = tvc[i]; stacktop = 1; 
						mvcvisit[tvc[i]] = 1;
						while (stacktop > 0){
							int curnode = stack[stacktop];
							stacktop--;
							for (int j = nodeid[curnode]; j < nodeid[curnode + 1]; j++) {
								if (mvcvisit[edge[j].y] == 0) {
									mvcvisit[edge[j].y] = 1;
									stacktop++;
									stack[stacktop] = edge[j].y; 
								}
							}
						}
					}
				}
			}
			
			if (usednodes < mvc || (usednodes == mvc && cc < mcc)){
				mvc = usednodes;
				for (int i = 0; i < mvc; i++) {
					vc[i] = tvc[i];
				}
				mcc = cc; 
			}
			return; 
		}

		if (curnode > n) return; 
		if (usednodes > mvc - 1) return; //no need to further test
		
		// choose node curnode
		int uncoverededgeBak = uncoverededge; 
		for (int i = nodeid[curnode]; i < nodeid[curnode + 1]; i ++){		
			if (edgecover[edge[i].id] == 0)  uncoverededge--;
			edgecover[edge[i].id] ++;
		}
		
		// include curnode into the vertex cover	
		if (uncoverededge != uncoverededgeBak){
			tvc[usednodes] = curnode;
			findMinVertexCover(curnode + 1, usednodes + 1);
		}

		// restore everything
		for (int i = nodeid[curnode]; i < nodeid[curnode + 1]; i ++){
			edgecover[edge[i].id] --;
		}
		uncoverededge = uncoverededgeBak;
		
		// discard curnode	
		findMinVertexCover(curnode + 1, usednodes);
	}

	//********** prepare for bipartite graph ******************//
	std::vector<CliqueType> vccliques; //in bipartite graph, this vector record all cliques in the core
	int vccliquecount;
	int* tc; // the vertexs of a temporary clique;
	std::vector<EdgeType> vcedges; // the edges from vc nodes to the other nodes.
	int vcedgecount; 

	//bipartite graph; recursively find all cliques on the induced graph on nodes in vc[0..mvc-1]
	void findAllCliquesinVC(){
		tc = new int[n + 1];
		vccliquecount = 0;
		findCliquesRec(0, 0);
		for (int i = 0; i < vccliquecount; i++){
			vccliques[i].buildmap();
		}
		delete[] tc;
	}

	//bipartite graph; recursively find all cliques on the induced graph on nodes in vc[0..mvc-1]
	void findCliquesRec(int curnode, int usednodes){
		if (curnode == mvc) return; 

		//vc[curnode] must fully connect to the existing nodes in tc[0..usednodes-1] to form a clique. 
		bool fullyconnected = true; 
		for (int i = 0; i < usednodes; i++){
			if (edgeset.count(hashEdge(tc[i], vc[curnode], n)) == 0) {
				fullyconnected = false; 
				break;
			}
		}
		if (fullyconnected){
			tc[usednodes] = vc[curnode];
			CliqueType c; 
			c.size = usednodes + 1; 
			c.nodes = new int[c.size];
			for (int i = 0; i <= usednodes; i++){
				c.nodes[i] = tc[i];
			}
			vccliques.push_back(c);
			vccliquecount++;
			findCliquesRec(curnode + 1, usednodes + 1);
		}

		findCliquesRec(curnode + 1, usednodes);
	}

	void findAllEdgesnotinVC(){
		std::set<int> setvc; 
		vcedgecount = 0; 
		for (int i = 0; i < mvc; i++) setvc.insert(vc[i]);
		for (int i = 1; i <=m; i++){
			if ((setvc.count(edge[i].x) == 1) && (setvc.count(edge[i].y) == 0)){
				vcedges.push_back(edge[i]);
				vcedgecount++;
			}
		}
	}

	//build the bipartite graph with nodes of two sides.
	//one side: vccliques[0..vccliquecount-1] => id: 1..vccliquecount
	//one side: vcedges[0..vcedgecount-1] => id: vccliquecount + 1 .. vccliques + vcedgecount
	//a cliques and an edge is linked if the edge has one node in the clique and the other node fully connected to the clique
	
	void outputCore(){
		for (int i = 0; i < mvc; i++){
			cout << inputTransform(vc[i], -1) << " ";
		}
		cout << endl; 
	}

};


class BipartiteGraphType: public GraphType{
public:
	int bipartite; //only nodes [1..bipartite] are feasible for vc. 
	bool* vcfeasibility; // for bipartite graph, the vc nodes can only come from one side of the graph.
	double mincost; 
	double curcost;

	void generateBipartartGraph(GraphType &g){
		vccliquecount = g.vccliquecount;
		vcedgecount = g.vcedgecount;
		n = (vccliquecount + vcedgecount);
		m = 0; 
		vcfeasibility = new bool[n + 2];
		edge = new EdgeType[n * n + 2];
		useStatistic = g.useStatistic;
		if (useStatistic){
			for (int i = 1; i <= vccliquecount; i++) {
				cost[i] = g.cost[g.vccliques[i-1].size + 1];
			}
			mincost = std::numeric_limits<double>::max();
			curcost = 1;  
		}
		for (int i = 1; i <= n; i++)
			for (int j = 1; j <= n; j++){
				if (i <= vccliquecount && j <= vccliquecount) continue; 
				if (i > vccliquecount && j > vccliquecount) continue; 
				if ( i >= j) continue;
				CliqueType &curclique = g.vccliques[i - 1];
				EdgeType &curedge = g.vcedges[j - vccliquecount - 1];
				// printf("processing clique:\n");
				// curclique.output();
				// printf("processing edge:\n");
				// curedge.output();
				if (curclique.nodemap.count(curedge.x)>0 && (curclique.fullyconnect(curedge.y, g.n, g.edgeset))){
					m++;
					edge[m].assign(i,j);
					// printf("%d th edge: ", m); edge[m].output();
				}
				
			}
 
		// printf("output graph: \n");
		// outputGraph();
		
		init();
		// outputGraph();

		bipartite = vccliquecount;
		uncoverededge = vcedgecount; 
		for (int i = 1; i <= bipartite; i++) vcfeasibility[i] = true; 
		for (int i = bipartite + 1; i <= n; i++) vcfeasibility[i] = false; 
		for (int i = 0; i <= n; i++) edgecover[i] = 0; 
	}


	void removes(){
		delete[] vcfeasibility;
	}

	void findMinVertexCover(int curnode, int usednodes){
		if (uncoverededge == 0) {
			if (usednodes < mvc || (useStatistic && curcost < mincost)){
				mvc = usednodes;
				curcost = mincost;
				for (int i = 0; i < mvc; i++) {
					vc[i] = tvc[i];
				}
			}
			return; 
		}

		if (curnode > bipartite) return; 
		if (useStatistic) {
			if (curcost >= mincost) return; 
		} else if (usednodes > mvc - 2) return; //no need to further test
		
		// choose node curnode
		int uncoverededgeBak = uncoverededge; 
		double curcostBak = curcost;
		for (int i = nodeid[curnode]; i < nodeid[curnode + 1]; i ++){
			if (edgecover[edge[i].y] == 0)  uncoverededge--;
			edgecover[edge[i].y] ++;
		}
			
		// if curnode can cover an uncovered edge, try to put curnode into the cover. 
		if (uncoverededge != uncoverededgeBak){
			tvc[usednodes] = curnode;
			if (useStatistic) curcost *= cost[curnode];
			findMinVertexCover(curnode + 1, usednodes + 1);
		}

		//restore everything
		for (int i = nodeid[curnode]; i < nodeid[curnode + 1]; i ++){
			edgecover[edge[i].y] --;
		}
		uncoverededge = uncoverededgeBak;
		if (useStatistic) curcost = curcostBak;
		
		// try not to put curnode into the cover.
		findMinVertexCover(curnode + 1, usednodes);
	}

	void outputVC(GraphType &g){
		cout << "The core nodes are: "; 
		g.outputCore(); 
		
		cout << "We need " << mvc << " crystals." << endl;
		for (int i = 0; i < mvc; i++){
			cout << "The " << i + 1 << "th Crystal's Core:"; 
			CliqueType &curclique = g.vccliques[vc[i] - 1];
			curclique.output();
			// cout << nodeid[vc[i]] << " to " << nodeid[vc[i] + 1] << endl; 
			cout << "The " << i + 1 << "th Crystal's Budnodes:"; 
			std:set<int> budnodes; 
			for (int j = nodeid[vc[i]]; j < nodeid[vc[i] + 1]; j++) {
				int k = edge[j].y - bipartite - 1; // edge id in vcedges;
				int l = g.vcedges[k].y; //vcedges: edges from VC to nonVC
				if (budnodes.count(l) == 0) {
					budnodes.insert(l);
					cout << " " << inputTransform(l, -1); 
				}
			}
			cout << endl; 

			
		}
	}
};


void input(GraphType &g){
	ifstream patterndoc;
    patterndoc.open("pattern.txt");
    
    // node counted from 1, edge counted from 1
   
    patterndoc >> g.n >> g.m >> g.useStatistic;
    n = g.n; 
    g.edge = new EdgeType[g.m * 2 + 2];
    
    
    for (int i = 1; i <= g.m; i++){
    	patterndoc >> (g.edge[i].x) >> (g.edge[i].y);	
    	inputTransform(g.edge[i].x, 1);
    	inputTransform(g.edge[i].y, 1);
    	g.edge[i + g.m].x = g.edge[i].y;
    	g.edge[i + g.m].y = g.edge[i].x;
    }

    g.m = g.m * 2; 
    sort(g.edge + 1, g.edge + g.m + 1);
    g.init();
    if (g.useStatistic) {
    	int k, M;
    	patterndoc >> k >> M; 
    	for (int i = 1; i <= k; i++) {
    		int total;
    		patterndoc >> total; 
    		g.cost[i] = total * 1.0 / M;
    	}
    }
}



int main () {
	GraphType g; 
	input(g);
	g.findMinVertexCover(1, 0);
	g.findAllCliquesinVC();
	g.findAllEdgesnotinVC();

	//generate the bipartite graph
	BipartiteGraphType bg;
	bg.generateBipartartGraph(g);
	bg.findMinVertexCover(1, 0);
	bg.outputVC(g);
	bg.removes();
	g.removes();
    return 0;
}
