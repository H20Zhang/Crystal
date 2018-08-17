#include <set>
#include <map>
#include <list>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <limits>

using namespace std;
const int MAX = 10000;

//all graph edges and nodes are counted from 1.

//all other arrays are counted from 0.


struct EdgeType{
	int x;
	int y;
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
		printf("(%d %d)\n", x, y);
	}
};

int n; 

struct AutoType{
	int n;
	std::map<int,int> nodemap;
	AutoType(int thisn, std::map<int,int> &gen){
		n = thisn;
		nodemap = gen;
	}

	void output(){
		for (int i = 0; i < n; i++) cout << nodemap[i] << " "; 
		cout << endl; 	
	}
};



inline int hashEdge(int a, int b, int n) {return (a * (n + 1) + b);}

class GraphType{
public:
	int n, m;
	EdgeType* edge;
	std::set<int> edgeset; 
	int* nodeid; // nodeid[i] denote the starting index of node i in the sorted edge list.

	bool isAutomorphic(std::map<int, int> &nodemap){
		for (int i = 0; i < m; i++){
			if (edgeset.count(hashEdge(nodemap[edge[i].x], nodemap[edge[i].y], n)) == 0) return false; 
		}
		return true; 
	}

	void removes(){
		delete[] edge;
		delete[] nodeid; 
	}

	void outputGraph(){
		cout << "*******Graph Info*******" << endl; 
		cout << n << " " << m << endl; 

		for (int i = 1; i <= m; i++){
	    	cout << (edge[i].x) << " " << (edge[i].y)  << endl;	
	    }

	    for (int i = 1; i <= n; i++){
	    	cout << nodeid[i] << endl;	
	    }
	}
	
	void init(){ //assume all edges [0..m-1] are sorted, construct auxiliate structures
		//pivot edges
    	edge[m].assign(MAX, MAX);  

    	//index the edges
		nodeid = new int[n + 2];
		//pivot node
		 
		for (int i = 1; i <n; i++) nodeid[i]=-1;
		nodeid[edge[0].x] = 0;
    	for (int i = 1; i < m; i++){
    		if (edge[i].x != edge[i - 1].x) nodeid[edge[i].x] = i;
    	}
    	
    	nodeid[n] = m;
    	for (int i = n; i>0; i--) if (nodeid[i - 1] == -1) nodeid[i - 1] = nodeid[i];
    	
    	for (int i = 0; i < m; i++){
			edgeset.insert(hashEdge(edge[i].x, edge[i].y, n));
		}
		
	}
};

std::list<EdgeType> partialorders;
std::list<AutoType> autoset; 

GraphType g; 


void input(){
	ifstream patterndoc;
    patterndoc.open("pattern.txt");
    // node counted from 1, edge counted from 1
   	int temp;
    patterndoc >> g.n >> g.m >> temp;
    g.edge = new EdgeType[g.m * 2 + 2];
    
    
    for (int i = 0; i < g.m; i++){
    	patterndoc >> (g.edge[i].x) >> (g.edge[i].y);	
    	g.edge[i + g.m].x = g.edge[i].y;
    	g.edge[i + g.m].y = g.edge[i].x;
    }

    g.m = g.m * 2; 
    sort(g.edge, g.edge + g.m);
    g.init();
}



void enumerateauto(int c, int n, std::map<int,int> &curaut){
	if (c == n){
		if (g.isAutomorphic(curaut)){
			AutoType aut(n, curaut);
			autoset.push_back(aut);
		}
		return;
	}
	for (int i = 0; i < n; i++){
		if (curaut.count(i) == 0){
			curaut[i] = c; 
			enumerateauto(c + 1, n, curaut);
			curaut.erase(i);
			// printf("%d ", curaut.count(i));

		}
	}
}

int main () {	
	input();
	std::map<int,int> curaut;
	enumerateauto(0, g.n, curaut);
	// for (std::list<AutoType>::iterator it=autoset.begin(); it != autoset.end(); ++it) {
	// 	(*it).output();	
	// }
	for (int i = 0; i < g.n; i ++){
		std::list<AutoType> autosetcopy;
		bool needed = false;  
		std::set<int> equclass; 
		equclass.insert(i);
		for (std::list<AutoType>::iterator it=autoset.begin(); it != autoset.end(); ++it) {
			if (equclass.count((*it).nodemap[i]) == 0) {
				equclass.insert((*it).nodemap[i]);
				EdgeType porder; 
				porder.assign(i, (*it).nodemap[i]); 
				partialorders.push_back(porder);
				needed = true; 
			}
			autosetcopy.push_back(*it);
		}
		if (!needed) continue; 
		autoset.clear();
		for (std::list<AutoType>::iterator it = autosetcopy.begin(); it != autosetcopy.end(); ++it) {
			bool yes = true; 
			std::map<int, int> &tauto = (it->nodemap); 
			for (std::list<EdgeType>::iterator pit = partialorders.begin(); pit != partialorders.end(); ++pit){
				EdgeType &e = *pit; 
				if (tauto[e.x] >= tauto[e.y]){
					yes = false; 
					break; 
				} 
			}
			if (yes) autoset.push_back(*it);
		}
	}

	for (std::list<EdgeType>::iterator pit = partialorders.begin(); pit != partialorders.end(); ++pit){
		pit->output();
	}
}
