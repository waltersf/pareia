#include <iostream>
#include <fstream>
#include <map>
#include <cstring>
#include "fasttrie.h"
using namespace std;

void usingTrie(){
	jdk_fasttree<int, 255, char> trie;

	fstream file1;
	file1.open("dados.txt", fstream::in);
	char buffer[4096];
	char buffer2[4096];
	while(! file1.eof()){
		file1.getline(buffer, 4096);
		if (strlen(buffer) == 0){
			break;
		}
		for (int i = 0; i < 100; i ++){
			sprintf(buffer2, "%s", buffer);
			trie.add(buffer2, strlen(buffer), 1);
		}
	}
	file1.close();
	cout << "Pressione enter " << endl;
	int a;
	cin >> a;
}
void usingMap(){
	map<char*, int> map;

	fstream file1;
	file1.open("dados.txt", fstream::in);
	char buffer[4096];
	char *buffer2;
	while(! file1.eof()){
		file1.getline(buffer, 4096);
		if (strlen(buffer) == 0){
			break;
		}
		for (int i = 0; i < 100; i ++){
			buffer2 = new char[strlen(buffer) + 1];
			sprintf(buffer2, "%s", buffer);
			map[buffer2] = 1;
		}
	}
	file1.close();
	cout << "Pressione enter " << map.size() << endl;
	int a;
	cin >> a;
}
int main(int argc, char **argv) {

	jdk_fasttree<char *, 256, char> trie;
	char * a = "alexandre19691209";
	char * b = "alexandre19681209";
	trie.add(a, strlen(a), a);

	cout << (NULL == trie.find(a, strlen(a))) << endl;
	cout << (NULL == trie.find(b, strlen(b))) << endl << flush;

	if (argc > 1){
		cout << "Trie" << endl;
		usingTrie();
	} else {
		cout << "Map" << endl;
		usingMap();
	}
	return 0;
}
