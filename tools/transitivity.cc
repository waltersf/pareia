#include <map>
#include <vector>
#include <zlib.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include "../ferapardaexception.h"
using namespace Feraparda;
using namespace std;
void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./transitivity -f <result_file> -s <minumum_score>" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-f <result_file>: path of Pareia's result file, gzip'ed (required)." << endl;
	cerr << "\t-s <minumum_score>: minimum score to consider. Pairs which score is below this value will be ignored." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
	cerr << "\tIf more than 1 file will be used, specify the parameters -f and -s multiple times:" << endl;
	cerr << "\t./transitivity -f file1 -s score1 -f file2 -s score2 ..." << endl;
}
void applyTransitivity(vector<string> inputFiles, vector<float> scores) {

	for (unsigned int i = 0; i < inputFiles.size(); i++) {
		//cout << "Reading " << inputFiles[i] << endl;

		int fd1 = open64(inputFiles[i].c_str(), O_RDONLY, 0666);
		if (fd1 < 0){
			throw FerapardaException(string("Error opening file (") +
					inputFiles[i] +
					string(") for reading candidate pairs: ") + string(strerror(errno)));
		}

		gzFile inFile = gzdopen(fd1, "r");
		char buf[4096];
		map<int, int> graph;

		for (;;) {
			if (!gzgets(inFile, buf, sizeof(buf))) {
				int err;
				gzerror(inFile, &err);
				if (err > 1) {
					cerr << gzerror(inFile, &err) << endl;
					exit(err);
				} else {
					break;
				}
			} else {
				char result;
				float weight;
				int id1, id2, rank;
				sscanf(buf, "%c %d %d %d %f", &result, &rank, &id1, &id2,
						&weight);
				if (weight >= scores[i]) {
					if (id1 > id2) {
						int aux = id1;
						id1 = id2;
						id2 = aux;
					}
					if (graph.find(id1) != graph.end()) {
						graph[id2] = graph[id1];
					} else {
						graph[id1] = id1;
						graph[id2] = id1;
					}
				}
			}
		}
		map<int, int>::iterator it;
		for (it = graph.begin(); it != graph.end(); ++it) {
			if ((*it).first != (*it).second) {
				cout << (*it).first << " " << (*it).second << endl;
			}
		}
		gzclose(inFile);
	}

}
int main(int argc, char ** argv) {
	vector<float> scores;
	vector<string> inputFiles;

	while (1) {
		int opt;
		opt = getopt(argc, argv, "hf:s:");
		switch (opt) {
		case 'f':
			inputFiles.push_back(optarg);
			break;
		case 's':
			scores.push_back(atof(optarg));
			break;
		case 'h':
			syntax();
			return 0;
			break;
			return -1;
		}
		if (opt == -1) {
			break;
		}
		if (opt == '?') {
			syntax();
			return -1;
		}
	}
	if (inputFiles.size() == 0 || inputFiles.size() != scores.size()){
		syntax();
		return -1;
	}
	applyTransitivity(inputFiles, scores);
}
