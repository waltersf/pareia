#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <zlib.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>
#include "project.h"
#include <algorithm>

using namespace std;
using namespace Feraparda;
typedef struct {
	int id;
	vector<string> lines;
	bool ended;
} cluster_t;

void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./setoperation [-1 <field_in_file1>] [-2 <field_in_file2>] [-d|-c] file1 file2" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-d: Perform difference" << endl;
	cerr << "\t-c: Use file with candidates instead of result file" << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
void performSetOperation(string fileName1, string fileName2, int fieldNumber1, int fieldNumber2, bool performDifference){
	fstream file1;
	fstream file2;

	file1.open(fileName1.c_str(), fstream::in);
	file2.open(fileName2.c_str(), fstream::in);


	string line2 = "-1";
	string fieldInFile2;

	char buffer[4096];

	while(! file1.eof()){
		file1.getline(buffer, 4096);
		string line1 = string(buffer);

		if (line1.size() == 0){
			break;
		}
		if (! file2.eof()){

			stringstream ss1(line1);
			string fieldInFile1;
			for(int i = 0; i < fieldNumber1; i++){
				ss1 >> fieldInFile1;
			}

			while (false == file2.eof() && atol(fieldInFile1.c_str()) > atol(fieldInFile2.c_str())){
				file2.getline(buffer, 4096);
				line2 = string(buffer);
				stringstream ss2(line2);
				for(int i = 0; i < fieldNumber2; i++){
					ss2 >> fieldInFile2;
				}
			}
			if (file2.eof()){
				if (performDifference){
					cout << line1 << endl;
				} else {
					break;
				}
			} else if ((fieldInFile1 == fieldInFile2 && false == performDifference) ||
					(fieldInFile1 != fieldInFile2 && performDifference)){
				cout << line1 << endl;
			}

		} else {
			if (performDifference){
				cout << line1 << endl;
			} else {
				break;
			}
		}
	}
	file1.close();
	file2.close();
}
int GetFieldValue(char *line, int fieldIndex){
	stringstream ss1(line);
	string fieldValue;
	for(int i = 0; i < fieldIndex; i++){
		ss1 >> fieldValue;
	}
	return atoi(fieldValue.c_str());
}
void GetNextCluster(cluster_t *cluster, gzFile file, int fieldIndex, int *counter, char *line) {
	cluster->lines.clear();
	if (gzeof(file) && line[0] == '\0'){
		cluster->ended = true;
	} else {
		cluster->ended = false;
		char buffer[4096];
		int err;
		if (line[0] == '\0'){
			gzgets(file, buffer, sizeof(buffer));
		} else {
			strcpy(buffer, line);
			line[0] = '\0';
		}
		cluster->lines.push_back(string(buffer));
		cluster->id = GetFieldValue(buffer, fieldIndex);


		while (! gzeof(file)) {
			//z_off_t offset = gztell(file);
			if (!gzgets(file, buffer, sizeof(buffer))){
				gzerror(file, &err);
				if (err > 1){
					throw FerapardaException(gzerror(file, &err));
				} else {
					break;
				}
			} else {
				int id = GetFieldValue(buffer, fieldIndex);
				if (cluster-> id != id){
					//gzseek(file, offset, SEEK_SET); //point back to be able to read next cluster
					//save the read line for next execution
					strcpy(line, buffer);
					break;
				} else {
					cluster->lines.push_back(string(buffer));
				}
			}
		}
		*counter += cluster->lines.size();
	}
}
void PrintClusterDifference(cluster_t cluster1, cluster_t cluster2, int *counter){
	sort(cluster1.lines.begin(), cluster1.lines.end());
	sort(cluster2.lines.begin(), cluster2.lines.end());

	vector<string> diff(cluster1.lines.size());
	vector<string>::iterator endOfDiffIterator = set_difference (
			cluster1.lines.begin(), cluster1.lines.end(),
			cluster2.lines.begin(), cluster2.lines.end(), diff.begin());


	vector<string>::iterator it;
	for(it = diff.begin(); it != endOfDiffIterator; it++){
		cout << (*it);
		(*counter) ++;
	}
}
void PrintCluster(cluster_t cluster1){
	for(vector<string>::iterator it = cluster1.lines.begin(); it != cluster1.lines.end(); it++){
		cout << (*it);
	}
}
void performSetDiffOperationOverCandidates(string fileName1, string fileName2, int fieldNumber1, int fieldNumber2){
	int fd1 = open64(fileName1.c_str(), O_RDONLY, 0666);
	if (fd1 < 0){
		throw FerapardaException(string("Error opening file (") +
				fileName1 +
				string(") for reading candidate pairs: ") + string(strerror(errno)));
	}
	int fd2 = open64(fileName2.c_str(), O_RDONLY, 0666);
	if (fd2 < 0){
		throw FerapardaException(string("Error opening file (") +
				fileName2 +
				string(") for reading candidate pairs: ") + string(strerror(errno)));
	}

	gzFile file1 = gzdopen(fd1, "r");
	gzFile file2 = gzdopen(fd2, "r");

	if (file1 == NULL || file2 == NULL){
		throw FerapardaException("File not found when reading candidate pairs");
	}

	cluster_t cluster1;
	cluster_t cluster2;

	int counter1 = 0;
	int counter2 = 0;
	int counterDiff = 0;
	char line1[4096], line2[4096];
	line1[0] = '\0';
	line2[0] = '\0';

	GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
	GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
	while(! cluster1.ended) {
		if (cluster1.id < cluster2.id){
			PrintCluster(cluster1);
			GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
		} else if (cluster1.id == cluster2.id){
			PrintClusterDifference(cluster1, cluster2, &counterDiff);

			GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
			GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
		} else {
			//Just ignore the lines in cluster 2.
			if (!cluster2.ended) {
				GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
			} else {
				PrintCluster(cluster1);
				GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line2);
			}
		}
	}
	cerr << "Candidadates in file 1:\t\t\t" << counter1 << endl;
	cerr << "Candidadates in file 2:\t\t\t" << counter2 << endl;
	gzclose(file1);
	gzclose(file2);
}
void performSetUnionOperationOverCandidates(string fileName1, string fileName2, int fieldNumber1, int fieldNumber2){
	int fd1 = open64(fileName1.c_str(), O_RDONLY, 0666);
	if (fd1 < 0){
		throw FerapardaException(string("Error opening file (") +
				fileName1 +
				string(") for reading candidate pairs: ") + string(strerror(errno)));
	}
	int fd2 = open64(fileName2.c_str(), O_RDONLY, 0666);
	if (fd2 < 0){
		throw FerapardaException(string("Error opening file (") +
				fileName2 +
				string(") for reading candidate pairs: ") + string(strerror(errno)));
	}

	gzFile file1 = gzdopen(fd1, "r");
	gzFile file2 = gzdopen(fd2, "r");


	if (file1 == NULL || file2 == NULL){
			throw FerapardaException("File not found when reading candidate pairs");
	}
	cluster_t cluster1;
	cluster_t cluster2;

	int counter1 = 0;
	int counter2 = 0;
	int counterDiff = 0;

	char line1[4096], line2[4096];
	line1[0] = '\0';
	line2[0] = '\0';

	GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
	GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
	bool finishedCluster2 = false;
	bool lastCluster2WasPrinted = false;
	while(! cluster1.ended) {
		lastCluster2WasPrinted = false;
		if (cluster1.id < cluster2.id){
			PrintCluster(cluster1);
			GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
		} else if (cluster1.id == cluster2.id){
			PrintCluster(cluster1);
			PrintClusterDifference(cluster2, cluster1, &counterDiff);

			GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
			GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
		} else {
			if (!finishedCluster2){
				counterDiff += cluster2.lines.size();
				PrintCluster(cluster2);
				lastCluster2WasPrinted = true;
			}
			if (!cluster2.ended) {
				GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
			} else {
				finishedCluster2 = true;
				PrintCluster(cluster1);
				GetNextCluster(&cluster1, file1, fieldNumber1, &counter1, line1);
			}
		}
	}
	if (!lastCluster2WasPrinted) {
		PrintCluster(cluster2);
	}
	while (!cluster2.ended){
		GetNextCluster(&cluster2, file2, fieldNumber2, &counter2, line2);
		PrintCluster(cluster2);
	}
	cerr << "Candidadates in file 1:\t\t\t" << counter1 << endl;
	cerr << "Candidadates in file 2:\t\t\t" << counter2 << endl;
	cerr << "Candidadates that exists only in file 2:\t" << counterDiff << endl;
	gzclose(file1);
	gzclose(file2);
}
int main(int argc, char ** argv) {
	string fileName1;
	string fileName2;
	bool performDifference = false;
	bool considerAsCandidates = false;
	int field1 = 1;
	int field2 = 1;

	while (1) {
		int opt;
		opt = getopt(argc, argv, "dc1:2:");
		switch (opt) {
		case '1':
			field1 = atoi(optarg);
			break;
		case '2':
			field2 = atoi(optarg);
			break;
		case 'c':
			considerAsCandidates = true;
			break;
		case 'd':
			performDifference = true;
			break;
		case 'h':
			syntax();
			return 0;
			break;
		}
		if (opt == -1) {
			break;
		}
		if (opt == '?') {
			syntax();
			return -1;
		}
	}
	if (optind < argc){
		fileName1 = string(*(argv + optind));
	}
	if (optind + 1< argc){
		fileName2 = string(*(argv + optind + 1));
	}
	if(fileName1.size() > 0 && fileName2.size() > 0) {
		if (considerAsCandidates){
			if (performDifference){
				performSetDiffOperationOverCandidates(fileName1, fileName2, field1, field2);
			} else {
				performSetUnionOperationOverCandidates(fileName1, fileName2, field1, field2);
			}
		} else {
			performSetOperation(fileName1, fileName2, field1, field2, performDifference);
		}
		return 0;
	} else {
		syntax();
		return 2;
	}
}
