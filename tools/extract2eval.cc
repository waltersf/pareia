#include <cstdio>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include "project.h"
#include "datasource.h"
#include <zlib.h>
#include <fcntl.h>
using namespace std;
using namespace Feraparda;
void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./extract2eval -p <project_file> -r <result_file> [-n <number of records>] [-m <mininum_score>] [-x <maximum_score>]" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-p <project_file>: path of Pareia's project file." << endl;
	cerr << "\t-n <number_of_records_to_dump>: Max. number of records to dump in stdout (default: all)." << endl;
	cerr << "\t-r <result_file>: File with the result of comparisons." << endl;
	cerr << "\t-m <score>: Minimum score." << endl;
	cerr << "\t-x <score>: Maximum score (default minimum score)." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
void DumpRecords(string projectFile, string resultFile, int total, float min, float max, bool keyIsInteger) {
	Project *p = new Project(projectFile, false);
	DataSource *ds1;
	DataSource *ds2;
	try {

		Cache<unsigned int, Record*> *cache = new Cache<unsigned int, Record *>(
				CACHE_RECORD, true, true, LessRecentlyUsed);
		cache->setSize(10000);
		Cache<unsigned int, Record*> *cache2 = NULL;
		if (p->getTaskType() == LINKAGE){
			cache2 = new Cache<unsigned int, Record *>(
					CACHE_RECORD, true, true, LessRecentlyUsed);
			cache2->setSize(10000);
		}

		p->setCache(cache, cache2);

		p->Init();
		if ((ds1 = p->getDataSource("0")) != NULL) {
			ds1->Load(p->getCache());
		}
		if (p->getTaskType() == LINKAGE){
			if ((ds2 = p->getDataSource("1")) != NULL) {
				ds2->Load(p->getCache2());
			}
		}
		int fd = open64(resultFile.c_str(), O_RDONLY, 0666);
		if (fd < 0){
			throw FerapardaException(string("Error opening file (") +
					resultFile +
					string(") for outputing candidate pairs: ") + string(strerror(errno)));
		}
		gzFile inFile = gzdopen(fd, "r");
		if (inFile == NULL){
			throw FerapardaException("File not found when reading candidate pairs at Reader Filter");
		}
		unsigned long counter = 0;
		char buf[4096];
		int err;

		vector<string> fields = ds1->getFieldNames();
		cout << "Cluster\tNota\t";
		for (vector<string>::iterator it = fields.begin(); it != fields.end(); ++it) {
			cout << (*it) << "\t";
		}
		cout << endl;
		string lastId = "";
		for (;;) {
			counter++;
			if ((counter % 100000) == 0) {
				cerr << counter << " pairs processed up to now" << endl
						<< flush;
			}
			if (!gzgets(inFile, buf, sizeof(buf))){
				gzerror(inFile, &err);
				if (err > 1){
					throw FerapardaException(gzerror(inFile, &err));
				} else {
					break;
				}
			} else {
				char id1[100];
				char id2[100];
				int instance;
				float score;
				char result;
				sscanf(buf, "%c %d %s %s %f", &result, &instance, id1, id2, &score);

				if (score >= min && score <= max){
					if (atol(id2) < atol(id1)) {
						char temp[100];
						strncpy(temp, id1, 100);
						strncpy(id1, id2, 100);
						strncpy(id2, temp, 100);
					}

					Record *r1 = ds1->GetRecordByDatabaseId(string(id1), keyIsInteger);
					Record *r2;
					if (p->getTaskType() == LINKAGE){
						r2 = ds2->GetRecordByDatabaseId(string(id2), keyIsInteger);
					} else {
						r2 = ds1->GetRecordByDatabaseId(string(id2), keyIsInteger);
					}
					if (r2 == NULL || r1 == NULL){
						throw FerapardaException("Record with key " + string(id1) + " or key " + string(id2) + " not found.");
					}

					if (lastId != r2->getField("id")){
						cout << r2->getField("id")  << "\t" << score << "\t" << r2->Pack() << endl;
						lastId = r2->getField("id");
					}
					cout << r2->getField("id") << "\t" << score << "\t" << r1->Pack() << endl;
				}
			}
		}
		gzclose(inFile);
		delete cache;
		if (cache2 != NULL){
			delete cache2;
		}
	} catch (FerapardaException fe){
		cerr << "Internal exception: " << fe.Report() << endl;
	} catch (std::exception e) {
		cerr << "Exception " << e.what() << endl;
	}
	delete p;
}
int main(int argc, char ** argv) {
	string *projectFile = NULL;
	string *resultFile = NULL;

	int total = 0;
	float min = 0.0;
	float max = 0.0;
	bool keyIsInteger = true;

	while (1) {
		int opt;
		opt = getopt(argc, argv, "hip:n:r:m:x:");
		switch (opt) {
		case 'p':
			projectFile = new string(optarg);
			break;
		case 'i':
			keyIsInteger = false;
			break;
		case 'r':
			resultFile = new string(optarg);
		case 'n':
			total = atoi(optarg);
			break;
		case 'm':
			min = atof(optarg);
			break;
		case 'x':
			max = atof(optarg);
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
	if (projectFile == NULL || resultFile == NULL){
		syntax();
		return -1;
	}
	cerr << "Dumping records with score in range [" << min << "," << max << "]." << endl;
	DumpRecords(*projectFile, *resultFile, total, min, max, keyIsInteger);

	delete projectFile;
	delete resultFile;
}
