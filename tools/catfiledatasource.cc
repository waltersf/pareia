#include <cstdio>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include "project.h"
#include "datasource.h"
using namespace std;
using namespace Feraparda;
void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./catfiledatasource [-p <project_file>] [-r <number_of_records_to_dump>] [-s start] [-h]" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-p <project_file>: path of Pareia's project file." << endl;
	cerr << "\t-r <number_of_records_to_dump>: Total of records to dump in stdout." << endl;
	cerr << "\t-s <start> Record to start dumping from." << endl;
	cerr << "\t-t <start> Record to start dumping from, used only when is a linkage project. Dump from db2." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
void dumpRecords(DataSource *ds, int start, int rows, bool sequential){

	vector<string> fields = ds->getFieldNames();
	if (sequential){
		ds->setPosition(start);
	}
	for(int i = start; i < start + rows; i++){
		cout << "Record #" << i << endl;
		cout << "-------------------------------------------------------" << endl;
		if (ds->HasRecord(i)) {
			Record *r;
			if (sequential){
				r = ds->GetNextRecord();
			} else {
				 r = ds->GetRecord(i);
			}
			for (vector<string>::iterator it = fields.begin(); it
					!= fields.end(); ++it) {
				string fieldValue = r->getField(*it);
				if (fieldValue != "\n"){
					cout << (*it) << ": " << fieldValue << "\t";
				}
			}
			cout << endl; 
			delete r; //Only if it is not in cache!
		}
	}
}
void dumpDataSources(string projectFile, int start1, int start2, int rows, bool sequential) {
	Project *p = new Project(projectFile, false);
	DataSource *ds;
	try {
		p->Init();
		if ((ds = p->getDataSource("0")) != NULL) {
			ds->Load(NULL);
			cout << "First " << rows << " rows of datasource 0 - total: " << ds->getRecordCount() << endl;
			dumpRecords(ds, start1, rows, sequential);
		}
		if (p->getTaskType() == LINKAGE){
			if ((ds = p->getDataSource("1")) != NULL) {
				ds->Load(NULL);
				cout << "First " << rows << " rows of datasource 1 - total: " << ds->getRecordCount() << endl;
				dumpRecords(ds, start2, rows, sequential);
			}
		}
	} catch (FerapardaException fe){
		cout << "Internal exception: " << fe.Report() << endl;
	} catch (std::exception e) {
		cout << "Exception " << e.what() << endl;
	}
	delete p;
}
void indexDatasource(string projectFile) {
	Project *p = new Project(projectFile, false);
	DataSource *ds;
	try {
		p->Init();
		if ((ds = p->getDataSource("0")) != NULL) {
			//ds->Load(NULL);
			cout << "Indexing datasource 0." << endl;
			ds->Index();
			cout << "Finished indexing datasource 0." << endl;
		}
		if (p->getTaskType() == LINKAGE){
			if ((ds = p->getDataSource("1")) != NULL) {
				cout << "Indexing datasource 1." << endl;
				ds->Index();
				cout << "Finished indexing datasource 1." << endl;
			}
		}
	} catch (FerapardaException fe){
		cout << "Internal exception: " << fe.Report() << endl;
	} catch (std::exception e) {
		cout << "Exception " << e.what() << endl;
	}
	delete p;
}
int main(int argc, char ** argv) {
	string *projectFile = NULL;
	int start1 = 0;
	int start2 = 0;
	int rows = 10;
	bool index = false;
	bool sequential = false;

	while (1) {
		int opt;
		opt = getopt(argc, argv, "qhip:r:s:t:");
		switch (opt) {
		case 'p':
			projectFile = new string(optarg);
			break;
		case 's':
			start1 = atoi(optarg);
			break;
		case 't':
			start2 = atoi(optarg);
			break;
		case 'r':
			rows = atoi(optarg);
			break;
		case 'i':
			index = true;
			break;
		case 'q':
			sequential = true;
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
	if (projectFile == NULL){
		syntax();
		return -1;
	}
	if (index){
		indexDatasource(*projectFile);
	} else {
		dumpDataSources(*projectFile, start1, start2, rows, sequential);
	}
	delete projectFile;
}
