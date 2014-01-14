#include <cstdio>
#include <cstdlib>
#include <vector>
#include <iostream>
#include "project.h"
#include <unistd.h>
#include "datasource.h"
using namespace std;
using namespace Feraparda;
void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./getsample -p <project_file> -r <number_of_records_to_dump>" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-p <project_file>: path of Pareia's project file." << endl;
	cerr << "\t-d <id_data_source>: id of data source (0 or 1)." << endl;
	cerr << "\t-r <number_of_records_to_dump>: Total of random records to dump in stdout." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
void sample(string projectFile, int rows, string idDs) {
	Project *p = new Project(projectFile, false);
	DataSource *ds;
	try {
		p->Init();
		if ((ds = p->getDataSource(idDs)) != NULL) {
			ds->Load(NULL);
			unsigned long totalOfRecords = ds->getRecordCount();
			/* initialize random seed: */
			srand(time(NULL));

			for(int i = 0; i < rows; i++){
				char *buffer = ds->GetLine(rand() % totalOfRecords);
				cout << buffer << endl;
				delete[] buffer;
			}
		} else {
			delete p;
			throw FerapardaException("Data source not found.");
		}
		delete p;

	} catch (FerapardaException fe){
		cout << "Internal exception: " << fe.Report() << endl;
	} catch (std::exception e) {
		cout << "Exception " << e.what() << endl;
	}
}
int main(int argc, char ** argv) {
	string *projectFile = NULL;
	int rows = 10;
	string idDs = "0";

	while (1) {
		int opt;
		opt = getopt(argc, argv, "hp:d:r:");
		switch (opt) {
		case 'p':
			projectFile = new string(optarg);
			break;
		case 'd':
			idDs = string(optarg);
			break;
		case 'r':
			rows = atoi(optarg);
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
	sample(*projectFile, rows, idDs);
	delete projectFile;
}
