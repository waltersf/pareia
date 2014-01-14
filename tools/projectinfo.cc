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
	cerr << "Usage: ./projectinfo [-p <project_file>] [-h]" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-p <project_file>: path of Pareia's project file." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
int main(int argc, char ** argv) {
	string *projectFile = NULL;

	while (1) {
		int opt;
		opt = getopt(argc, argv, "hp:");
		switch (opt) {
		case 'p':
			projectFile = new string(optarg);
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

	Project *p = new Project(*projectFile, false);
	try {
		p->Init();
		Comparators comparators = p->GetComparators();
		for (Comparators::iterator it = comparators.begin(); it!= comparators.end(); ++it) {
			cout << (*it)->GetInfo() << endl; 
		}
	} catch (FerapardaException fe){
		cout << "Internal exception: " << fe.Report() << endl;
	} catch (std::exception e) {
		cout << "Exception " << e.what() << endl;
	}
	delete p;
	delete projectFile;
}
