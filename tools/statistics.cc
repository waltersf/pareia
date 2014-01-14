#include <map>
#include <vector>
#include <map>
#include <zlib.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include "project.h"
#include "datasource.h"

typedef map<string, int> * map_counter;

using namespace std;
void syntax() {
	cerr << "Pareia: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./statistics -p <project_file> [-o output_dir]" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-p <project_file>: path of Pareia's project file (required)." << endl;
	cerr << "\t-o <output_dir>: path to directory where files will be saved (optional)." << endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
}
void generateStatisticsForDataSource(DataSource *ds, string output){

	cout << "Generating statistics for data source " << ds->getId()
		<<". Total of records: " << ds->getRecordCount() << endl;
	cout << "-------------------------------------------------------" << endl;
	cout << "Fields present:" << endl;
	ds->ListFields("\t\t");
	cout << "-------------------------------------------------------" << endl;

	vector<string> fields = ds->getFieldNames();
	int totalOfRecords = ds->getRecordCount();
	vector<map<string, int > * > data(fields.size());

	int counter = 0;
	for(vector<string>::iterator it = fields.begin(); it
				!= fields.end(); ++it) {
		data[counter ++] = new map<string, int >();
	}

	for(int i = 0; i < totalOfRecords; i++){
		if ((i % 100000) == 0 && i > 0){
			cout << "Processed record #" << i << endl;
		}
		Record *r = ds->GetRecord(i);

		for (unsigned int f = 0; f < fields.size(); f ++) {
			string name = fields[f];

			if (name != "nw" && name != "id"){
				map<string, int > *mapValue = data[f];

				string fieldValue = r->getField(f);

				map<string, int >::iterator it2;
				if ((it2 = mapValue->find(fieldValue)) != mapValue->end()){
					//increments the counter
					(*it2).second ++;
				} else {map<string, int > *mapValue = data[f];
					mapValue->insert(pair<string,int>(fieldValue, 1));
				}
			}
		}
		delete r; //Only if it is not in cache!
	}
	float log2BaseE = log(2);

	for (unsigned int f = 0; f < fields.size(); f ++) {
		string name = fields[f];
		fstream resultFile;
		string fileName(output + "/" + name + ".txt");
		resultFile.open (fileName.c_str(), fstream::in | fstream::out | fstream::trunc);

		map<string, int > mapValue = *(data[f]);
		int size = mapValue.size();

		for(map<string, int>::iterator it2 = mapValue.begin(); it2 != mapValue.end(); ++it2) {
			//float weight1 = log((float) totalOfRecords / (float) it2->second) / log2BaseE ;
			//Fórmula do Odilon
			float weight1 = log(1.0 / ((float) it2->second / (float) totalOfRecords)) / log2BaseE ;
			/*see: "An Empirical Modification to the Probabilistic Record Linkage
			* Algorithm Using Frequency-Based Weight Scaling" - Zhu et al.
			* Journal of the American Medical Informatics Association Volume 16 Number 5 September / October 2009
			*/
			float weight2 = sqrt(totalOfRecords / (float) (size * it2->second));

			resultFile << it2->second << "\t" << weight1 << "\t" << weight2 << "\t" << it2->first << endl;
		}
		delete data[f];
		resultFile.close();
	}
}
void generateStatistics(string projectFile, string output) {
	Project *p = new Project(projectFile, false);
	DataSource *ds;
	try {
		p->Init();
		if ((ds = p->getDataSource("0")) != NULL) {
			ds->Load(NULL);
			generateStatisticsForDataSource(ds, output);
		}
		if (p->getTaskType() == LINKAGE){
			if ((ds = p->getDataSource("1")) != NULL) {
				ds->Load(NULL);
				generateStatisticsForDataSource(ds, output);
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
	string project("");
	string output("");

	while (1) {
		int opt;
		opt = getopt(argc, argv, "hp:o:");
		switch (opt) {
		case 'p':
			project.append(optarg);
			break;
		case 'o':
			output.append(optarg);
			break;
		case 'h':
			syntax();
			return 0;
		}
		if (opt == -1) {
			break;
		}
		if (opt == '?') {
			syntax();
			return -1;
		}
	}
	if (project.size() == 0){
		syntax();
		return -1;
	}
	cout << "Output dir: " << output << endl;
	generateStatistics(project, output);
	return 0;
}
