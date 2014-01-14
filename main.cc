#include "project.h"
#include "Manager.h"
#include "FilterDev.h"
#include "logging.h"
#include "ferapardaexception.h"
#include <pvm3.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <cstdlib>
using namespace Feraparda;

void syntax() {
	cerr << "Feraparda: Deduplication and record linkage parallel tool - version 1.0" << endl;
	cerr << "(c) e-Speed/DCC/UFMG " << endl;
	cerr << "Usage: ./main -p <project_file> [-l <logging_config>] [-v] [-d] [-h] [-c <anthill_conf>] [-g]" << endl;
	cerr << "Where:" << endl;
	cerr << "\t-l <logging_config>: path of logging configuration file (default logging.properties)" << endl;
	cerr << "\t-p <project_file>: path of Feraparda's project configuration (required)" << endl;
	cerr << "\t-d : performs deterministic linkage/deduplication (default false)" << endl;
	cerr << "\t-v: captures the PVM stdout and stderr (default false)" << endl;
	cerr << "\t-b: Executes up to blocking/merging stage, saving the candidate pairs in a file (default false)" << endl;
	cerr << "\t-m: Executes from blocking/merging stage, reading the candidate pairs from a file (default false)" << endl;
	cerr << "\t-g: Executes an extra stage that tries to optimize communication between Anthill nodes (very experimental, do not use!) "<< endl;
	cerr << "\t-h: shows the syntax and exit" << endl;
	cerr << "\t-i: indexes data sources when their type is delimited and DO NOT perform deduplication nor linkage" << endl;
	cerr << "\t-r: reads all records before starting comparison" << endl;
	cerr << "\t-n: saves each field comparison score in result" << endl;
	cerr << "\t-c <anthill_conf>: path of Anthill configuration file (default conf.xml)" << endl;
	cerr << "\t-e <experiment_dir>: path where all configuration files reside " << endl;
	cerr << "\t\t\t  (note: default file names will be used)" << endl;
}
int main(int argc, char ** argv, char *envp[]) {

	project_parameters_t parameters;

	char anthillConf[255];
	char loggingConfig[255];
	char experiment[255];
	short optProject = 0;
	short optLogging = 0;
	short optVerbose = 0;
	short optExperiment = 0;

	parameters.readAllRecordsBeforeCompare = false;
	parameters.useSmallerId = false;
	parameters.useHeuristic = false;
	parameters.stopAtMerging = false;
	parameters.saveComparisonScores = false;
	parameters.indexDataSources = false;
	parameters.startAtMerging = false;
	parameters.deterministic = false;

	strcpy(anthillConf, "conf.xml");
	strcpy(parameters.configurationFile, "project.xml");
	while (1) {
		int opt;
		opt = getopt(argc, argv, "bdghimnrsvp:l:c:e:");
		switch (opt) {
		case 'b':
			parameters.stopAtMerging = true;
			parameters.startAtMerging = false;
			break;
		case 'c':
			strcpy(anthillConf, optarg);
			break;
		case 'd':
			parameters.deterministic = true;
			break;
		case 'e':
			strcpy(experiment, optarg);
			
			strcpy(loggingConfig, optarg);
			strcat(loggingConfig, "/logging.properties");
			
			strcpy(anthillConf, optarg);
			strcat(anthillConf, "/conf.xml");
			
			strcpy(parameters.configurationFile, optarg);
			strcat(parameters.configurationFile, "/project.xml");
			
			optExperiment = 1;
			optLogging = 1; 
			
			break;
		case 'g':
			parameters.useHeuristic = true;
			break;
		case 'h':
			syntax();
			return 0;
			break;
		case 'i':
			parameters.indexDataSources = true;
			break;
		case 'l':
			strcpy(loggingConfig, optarg);
			optLogging = 1;
			break;
		case 'm':
			parameters.stopAtMerging = false;
			parameters.startAtMerging = true;
			break;
		case 'n':
			parameters.saveComparisonScores = true;
			break;
		case 'p':
			strcpy(parameters.configurationFile, optarg);
			optProject = 1;
			break;
		case 'r':
			parameters.readAllRecordsBeforeCompare = true;
			break;
		case 's':
			parameters.useSmallerId = true;
			break;
		case 'v':
			optVerbose = 1;
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
	if (!optProject && !optExperiment){
		syntax();
		return -1;
	}
	if (optVerbose) {
		pvm_catchout(stdout);
		pvm_catchout(stderr);
	}
	try {
		if (optLogging){
			LogFactory::Configure(loggingConfig);
		} else {
			LogFactory::Configure("logging.properties");
		}
		Layout *layout = initDs(anthillConf, argc, argv);
		//When using strlen, remember add 1 for the \0 character.
		appendWork(layout, (void *) &parameters, sizeof(project_parameters_t));
		pvm_catchout(0);
		finalizeDs(layout);
	} catch(FerapardaException &ex) {
			cerr << "--------------------------------------------------------" << endl;
			cerr << "An exception had been threw in main() " << ex.Report() << endl;
			cerr << "--------------------------------------------------------" << endl;
	} catch(FerapardaException *ex) {
		cerr << "--------------------------------------------------------" << endl;
		cerr << "An exception had been threw in main() " << ex->Report() << endl;
		cerr << "--------------------------------------------------------" << endl;
	}
	return 0;
}
