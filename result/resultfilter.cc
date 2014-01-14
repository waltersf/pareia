/**
 * resultfilter.c
 * (c) e-Speed/DCC/UFMG
 * (c) Walter dos Santos Filho - waltersf AT gmail.com
 */
#include "../project.h"
#include "../logging.h"
#include "../groupedmessage.h"
#include "../messagesfp.h"
#include "../stlutil.h"
#include "../anthillutil.h"
#include "resultfilter.h"
#include <fcntl.h>
#include <iostream>
#include <sstream>
using namespace std;
using namespace Feraparda;

CREATE_ANTHILL_FILTER (ResultFilter, filter, new AnthillUtil());

ResultFilter::ResultFilter(AnthillUtil *ahUtil, void *work) {
	this->ahUtil = ahUtil;
	logger = LogFactory::GetLogger("result");

	this->project = (Project *) work;

	myRank = ahUtil->Rank();
	OpenPorts();
	pairsGenerated = 0;
	this->config = config;
	this->linkage = project->getTaskType() == LINKAGE;
}
void ResultFilter::OpenPorts() {
	if (parameters.deterministic){
		in = ahUtil->GetInputPort("blockingInput");
	} else {
		in = ahUtil->GetInputPort("readerInput");
	}

}
void ResultFilter::Process() {
	if (parameters.deterministic){

		int fd = open64(project->getOutput()->getDeterministic().c_str(), O_RDWR | O_CREAT, 0666);
		if (fd < 0){
			throw FerapardaException(string("Error opening file (") +
					project->getOutput()->getDeterministic() +
					string(") for outputing result pairs in result filter: ") + string(strerror(errno)));
		}
		gzFile outFile = gzdopen(fd, "wb");
		result_grouped_msg_t msg;
		while (ahUtil->Read(in, &msg, sizeof(result_grouped_msg_t)) != EOW) {
			for(int i =0; i < msg.total; i++){
				gzprintf(outFile, "%d %d\n", msg.msgs[i].id1, msg.msgs[i].id2);
			}
		}
		gzseek(outFile, 1L, SEEK_CUR); /* add one zero byte */
		gzclose(outFile);

	} else if (!parameters.stopAtMerging){
		int fd = open64(project->getOutput()->getOutput().c_str(), O_RDWR | O_CREAT	, 0666);
		if (fd < 0){
			throw FerapardaException(string("Error opening file (") +
					project->getOutput()->getOutput() +
					string(") for outputing result pairs in result filter: ") + string(strerror(errno)));
		}
		gzFile outFile = gzdopen(fd, "wb");
		result_grouped_msg_t msg;

		float minForOutput = project->getOutput()->getMinimumWeight();

		float min = project->getClassifier()->getMinResultForMatch();
		float max = project->getClassifier()->getMaxResultForUnmatch();
		int resultGreaterThanMinimum = 0;

		int counter = 0;
		while (ahUtil->Read(in, &msg, sizeof(result_grouped_msg_t)) != EOW) {
			counter += msg.total;
			for (int i = 0; i < msg.total; i++) {
				resultGreaterThanMinimum += SaveResult(outFile, msg.msgs[i], min, max, minForOutput);
			}
			if ((counter % 5000000) == 0){
				cout << counter << " pairs compared " << endl;
			}
		}
		if (logger->IsLevelEnabled(Feraparda::LOGINFO)) {
			LOGFPINFO(logger, "Results greater than " + Util::ToString(min) + ": " +
					Util::ToString(resultGreaterThanMinimum));
			LOGFPINFO(logger, "Results less than " + Util::ToString(min) + ": " +
					Util::ToString(counter - resultGreaterThanMinimum));
		}
		gzseek(outFile, 1L, SEEK_CUR); /* add one zero byte */
		gzclose(outFile);
	}
}
int ResultFilter::SaveResult(gzFile out, result_msg_t msg, float min, float max, float minForOutput) {
	if (logger->IsLevelEnabled(LOGDEBUG)) {
		ostringstream o;
		o << "Result for comparison of pair (" << msg.key1 << ","
				<< msg.key2 << ") : " << msg.result << "("
				<< msg.id1 << "," << msg.id2 << ")";
		LOGFPDEBUG(logger, o.str());
	}
	char result = 'N';
	if (msg.weightResult > max){
		if (msg.weightResult < min){
			result = '?';
		} else {
			result = 'Y';
		}
	}
	if (msg.weightResult >= minForOutput) {
		char *comparisonScores;
		if (parameters.saveComparisonScores){
			comparisonScores = msg.resultVector;
		} else {
			comparisonScores = '\0';
		}
		if (!this->linkage && strcmp(msg.key1, msg.key2) > 0) {
			gzprintf(out, "%c %d %s %s %3.4f %3.4f %d %s\n", result, myRank,
					msg.key2, msg.key1, msg.result, msg.weightResult,
					msg.blockId, comparisonScores?comparisonScores: "");

		} else {
			gzprintf(out, "%c %d %s %s %3.4f %3.4f %d %s\n", result, myRank,
					msg.key1, msg.key2, msg.result, msg.weightResult,
					msg.blockId, comparisonScores?comparisonScores: "");
		}
		return 1;
	} else {
		return 0;
	}
}
