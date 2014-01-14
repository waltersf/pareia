#ifndef _RESULT_H
#define _RESULT_H

#define MAX_RESULTS_PER_FILE 100000
#define MINIMUM 10

#include "anthillutil.h"
#include "basefilter.h"
#include "project.h"

#include <iostream>
#include <string>

#include <zlib.h>
using namespace std;
using namespace __gnu_cxx;

namespace Feraparda {
	class ResultFilter : public FerapardaBaseFilter{
	private:
		int SaveResult(gzFile out, result_msg_t msg, float min, float max, float maxOutput);
		Project *project;

		public:
	 		void Process();
			~ResultFilter(){}
	 		ResultFilter(AnthillUtil *ahUtil, void *work);
		private:
			string config;
			int myRank;
			int pairsGenerated;
			bool linkage;
	  	void Classify(int, int);

		  void OpenPorts();
      AnthillInputPort in;
	};
}
#endif
