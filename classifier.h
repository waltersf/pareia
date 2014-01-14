#ifndef CLASSIFIER_H_
#define CLASSIFIER_H_
#include "datasource.h"
using namespace std;
namespace Feraparda{
	enum ClassifierType {FELLEGI_SUNTER};
	enum ClassifyResult {NON_MATCH, MATCH, DOUBT};
	class Classifier{
		private:
			ClassifierType type;
			double minResultForMatch;
			double maxResultForUnmatch;
		public:
			double getMinResultForMatch(){return minResultForMatch;}
			double getMaxResultForUnmatch(){return maxResultForUnmatch;}
			virtual ClassifyResult Classify(double) = 0;
			virtual ~Classifier(){};
			Classifier(ClassifierType t, double min, double max){
			  type = t;
			  minResultForMatch = min;
			  maxResultForUnmatch = max;
			}
	};
	class FSClassifier : public Classifier {
	  public:
		FSClassifier(ClassifierType t, double min, double max): Classifier(t, min, max){
		}
		~FSClassifier(){}
	  	ClassifyResult Classify(double);
	};
}
#endif
