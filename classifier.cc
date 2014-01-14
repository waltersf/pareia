#include "classifier.h"
#include "datasource.h"
using namespace std;
using namespace Feraparda;

ClassifyResult FSClassifier::Classify(double v){
  if (v >= getMinResultForMatch()){
	return MATCH;
  } else if(v <= getMaxResultForUnmatch()){
	return NON_MATCH;
  } else {
	return DOUBT;
  }
};
