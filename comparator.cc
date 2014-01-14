#include "comparator.h"
#include "datasource.h"
#include "encode.h"
#include <string>
#include <sstream>

using namespace std;
using namespace Feraparda;

string Comparator::GetInfo(){
	ostringstream o;
	o << "Comparator: Fields: (" << field1 << ", " << field2 << "). M and U: (" << m << ", " << u << ") Missing, Match, Mismatch: (" 
			<< missing << ", " << matchWeight << ", " << mismatchWeight << "). Use weight table: " << useWeightTable; 
	return o.str();
};
float ExactStringComparator::Compare(Record *r1, Record *r2) {
	string f1 = r1->getField(this->field1Index);
	string f2 = r2->getField(this->field2Index);

	if(f1.size() == 0 || f2.size() == 0) {
		return this->missing;
	} else if (f1 == f2){
		if (useWeightTable) {
			return mismatchWeight;
			//Just one query, after all, values are equal :P
			return r1->getWeight(this->field1, f1);
		} else {
			return matchWeight;
		}
	} else {
		if (useWeightTable){
			return 0; //FIXME: ter valor padrão quando não casa : return r1->getDefaultWeight(this->field1);
		} else {
			return mismatchWeight;
		}
	}
}
float ApproximatedStringComparator::Compare(Record *r1, Record *r2) {
	string f1 = r1->getField(this->field1Index);
	string f2 = r2->getField(this->field2Index);
	if (f1.size() == 0 || f2.size() == 0) {
		return this->missing;
	} else {
		float result = this->function(f1.c_str(), f2.c_str());
		if (result < this->minValueToBeMatch) {
			return mismatchWeight;
			if (useWeightTable){
				return 0; //FIXME r1->getDefaultWeight(this->field1);
			} else {
				return mismatchWeight;
			}
		} else {
			//if (result < 1){
			//	cout << " Resultado maior ou igual " << result << " " << this->minValueToBeMatch << " " << r1->getField(this->field1) << " " << r2->getField(this->field2) << endl;
			//}
			//cout << "[AppComparator] (" << f1 << "," << f2 << "): " << 
			//	(result	* LOG_RATIO_M_U(this->m, this->u)) << endl;
			if (useWeightTable){
				//cout << "Weight table " << r1->getWeight(this->field1, f1) << " " << r2->getWeight(this->field2, f2) << " " << f1 << " " << f2 << endl;
				return MIN(r1->getWeight(this->field1, f1), r2->getWeight(this->field2, f2)); // * result;
			} else {
				return matchWeight ; //result * matchWeight; //FIXME: Must have a parameter to indicate if it is to use the result in weight!
			}
		} 
	}
}
float EncodedStringComparator::Compare(Record *r1, Record *r2) {
	string f1 = r1->getField(this->field1Index);
	string f2 = r2->getField(this->field2Index);
	/*
	 if (function == &doubleMetaphone){
	 //FIXME: Needs to be refactored to C++ syntax
	 char **metaphone = (char **) malloc(sizeof(char *) * 2);
	 char *dup = (char *) malloc(sizeof(char) * fieldValue.size());
	 strcpy(dup, f1.c_str());
	 int size = expressions[e]->getTransformMaxSize();
	 if (size <= 0){
	 size = 5;
	 }
	 doubleMetaphone((const char*) dup, metaphone, size);	
	 key += metaphone[0];
	 key += metaphone[1];
	 free(metaphone[0]);
	 free(metaphone[1]);
	 free(metaphone);
	 } else if (function == &nysiis){
	 }
	 char encoded1[2][100];
	 char encoded2[2][100];
	 
	 
	 if (f1 == "" || f2 == ""){
	 return this->missing;
	 } else { 
	 //FIXME
	 return this->function(f1.c_str(),f2.c_str());
	 }*/
	return 0;
}
/**
 * Compare two dates in ISO standard format: yyyyMMdd.
 * Rules (in order):
 * 	- If size of any date is < 8 => missing
 *  - If strings are equals => highest score
 *  - If year1 = year2, day1 = day2 and month1 = 06 and month2 = 07 (or vice-versa) => 90% of highest score (july and june)
 *  - If month1 = month2, day1 = day2 and decade of year1 = 60's and decade of year2 = 70's (or vice-versa) => 90% of highest score (in portuguese, 'sessenta' is close to 'setenta' when speaking)
 *  - Each mismatch in each digit after century in date decreases the score in 20% up to 0.
 */
float DateComparator::Compare(Record *r1, Record *r2) {
	string date1 = r1->getField(this->field1Index);
	string date2 = r2->getField(this->field2Index);
	float score = 0;

	if (date1.size() != 8 || date2.size() != 8){
		return this->missing;
	} else if (date1 == date2){
		score = 1;
	} else {
		string year1 = date1.substr(0, 4);
		string year2 = date2.substr(0, 4);

		string month1 = date1.substr(4, 2);
		string month2= date2.substr(4, 2);

		string day1 = date1.substr(6, 2);
		string day2 = date2.substr(6, 2);

		if (year1 == year2 && day1 == day2 && ((month1 == "07" && month2 == "06") || (month1 == "06" && month2 == "07"))){
			score = 0.9;
		} else if (month1 == month2 && day1 == day2 && ((year1[2] == '6' && year2[2] == '7') || (year1[2] == '7' && year2[2] == '6'))){
			score = 0.9;
		} else {
			score = 1;
			for (int i =2; i < 8; i++){
				if (date1[i] != date2[i]){
					score -= 0.2;
				}
			}
			score = score < 0? 0: score;
		}
	}
	if (score < minValueToBeMatch){
		return mismatchWeight;
		if (useWeightTable) {
			return 0;
		} else {
			return mismatchWeight;
		}
	} else {
		if (useWeightTable) {
			return score * MIN(r1->getWeight(this->field1, date1), r2->getWeight(this->field2, date2));
		} else {
			return score * matchWeight;
		}
	}
}
float BrazilianZipComparator::Compare(Record *r1, Record *r2) {
	string f1 = r1->getField(this->field1Index);
	string f2 = r2->getField(this->field2Index);

	//Size must be 8 digits
	if (f1 == f2 && f1.size() == 8) {
		if (f1.substr(5) == string("000") || f2.substr(5) == string("000")) { //a city zip code!
			return LOG_RATIO_M_U_MATCH(this->mCity, this->uCity);
		} else {
			return matchWeight;
		}
	} else if (f1.size() == 0 || f2.size() == 0) {
		return missing;
	} else {
		return mismatchWeight;
	}
}
float BrazilianCityComparator::Compare(Record *r1, Record *r2) {
	string f1 = r1->getField(this->field1Index);
	string f2 = r2->getField(this->field2Index);

	if(f1.size() == 0 || f2.size() == 0) {
		return this->missing;
	} else if (f1 == f2) {
		if (useWeightTable) {
            //Just one query, after all, values are equal :P
            return r1->getWeight(this->field1, f1);
		} else {
			return matchWeight;
		}
	} else if (f1.substr(0, 2) == f2.substr(0, 2)) { //a match for state!
		float matchState = LOG_RATIO_M_U_MATCH(this->mState, this->uState);
		return matchState + mismatchWeight;
	} else {
		return LOG_RATIO_M_U_MISMATCH(this->mState, this->uState);
	}
}
