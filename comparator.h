#ifndef COMPARATOR_H_
#define COMPARATOR_H_
#include "datasource.h"
#include "stringutil.h"
#include <math.h>
#include <cassert>
#include <string>
using namespace std;
using namespace Feraparda;

#define LOG2_BASE_E 0.693147181
#define LOG_RATIO_M_U_MATCH(m, u) log(m / u) / LOG2_BASE_E; 
#define LOG_RATIO_M_U_MISMATCH(m, u) log((1.0 - m) / (1.0 - u)) / LOG2_BASE_E

namespace Feraparda{
	class Comparator {
		protected:
			float m;
			float u;
			float missing;
			string field1;
			string field2;
			int field1Index;
			int field2Index;
			float mismatchWeight;
			float matchWeight;
			bool useWeightTable;
		public:
			Comparator(){
				m = 0.0; u =0.0;
				missing = 0.0;
				useWeightTable = false;
				field1Index = -1;
				field2Index = -2;
			}
			virtual ~Comparator() {};
			virtual float Compare(Record *, Record *) = 0;
			void setUseWeightTable(bool v){
				this->useWeightTable = v;
			}
			void setFieldIndex(int id, int index){
				if (id == 1){
					field1Index = index;
				} else {
					field2Index = index;
				}
			}
			//
			void setU(float u){
				assert(u > 0.0);
				this->u = u;
				if (m > 0){
					mismatchWeight = LOG_RATIO_M_U_MISMATCH(m, u);
					matchWeight = LOG_RATIO_M_U_MATCH(m, u);
				}
			}
			float getU(){return u;}
			void setM(float m){
				assert(m > 0.0); 
				this->m = m;
				if (u > 0){
					mismatchWeight = LOG_RATIO_M_U_MISMATCH(m, u);
					matchWeight = LOG_RATIO_M_U_MATCH(m, u);
				}
			}
			float getM(){return m;}
			void setMissing(float miss){this->missing = miss;}
			float getMissing(){return missing;}
			void setField1(string f){this->field1 = f;}
			string getField1(){return field1;}  
			void setField2(string f){this->field2 = f;}
			string getField2(){return field2;}  
			string GetInfo();
	};
	class StringComparator: public Comparator {
		private:
			int start;
			int end;
		public:
	};
	class ApproximatedStringComparator: public StringComparator{
	    public:
			float Compare(Record *, Record *);
			void setMinValueToBeMatch(float v){minValueToBeMatch = v;}
			void setFunction(float (*f)(const char *, const char *)){ function = f;}
			ApproximatedStringComparator(){
				minValueToBeMatch = 1.0;
			}
		private:
			float minValueToBeMatch;
			float (*function)(const char *, const char *);
	};
	class ExactStringComparator: public StringComparator {
	  public:
			float Compare(Record *, Record *);
	};
	class EncodedStringComparator: public StringComparator {
		public:
			float Compare(Record *, Record *);
			void setFunction(void (*f)(const char *, char **, int)){ function = f;}
		private:
			void (*function)(const char *, char **, int);
	};
	class DateComparator : public Comparator {
		private:
			float minValueToBeMatch;
		public:
			DateComparator(){
				this->minValueToBeMatch = 1.0;
			}
			void setMinValueToBeMatch(float v){minValueToBeMatch = v;}
			float Compare(Record *, Record *);
	};
	/**
	 * A comparator for the Brazilian's zip code. 
	 * In Brazil, zip codes have 8 digits. The 5 first ones identify the city and
	 * the 3 last identify the locality. But there are some cities that have only
	 * one zip code and so, the 3 last one digits are zeroes.
	 */ 
	class BrazilianZipComparator : public Comparator {
		public:
			float Compare(Record *, Record *);
			void setMCity(float m){mCity = m;}
			void setUCity(float u){uCity = u;}
		private:
			float mCity;
			float uCity;
			
	};
	class BrazilianCityComparator : public Comparator {
		public:
			float Compare(Record *, Record *);
			void setMState(float m){mState = m;}
			void setUState(float u){uState = u;}
		private:
			float mState;
			float uState;
			
	};
}
#endif
