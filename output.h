#ifndef OUTPUT_H_
#define OUTPUT_H_
#include <string>
using namespace std;
namespace Feraparda {
	enum OutputFormat {PAIR, RECORD, EXCEL};
	class Output {
		private: 
			/** Path to the histogram */
			string histogram;
			string candidates;
			OutputFormat format;
			/** File where the results will be stored */
			string output;
			string deterministic;
			float minimumWeight;
			float maximumWeigth;
			
			void Init(OutputFormat format, string output, float min, float max, string histogram, string candidates, string deterministic){
				this->format = format;
				this->output = output;
				this->minimumWeight = min;
				this->maximumWeigth = max;
				this->histogram = histogram;
				this->candidates = candidates;
				this->deterministic = deterministic;
			}
		public:
			Output(OutputFormat format, string output, float min, float max, string histogram, string candidates, string deterministic){
				Init(format, output, min, max, histogram, candidates, deterministic);
			}
			bool IsCreateHistogram(){return ""!= histogram;}
			OutputFormat getFormat(){return format;}
			void setFormat(OutputFormat format){this->format = format;}
			string getOutput(){return output;}
			string getCandidates(){return candidates;}
			string getDeterministic(){return deterministic;}
			void setOutput(string out){this->output = out;}
			string getHistogram(){return histogram;}
			void setHistogram(string path){this->histogram = path;}
			float getMinimumWeight(){return minimumWeight;}
			void setMinimumWeight(float min){this->minimumWeight = min;}
			float getMaximumWeight(){return maximumWeigth;} 
			void setMaximumWeight(float max){this->maximumWeigth = max;}
	};
}
#endif /*OUTPUT_H_*/
