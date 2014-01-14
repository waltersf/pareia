#ifndef PROJECT_H_
#define PROJECT_H_
#include "blocking.h"
#include "cache.h"
#include "datasource.h"
#include "stlutil.h"
#include "classifier.h"
#include "comparator.h"
#include "output.h"
#include <cstring>
#include <cerrno>
#include <vector>
#include <ext/hash_map>
using namespace std;
using namespace Feraparda;
typedef hash_map<string, DataSource *, stu_comparator_t, stu_eqstr_t>
		DataSources;
typedef vector<Comparator *> Comparators;
typedef vector<Block *> Blocks;

/**
 *
 */
typedef struct {
  char configurationFile[255];
  bool readAllRecordsBeforeCompare;
  bool useHeuristic;
  bool useSmallerId;
  bool stopAtMerging;
  bool startAtMerging;
  bool saveComparisonScores;
  bool indexDataSources;
  bool deterministic;
} project_parameters_t;

/**
 * Feraparda is a program that executes the deduplication or record linkage process.
 */
namespace Feraparda {

/**
 * Defines a deduplication or record linkage process.
 */
enum TaskType {
	LINKAGE, DEDUPLICATION
};
class Project {
protected:
	Project(){
		output = NULL;
		classifier = NULL;
		cache = NULL;
		readAllRecordsBeforeCompare = false;
		loaded = false;
		taskType = DEDUPLICATION;
	}//Mainly for unit tests
public:
	void UpdateFieldsIndexes(){
		Comparators::iterator it;
		DataSource *ds0 = getDataSource("0");
		DataSource *ds1 = NULL;
		if (LINKAGE == taskType){
			ds1 = getDataSource("1");
		}
		for(it = comparators.begin(); it != comparators.end(); it++){
			(*it)->setFieldIndex(1, ds0->getFieldIndex((*it)->getField1()));
			if (LINKAGE == taskType){
				(*it)->setFieldIndex(2, ds1->getFieldIndex((*it)->getField2()));
			} else {
				(*it)->setFieldIndex(2, ds0->getFieldIndex((*it)->getField2()));
			}
		}
	}
	void EnableReadAllRecordsBeforeCompare(){
		this->readAllRecordsBeforeCompare = true;
	}
	virtual void Init();
	string getResultVector() {
		return resultVector.str();
	}
	void setIntraReadersCacheSize(int intraReaders) {
		this->intraReaders = intraReaders;
	}
	int getIntraReadersCacheSize() {
		return intraReaders;
	}
	bool getOpenWeightTable() {
		return this->openWeightTable;
	}
	static const int MaxPathSize = 255;
	Project(string configFile, bool openWeightTable)  {
		output = NULL;
		classifier = NULL;
		cache = NULL;
		this->openWeightTable = openWeightTable;
		config = configFile;
		loaded = false;
		taskType = DEDUPLICATION;
	}
	virtual ~Project();
	///
	Classifier *getClassifier() {
		return classifier;
	}
	void setClassifier(Classifier *c) {
		this->classifier = c;
	}

	void setCache(Cache<unsigned int, Record*> *cache, Cache<unsigned int,
			Record*> *cache2) {
		this->cache = cache;
		this->cache2 = cache2;
	}
	Cache<unsigned int, Record*> *getCache() {
		return cache;
	}
	Cache<unsigned int, Record*> *getCache2() {
		return cache2;
	}
	string getConfig() {
		return config;
	}
	string getName() {
		return name;
	}
	void setName(string name_) {
		name = name_;
	}
	void setTmpDir(string dir){
		this->tmpDir = dir;
	}
	string getTmpDir(){
		return this->tmpDir;
	}
	TaskType getTaskType() {
		return taskType;
	}
	void setTaskType(TaskType type_) {
		taskType = type_;
	}
	DataSource * getCurrentDataSource() {
		return currentDataSource;
	}
	Block *getCurrentBlock() {
		return currentBlock;
	}
	Block *getCurrentDeterministic() {
		return currentDeterministic;
	}
	Block *getCurrentExclude() {
		return currentExclude;
	}
	virtual DataSource * getDataSource(string id) {
		if (dataSources.find(id) != dataSources.end()) {
			return dataSources[id];
		} else {
			throw FerapardaException("Datasource id=" + id + " not found");
		}
	}
	///
	void AddDataSource(DataSource *ds) {
		dataSources[ds->getId()] = ds;
		currentDataSource = ds;
	}
	;
	void AddBlock(Block *block) {
		blocks.push_back(block);
		currentBlock = block;
	}
	void AddDeterministic(Block *block) {
		deterministic.push_back(block);
		currentDeterministic = block;
	}
	void AddExclude(Block *block) {
		exclude.push_back(block);
		currentExclude = block;
	}
	void AddComparator(Comparator *comparator) {
		comparators.push_back(comparator);
	}
	Comparators GetComparators(){
		return comparators;
	}
	bool Validate();
	//float prj_classify(project_t *, record_t, record_t);
	void Execute();
	void ListDataSources();
	void ListBlocking();
	void ListCaching();
	void PrintConfiguration();
	void AddOutput(Output *output) {
		this->output = output;
	}
	Output *getOutput() {
		return output;
	}
	float Compare(Record*, Record*);
	int GetTotalOfBlocks(){
		return this->blocks.size();
	}
private:
	bool loaded;
	bool openWeightTable;
	std::stringstream resultVector;
	string name; /**< Project name*/
	string tmpDir;
	TaskType taskType;

	DataSources dataSources;
	DataSource *currentDataSource; /**Last added datasource*/

	Output *output;

	Blocks blocks;
	Block *currentBlock;

	Blocks deterministic;
	Block *currentDeterministic;

	Blocks exclude;
	Block *currentExclude;

	Comparators comparators;
	Classifier *classifier;
	Cache<unsigned int, Record*> *cache;
	Cache<unsigned int, Record*> *cache2;
	string config;

	static void Start(void *data, const char *el, const char **attr);
	static void End(void *data, const char *el);
	int intraReaders;

	bool readAllRecordsBeforeCompare;
};
}
#endif
