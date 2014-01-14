#ifndef DATASOURCE_H_
#define DATASOURCE_H_
#include "cache.h"
#include "ferapardaexception.h"
#include "stlutil.h"
#include "logging.h"
#include "stringutil.h"
#include "constants.h"
#include <map>
#include <string>
#include <cassert>
#include <iostream>
#include <ext/hash_map>
using namespace std;
namespace Feraparda {
enum FieldType {
	FIELD_INTEGER,
	FIELD_STRING,
	FIELD_DATE_YMD,
	FIELD_DATE_MDY,
	FIELD_DATE_DMY,
	FIELD_NUMERIC,
	FIELD_DOUBLE
};

/** Limits of data source */
typedef struct {
	int start;
	int end;
} DataSourceBoundaries;

/**
 * Store the meta information about a datasource field.
 */
class FieldInfo {
private:
	string name;
	FieldType type;
	int size;
	int order;
	bool useSeparator;
	string separator;
	int offset;
	map<string, float> weightTable;
	float defaultWeight;
	string readMask;
	bool openWeightTable;
public:
	float getDefaultWeight(){
		return defaultWeight;
	}
	string getReadMask(){
		return readMask;
	}
	void setSeparator(string sep){
		this->separator = sep;
		this->readMask = string("%[^").append(sep).append("]").append(sep);
	}
	void setWeightTable(bool v) {
		openWeightTable = v;
	}
	float getWeightOfTerm(string term);
	//
	string getName() {
		return name;
	}
	FieldType getType() {
		return type;
	}
	int getOffset() {
		return offset;
	}
	int getSize() {
		return size;
	}
	bool isUseSeparator(){
		return useSeparator;
	}
	int getOrder(){
		return order;
	}
	int getSeparatorSize(){
		return separator.size();
	}
	//
	/**
	 * @param wt Weight table for the field.
	 */
	FieldInfo(string n, FieldType t, int o, int s, bool useSeparator, int order, string wt, float dw);
	FieldInfo(string n, FieldType t, int o, int s, bool useSeparator, int order);
};
}
typedef hash_map<string, Feraparda::FieldInfo *, stu_comparator_t, stu_eqstr_t>
		FieldsInfo;
namespace Feraparda {
/*
 * Represents a record.
 */
class Record {
public:
	Record *Clone() {
		return new Record(this->id, this->fieldsValue, this->info, this->size);
	}
	Record(int id, vector<string> *data, FieldsInfo *info, int size){
		this->info = info;
		this->fd = -1;
		this->size = size;
		this->id = id;
		this->fieldsValue = new vector<string>(info->size(), "");
	}
	Record(int id, char *data, FieldsInfo *info){
		this->info = info;
		this->fd = -1;
		this->size = size;
		this->id = id;
		this->fieldsValue = new vector<string>(info->size(), "");
		this->Unpack(data);
	}
	string Pack();
	void Unpack(char *, string, int);
	void Unpack(char *data) {
		Unpack(data, "\t", sizeof(char));
	}
	int getId() {
		return id;
	}
	float getWeight(string field, string value) {
		FieldInfo *fi = (*info)[field];
		return fi->getWeightOfTerm(value);
	}
	float getDefaultWeight(string field){
		FieldInfo *fi = (*info)[field];
		return fi->getDefaultWeight();
	}
	virtual string getField(string name);
	virtual string getField(int index);
	virtual ~Record() {
		if (fieldsValue != NULL){
			delete fieldsValue;
		}
	}
	Record(int id, char *data, FieldsInfo *info, int size);

	vector<string> getFieldNames(){
		vector<string> result;
		FieldsInfo::iterator it;

		for(it = info->begin(); it != info->end(); ++it){
			result.push_back((*it).second->getName());
		}
		return result;
	}
	string operator[](const string s);
protected:
	Record() {
		this->fieldsValue = NULL;
	}
private:
	vector<string> *fieldsValue;
	int fd;
	int size;
	FieldsInfo *info;
	int id;
};

class DataSource {
public:
	DataSource(string id) {
		logger = LogFactory::GetLogger("datasource");
		this->cache = NULL;
		this->id = id;
		this->position = 0;
	}
	virtual ~DataSource() {
		FieldsInfo::iterator i;
		for (i = fieldsInfo.begin(); i != fieldsInfo.end(); ++i) {
			delete (*i).second;
		}
	}
	//Setters and getters
	string getId() {
		return id;
	}
	void setCache(Cache<unsigned int, Record*> *cache) {
		this->cache = cache;
	}
	virtual void setPosition(int position)=0;
	virtual Record * GetNextRecord() = 0;

	virtual Record * GetRecord(int number) = 0;
	virtual Record * GetRecord(int number, bool useCache) = 0;
	virtual char * GetLine(int number) = 0;
	virtual void Load(Cache<unsigned int, Record*> *cache) = 0;

	/**
	 * Performs a binary search in dataset. The dataset MUST be sorted by database key.
	 */
	virtual Record *GetRecordByDatabaseId(string id, bool keyIsInteger);
	virtual int getRecordCount() {
		return recordCount;
	}
	virtual void Index(){
		return;
	}
	virtual bool HasRecord(int id) {
		return (id < recordCount && id >= 0);
	}
	virtual int getRecordLength() = 0;
	virtual DataSourceBoundaries GetBoundaries(int, int);

	virtual void AddField(string name, FieldType type, int size) {
		FieldInfo *fi = new FieldInfo(name, type, currentOffset, size, false, fieldsInfo.size());
		currentOffset += size;
		fieldsInfo[name] = fi;
	}
	virtual void AddField(string name, FieldType type, int size, string ft, float dw) {
		FieldInfo *fi = new FieldInfo(name, type, currentOffset, size, false, fieldsInfo.size(), ft, dw);
		currentOffset += size;
		fieldsInfo[name] = fi;
	}
	void ListFields(string padding);

	FieldInfo *getFieldInfo(string name) {
		if (fieldsInfo.find(name) != fieldsInfo.end()) {
			return fieldsInfo[name];
		} else {
			return NULL;
		}
	}
	FieldsInfo *getFieldsInfo() {
		return &fieldsInfo;
	}
	int getFieldIndex(string name){
		FieldsInfo::iterator it;
		for(it = fieldsInfo.begin(); it != fieldsInfo.end(); ++it){
			if (name == (*it).second->getName()){
				return (*it).second->getOrder();
			}
		}
		throw FerapardaException("Field " + name + " not found.");
	}
	//FIXME: Duplicated!
	vector<string> getFieldNames(){
		vector<string> result (fieldsInfo.size());
		FieldsInfo::iterator it;
		for(it = fieldsInfo.begin(); it != fieldsInfo.end(); ++it){
			result[(*it).second->getOrder()] = (*it).second->getName();
		}
		return result;
	}
protected:
	Cache<unsigned int, Record*> *cache;
	bool loaded;
	string id;
	FieldsInfo fieldsInfo;
	Logger *logger;
	int currentOffset;
	int recordCount;
	int position; //Used for sequential read.
};

class FileDataSource: public DataSource {
public:
	FileDataSource(string id, string fileName) :
		DataSource(id) {
		this->fileName = fileName;
	}
	virtual ~FileDataSource() {
		if (loaded) {
			fclose( fileDescriptor);
		}
	}
	string getFileName() {
		return fileName;
	}
protected:
	string fileName;
	FILE * fileDescriptor;
	int fileNumber;
};
/**
 *
 */
class FixedLengthRecordDataSource: public FileDataSource {
public:
	FixedLengthRecordDataSource(string id, string fileName,
			int recordLength_);

	void setPosition(int position);
	Record * GetNextRecord();
	virtual Record * GetRecord(int number);
	virtual Record * GetRecord(int number, bool useCache);
	virtual char * GetLine(int number);
	virtual void Load(Cache<unsigned int, Record*> *cache);
	int getRecordCount() {
		return recordCount;
	}
	int getRecordLength() {
		return recordLength;
	}
	void setRecordLength(int v) {
		recordLength = v;
	}
protected:
	int recordLength;
};

class DelimitedRecordDataSource: public FileDataSource {
public:
	DelimitedRecordDataSource(string file);
	DelimitedRecordDataSource(string id, string fileName,
			string fieldSeparator, string rowSeparator) :
		FileDataSource(id, fileName) {
		this-> loaded = false;
		this->fieldSeparator = fieldSeparator;
		this->rowSeparator = rowSeparator;
		this->indexPageSize = 100; //TODO: should be a parameter
		this->recordsToMove = 0;
	}
	void setPosition(int position);
	Record * GetNextRecord();
	virtual Record * GetRecord(int number);
	virtual Record * GetRecord(int number, bool useCache);
	virtual char * GetLine(int number);
	virtual void Load(Cache<unsigned int, Record*> *cache);
	virtual int getRecordCount() {
		return recordCount;
	}
	int getRecordLength(){
		return 0;
	}
	void setFieldSeparator(string delim) {
		this->fieldSeparator = delim;
	}
	string getFieldSeparator() {
		return this->fieldSeparator;
	}
	void setRowSeparator(string delim) {
		this->rowSeparator = delim;
	}
	string getRowSeparator() {
		return this->rowSeparator;
	}
	virtual void AddField(string name, FieldType type, int size) {
		FieldInfo *fi = new FieldInfo(name, type, currentOffset, size, true, fieldsInfo.size());
		currentOffset += size;
		fieldsInfo[name] = fi;
		fi->setSeparator(this->fieldSeparator);
	}
	virtual void AddField(string name, FieldType type, int size, string ft, float dw) {
		FieldInfo *fi = new FieldInfo(name, type, currentOffset, size, true,fieldsInfo.size(), ft, dw);
		currentOffset += size;
		fieldsInfo[name] = fi;
		fi->setSeparator(this->fieldSeparator);
	}
	virtual void Index();
protected:
	string fieldSeparator;
	string rowSeparator;
	int indexPageSize;
	vector<unsigned long long> memoryIndex;
	int recordsToMove;
}; //DelimitedRecordDataSource
} //namespace
#endif
