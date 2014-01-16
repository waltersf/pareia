#define _FILE_OFFSET_BITS 64

#include "cache.h"
#include "datasource.h"
#include "ferapardaexception.h"
#include <string>
#include <iostream>
#include <sstream>
//FIXME: Use C++ class!!!
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <cerrno>
#include <cstring>

using namespace std;
using namespace Feraparda;


/**
 * Record class members
 */
Record::Record(int id, char *data, FieldsInfo *info, int size) {
	this->info = info;
	this->fd = -1;
	this->size = size;
	this->id = id;
	int fieldCounter = info->size();

	this->fieldsValue = new vector<string>(fieldCounter);

	FieldsInfo::iterator it;

	FieldInfo *field0 = (*info->begin()).second;

	if (field0->isUseSeparator()) {
		this->Unpack(data, field0->getReadMask(), field0->getSeparatorSize());

	} else {
		for (it = info->begin(); it != info->end(); it++) {
			FieldInfo *field = (*it).second;
			int fieldSize = field->getSize();
			int dataSize = 0;
			int offset = field->getOffset();
			char fieldValue[fieldSize];
			int y = 0;
			// Ignores if the first char is space
			if (!isspace((char) data[offset])) {
				fieldValue[y++] = (char) data[offset];
				dataSize++;
			}

			for (int i = 1; i < fieldSize; i++) {
				if (isspace((char) data[offset + i])) {
					while (i != (fieldSize - 1) && isspace((char) data[offset
							+ (i + 1)])) {
						i++;
					}
				}
				fieldValue[y++] = (char) data[offset + i];
				dataSize++;
			}
			string aux(fieldValue, dataSize);
			string value = aux;
			//Remove spaces
			string::size_type pos = value.find_last_not_of(' ');
			if (pos != string::npos) {
				value.erase(pos + 1);
				pos = value.find_first_not_of(' ');
				if (pos != string::npos)
					value.erase(0, pos);
			} else {
				value.erase(value.begin(), value.end());
			}
			(*this->fieldsValue)[field->getOrder()] = value;
		}
	}
}
string Record::getField(int index) {
	return this->fieldsValue->at(index);
}
string Record::getField(string name) {
	FieldInfo *fi = (*info)[name];
	if (fi == NULL) {
		throw FerapardaException("Invalid field name : " + name);
	}
	return this->fieldsValue->at(fi->getOrder());
}
/***************************************
 * DataSource class members.
 */
void DataSource::ListFields(string padding) {
	FieldsInfo::iterator i;
	for (i = fieldsInfo.begin(); i != fieldsInfo.end(); ++i) {
		FieldInfo *info = (*i).second;
		cout << padding << "[Name]: " << info->getName() << " [Type]: "
				<< info->getType() << " [Size]: " << info->getSize() << endl;
	}
}
Record * DataSource::GetRecordByDatabaseId(string id, bool keyIsInteger){
	unsigned long end = getRecordCount() - 1;
	unsigned long pos = end / 2;
	unsigned long start = 0;

	while (end != start){
		Record *r = GetRecord(pos, false);
		string fieldValue = r->getField("id");
		if (keyIsInteger){
			int idInt = atoi(id.c_str());
			int fieldValueInt = atoi(fieldValue.c_str());

			if (fieldValueInt == idInt) {
				if (cache != NULL) {
					cache->Put(atoi(id.c_str()), r);
				}
				return r;
			} else if (fieldValueInt > idInt){
				end = pos;
			} else {
				start = pos;
			}
			pos = (end - start) / 2 + start;
		} else {
			if (fieldValue == id) {
				if (cache != NULL) {
					cache->Put(atoi(id.c_str()), r);
				}
				return r;
			} else if (fieldValue > id){
				end = pos;
			} else {
				start = pos;
			}
			pos = (end - start) / 2 + start;
		}
		delete r;
	}
	return NULL;
}

/***************************************
 * FixedLengthRecordDataSource class members.
 */

FixedLengthRecordDataSource::FixedLengthRecordDataSource(string id,
		string fileName, int recordLength_) :
			FileDataSource(id, fileName) {

	this->fileDescriptor = NULL;
	this->fileNumber = 0;
	this->recordLength = recordLength_;
	this->loaded = false;
	this->currentOffset = 0;
}
string Record::operator[](const string s) {
	return this->getField(s);
}
/**
 * Packs the record in a string where fields are separeted by tabs.
 */
string Record::Pack() {
	string value;
	int fieldCounter = info->size();

	for (int i = 0; i< fieldCounter - 1; ++i){
		value.append(this->fieldsValue->at(i)).append("\t");
	}
	value.append(this->fieldsValue->at(fieldCounter - 1));
	return value;
}
/**
 * Unpacks the record in a string where fields are separeted by tabs.
 */
void Record::Unpack(char *data, string sep, int separatorSize){
	char buffer[1024];
	int fieldCounter = info->size();
	int pos = 0;
	bool ok;

	//FIXME: Não funciona se especificar o separador como tab
	const char *readMask = "%[^\t]\t"; //string("%[^").append(sep).append("]").append(sep).c_str();
	int maskSize = separatorSize;

	for (int i = 0; i < fieldCounter; i++) {
		ok = (sscanf(&(data[pos]), readMask, buffer) > 0);
		if (ok) {
			pos += maskSize + strlen(buffer);
			(*this->fieldsValue)[i] = string(buffer);
		} else {
			pos += maskSize;
			(*this->fieldsValue)[i] = "";
		}
	}
}
char *FixedLengthRecordDataSource::GetLine(int id) {
	unsigned long long pos = recordLength * id;
	//TODO carega 2*tamanho para a cache
	if (fseeko64(this->fileDescriptor, pos, SEEK_SET) < 0) {
		throw FerapardaException("Error fseeking byte in datafile" + string(strerror(errno)));
	}
	char *data = new char[recordLength];
	int ret = fread(data, 1, recordLength, this->fileDescriptor);
	if (ret < 0) {
		delete [] data;
		throw FerapardaException("Error reading datafile");
	}
	return data;
}
void FixedLengthRecordDataSource::setPosition(int pos) {
	this->position = pos;
}
Record *FixedLengthRecordDataSource::GetNextRecord() {
	return GetRecord(this->position);
}
Record *FixedLengthRecordDataSource::GetRecord(int id) {
	return GetRecord(id, true);
}
Record *FixedLengthRecordDataSource::GetRecord(int id, bool useCache) {
	/*
	 struct timeval end;
	 struct timeval start;
	 gettimeofday(&start, NULL);
	 */

	/*if (logger->IsLevelEnabled(LOGSTAT)){
	 LOGFPSTAT(logger, "Read record " + Util::ToString(id));
	 }*/
	ostringstream out;
	/*if (!loaded) {
	 out << "Datasource not loaded";
	 throw FerapardaException(out.str());
	 }*/
	if (!HasRecord(id)) {
		out << "Invalid record number: " << id << " (max: " << recordCount
				<< ")";
		throw FerapardaException(out.str());
	}
	//records begin from 0
	Record * record = CACHE_MISS;
	if (cache != NULL && useCache) {
		record = (Record *) cache->Get(id);
	}
	if (record == CACHE_MISS) {
		char *data = this->GetLine(id);
		record = new Record(id, data , &(this->fieldsInfo),
				recordLength);
		delete[] data;

		/*for(int i = 1; i <= 3 && (id + i) < this->recordCount; i++){
		 int newId = id + id;
		 cache->Put(newId,
		 new Record(newId,
		 (char*) (&mappedContent[0] + newId * recordLength),
		 this->fieldsInfo, recordLength));
		 }*/
		if (cache != NULL && useCache) {
			cache->Put(id, record);
		}
	}
	return record;
}
void FixedLengthRecordDataSource::Load(Cache<unsigned int, Record*> *cache) {
	ostringstream out;
	struct stat st;
	int fstatus;

	if (loaded) {
		out << "Datasource already loaded! " << loaded;
		throw FerapardaException(out.str());
		//exit(-1);
	}
	loaded = true;

	this->fileDescriptor = fopen(fileName.c_str(), "r");
	if (fileDescriptor == NULL) {
		out << "Error opening database file (fopen) " << fileName << " Cause: "
				<< strerror(errno);
		throw FerapardaException(out.str());
	}
	fstatus = stat(fileName.c_str(), &st);

	if (fstatus != 0) {
		out << "Error fstat failed at file " << fileName << " Cause: " << strerror(
				errno);
		throw FerapardaException(out.str());
	}
	recordCount = st.st_size / recordLength;
	this->setCache(cache);
}

float FieldInfo::getWeightOfTerm(string term) {
	float weight = 0;
	map<string, float>::iterator iter = weightTable.find(term);
	if (iter != weightTable.end()) {
		weight = iter->second;
	} else {
		weight = this->defaultWeight;
	}
	//cout << "->" << term<< "<-" << "  " << weight << endl;
	return weight;
}
FieldInfo::FieldInfo(string n, FieldType t, int o, int s, bool useSeparator, int order) {
	name = n;
	type = t;
	offset = o;
	size = s;
	this->order = order;
	this->useSeparator = useSeparator;
	separator = "\t";
}

//
/**
 * @param wt Weight table for the field.
 */
FieldInfo::FieldInfo(string n, FieldType t, int o, int s, bool useSeparator, int order, string weightTable, float dw) {
	name = n;
	type = t;
	offset = o;
	size = s;
	this->order = order;
	this->useSeparator = useSeparator;
	separator = "\t";

	this->defaultWeight = dw;

	//Open the weight table. The weight table has the following format: frequency, inverse of frequency, log(inverse freq), key
	if (weightTable.size() > 0) {
		string key;
		string freq;
		string invfreq;
		string loginvfreq;
		float weight;
		float max_weight = 0;

		try {
			ifstream in;
			//Open the file with the weight table
			in.open(weightTable.c_str());
			if (!in.is_open()) {
				ostringstream msg;
				msg << "Could not open weight table file " << weightTable << endl;
				cout << msg.str() << flush;
				throw FerapardaException(msg.str());
			}

			while (!in.eof()) {
				getline(in, freq, '\t'); //
				getline(in, invfreq, '\t'); // weight1
				getline(in, loginvfreq, '\t'); // weight2
				getline(in, key, '\n'); //key
				weight = atof(invfreq.c_str());

				if (weight > max_weight)
					max_weight = weight;
				this->weightTable[key] = weight;
			}
			in.close();
		} catch (ifstream::failure e) {
			throw FerapardaException("Error opening weight table for field "+ n);
		}
	}
}
/**
 * Returns the boundaries of the data source (first and last record) read by
 * filter instance. Useful when performing experiments and the entire data source
 * is replicated among the nodes, but only part of it should be read.
 */
DataSourceBoundaries DataSource::GetBoundaries(int instances, int rank) {
	int total = this->getRecordCount();
	//how many instances will process 1 more record
	int work = total / instances;
	int reminder = total % instances;

	DataSourceBoundaries bounds;

	bounds.start = rank * work; //(1 + work) * rank - (reminder < rank ? 1 : 0);
	bounds.end = (1 + rank) * work - 1;//min(bounds.start + work - (reminder == rank ? 1 : 0), total - 1);
	if (1 + rank == instances) {
		//last instance get more work
		bounds.end += reminder;
	}

	return bounds;
}
/***************************************
 * DelimitedRecordDataSource class members
 */
void DelimitedRecordDataSource::Index(){
	ostringstream out;

	this->fileDescriptor = fopen(fileName.c_str(), "r");
	if (this->fileDescriptor == NULL) {
			out << "Error opening database file (fopen) " << this->fileName
					<< ". Cause: " << strerror(errno);
			throw FerapardaException(out.str());
	}
	FILE *index = fopen((fileName + ".inx").c_str(), "w");
	if (index == NULL) {
		out << "Error writing database index file (fopen) " << fileName
				<< ".inx. Cause: " << strerror(errno) << ". This file is required when using delimited data sources.";
		throw FerapardaException(out.str());
	}
	unsigned long long position = 0;
	this->memoryIndex.push_back(position);

	const char *readMask = ("%[^" + this->rowSeparator + "]"+ this->rowSeparator).c_str();
	char buffer[MAX_RECORD_SIZE];
	int recordNumber = 0;
	int retIo;
	while (!feof(this->fileDescriptor)){
		retIo = fscanf(this->fileDescriptor, readMask, buffer);
		//cout << position << " " << buffer << endl;
		recordNumber ++;
		//position += strlen(buffer) + sizeOfSeparator; // because delimiter
		position = ftello64(this->fileDescriptor);
		if ((recordNumber % this->indexPageSize) == 0){
			//cout << "Informacao " << recordNumber << " "<< position << endl;
			this->memoryIndex.push_back(position);
		}
		if ((recordNumber % 1000000) == 0){
			cout << "Indexed " << (recordNumber / 1000000) << "M records up to now." << endl << flush;
		}
	}
	fclose(this->fileDescriptor);
	//store the index
	this->recordCount = recordNumber;
	fprintf(index, "%d\n", this->recordCount);
	vector<unsigned long long>::const_iterator it;
	for (it = this->memoryIndex.begin(); it != this->memoryIndex.end(); ++it) {
		fprintf(index, "%lld\n", *it);
	}
	fclose(index);

	/*this->Load(NULL);
	for(int i = 0; i < 20; i++){//this->recordCount; i++){
		Record *r = this->GetRecord(i);
		//cout << ">>>>>>>>>> " << r->getField("nome") << endl;
	}*/
}
char * DelimitedRecordDataSource::GetLine(int id) {
	unsigned long long pageId = this->memoryIndex[(unsigned long long) (id / this->indexPageSize)];

	//Move to the begining of page
	if (fseeko64(this->fileDescriptor, pageId, SEEK_SET) < 0) {
		throw FerapardaException("Error fseeking byte in datafile");
	}
	//Move to the record
	string aux = "%[^" + this->rowSeparator + "]"+ this->rowSeparator;
	const char *readMask = aux.c_str();
	int recordsToMove = (id % this->indexPageSize) + 1;

	bool useNewLineAsSeparator = (this->rowSeparator == "\n");

	char *buffer = new char[MAX_RECORD_SIZE];
	while (!feof(this->fileDescriptor) && recordsToMove > 0){
		if (useNewLineAsSeparator){
			if (fgets(buffer, MAX_RECORD_SIZE, this->fileDescriptor) == NULL){
				delete[] buffer;
				throw FerapardaException("Error reading datafile");
			}
		} else if (fscanf(this->fileDescriptor, readMask, buffer) < 0){
			delete[] buffer;
			throw FerapardaException("Error reading datafile");
		}
		recordsToMove --;
	}
	//fgets() also reads \n
	int len = strlen(buffer);
	if(buffer[len-1] == '\n' ){
		buffer[len-1] = 0;
	}
	return buffer;
}
void DelimitedRecordDataSource::setPosition(int pos) {
	this->position = pos;
	this->recordsToMove = (this->position % this->indexPageSize) + 1; // total of records to read until next record.
	unsigned long long pageId = this->memoryIndex[(unsigned long long) (this->position/ this->indexPageSize)];
	//Move to the begining of page
	if (fseeko64(this->fileDescriptor, pageId, SEEK_SET) < 0) {
		throw FerapardaException("Error fseeking byte in datafile");
	}

}
Record * DelimitedRecordDataSource::GetNextRecord() {
	//Move to the record
	string aux = "%[^" + this->rowSeparator + "]"+ this->rowSeparator;
	const char *readMask = aux.c_str();

	bool useNewLineAsSeparator = (this->rowSeparator == "\n");

	char *buffer = new char[MAX_RECORD_SIZE];
	do {
		if (useNewLineAsSeparator){
			if (fgets(buffer, MAX_RECORD_SIZE, this->fileDescriptor) == NULL){
				delete[] buffer;
				throw FerapardaException("Error reading datafile");
			}
		} else if (fscanf(this->fileDescriptor, readMask, buffer) < 0){
			delete[] buffer;
			throw FerapardaException("Error reading datafile");
		}
		//if (this->recordsToMove > 0){
			this->recordsToMove --;
		//}
	} while (!feof(this->fileDescriptor) && this->recordsToMove > 0);
	//fgets() also reads \n
	int len = strlen(buffer);
	if(buffer[len-1] == '\n' ){
		buffer[len-1] = 0;
	}
	Record *record = new Record(this->position, buffer, &(this->fieldsInfo), strlen(buffer) + 1);
	//cout << "DATA: move " << recordsToMove << " id "  << record->getField("id") << " pos  " << this->position << " " << buffer << endl;
	if (cache != NULL) {
		cache->Put(this->position, record);
	}
	this->position ++;
	delete[] buffer;
	return record;
}
Record * DelimitedRecordDataSource::GetRecord(int id) {
	return GetRecord(id, true);
}
Record * DelimitedRecordDataSource::GetRecord(int id, bool useCache) {

	ostringstream out;
	if (!HasRecord(id)) {
		out << "Invalid record number: " << id << " (max: " << recordCount
				<< ")";
		throw FerapardaException(out.str());
	}
	//records begin from 0
	Record * record = CACHE_MISS;
	if (cache != NULL && useCache) {
		record = (Record *) cache->Get(id);
	}
	if (record == CACHE_MISS) {

		//cout << "Linha " << id << " " <<  buffer << endl;
		//Note: strlen + 1 to include the string delimiter
		char *buffer = this->GetLine(id);
		record = new Record(id, buffer, &(this->fieldsInfo), strlen(buffer) + 1);
		delete[] buffer;
		if (cache != NULL && useCache) {
			cache->Put(id, record);
		}
	}
	return record;
}
/**
 * Loads and configure the data source. Notice that this kind of data source
 * uses 2 files: one for data and another for indexing. The index file name is
 * formed by the data file name append by the string inx.
 */
void DelimitedRecordDataSource::Load(Cache<unsigned int, Record*> *cache) {

	ostringstream out;

	if (this->loaded) {
		out << "Datasource already loaded! " << loaded;
		throw FerapardaException(out.str());
	}

	this->fileDescriptor = fopen(fileName.c_str(), "r");
	if (fileDescriptor == NULL) {
		out << "Error opening database file (fopen) " << fileName
				<< ". Cause: " << strerror(errno);
		throw FerapardaException(out.str());
	}
	FILE *index = fopen((fileName + ".inx").c_str(), "r");
	if (index == NULL) {
		out << "Error opening database index file (fopen) " << fileName
				<< ".inx. Cause: " << strerror(errno) << ". This file is required when using delimited data sources.";
		throw FerapardaException(out.str());
	}
	//First line contains the total of records.
	if (!feof(index)){
		int retIo = fscanf(index,"%d",&this->recordCount);
		if (retIo < 0){
			out << "Error seeking database file (fscanf) " << fileName
				<< ".inx. Cause: " << strerror(errno) << ". This file is required when using delimited data sources.";
			throw FerapardaException(out.str());
		}
	}
	unsigned long long _position = 0;
	while (!feof(index)){
		int retIo = fscanf(index,"%lld",&_position);
		if (retIo < 0 && !feof(index)){
			out << "Error seeking position in database index file (fscanf) " << fileName
				<< ".inx. Cause: " << strerror(errno) << ". This file is required when using delimited data sources.";
			throw FerapardaException(out.str());
		}
		this->memoryIndex.push_back(_position);
	}
	fclose(index);

	this->setCache(cache);
	this->loaded = true;
}
