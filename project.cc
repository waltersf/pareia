#include "project.h"
#include "datasource.h"
#include "blocking.h"
#include "cache.h"
#include "classifier.h"
#include "comparator.h"
#include "encode.h"
#include "ferapardaexception.h"
#include "messagesfp.h"
#include "output.h"
extern "C" {
#include <expat.h>
#include <stdio.h>
#include <stdlib.h>
static char queue[1000];
static int queuesize = 0;
}
#include <fstream>
#include <iostream>
#include <iomanip>

using namespace std;
using namespace Feraparda;
Project::~Project() {
	DataSources::iterator i;
	for (i=dataSources.begin(); i!= dataSources.end(); ++i) {
		delete (*i).second;
	}

	for (Comparators::size_type s = 0; s < comparators.size(); s++) {
		delete comparators[s];
	}
	for (Blocks::size_type s = 0; s < blocks.size(); s ++) {
		delete blocks[s];
	}
	for (Blocks::size_type s = 0; s < deterministic.size(); s ++) {
		delete deterministic[s];
	}
	for (Blocks::size_type s = 0; s < exclude.size(); s ++) {
			delete exclude[s];
		}
	if (output != NULL) {
		delete output;
	}
	if (classifier != NULL){
		delete classifier;
	}
	if (cache != NULL){
		delete cache;
	}
	if (cache2 != NULL){
		delete cache2;
	}
}
bool Project::Validate() {
	return true;
}
void Project::Execute() {
}
void Project::Init() {
	if (this->loaded){
		throw FerapardaException("Project already initiated");
	}
	currentDataSource = NULL;
	XML_Parser p = XML_ParserCreate(NULL);
	if (!p) {
		fprintf(stderr, "Couldn't allocate memory for parser\n");
	}
	XML_SetUserData(p, (void *) this);
	XML_SetElementHandler(p, Start, End);
	int CHUNK_SIZE = 8192;
	//FIXME C++
	FILE * in = fopen(getConfig().c_str(), "r");
	if (!in) {
		cerr << "Unable to open file " << getConfig()<< endl;
		exit(1); // call system to stop
	}
	int len;
	char* buff = (char*) malloc(8193 * sizeof(char));
	memset(buff, 1, 8192);
	for (;;) {
		if (!buff) {
			cerr << "Ran out of memory for parse buffer" << endl;
			exit(-1);
		}
		len = fread(buff, 1, CHUNK_SIZE, in);
		int done = feof(in);
		if (!XML_Parse(p, buff, len, done)) {
			cerr << "Parse error (project file) at line " << XML_GetCurrentLineNumber(p)
					<< " : " << XML_ErrorString(XML_GetErrorCode(p)) << endl;
			exit(-1);
		}
		if (done)
			break;
	}
	XML_ParserFree(p);
	fclose(in);
	free(buff);
	this->loaded = true;
}

void Project::ListDataSources() {
	DataSources::iterator i;
	string padding = "\t";
	cout << padding << "* DataSources " <<endl;
	for (i=dataSources.begin(); i!= dataSources.end(); ++i) {
		DataSource *ds = (*i).second;
		cout << padding << "[Datasource]: " << ds->getId() << endl;
		ds->ListFields(padding + padding);
	}
}

void Project::ListBlocking() {
	string padding = "\t";
	cout << padding << "* Blocking" <<endl;
	for (Blocks::size_type s = 0; s < blocks.size(); s ++) {
		blocks[s]->ListConjunctions(padding);
	}
}

static int evaluate_common_attributes(Comparator *comparator,
		const char **attrs, int i) {
	int retvalue = 0;
	if (strcmp(attrs[i], (const char *) "m") == 0) {
		comparator->setM(atof((char *)attrs[i + 1]));
		retvalue = 1;
	} else if (strcmp(attrs[i], (const char *) "u") == 0) {
		comparator->setU(atof((char *)attrs[i + 1]));
		retvalue = 1;
	} else if (strcmp(attrs[i], (const char *) "missing") == 0) {
		comparator->setMissing(atof((char *)attrs[i + 1]));
		retvalue = 1;
	} else if (strcmp(attrs[i], (const char *) "field1") == 0) {
		comparator->setField1(attrs[i + 1]);
		retvalue = 1;
	} else if (strcmp(attrs[i], (const char *) "field2") == 0) {
		comparator->setField2(attrs[i + 1]);
		retvalue = 1;
	} else if (strcmp(attrs[i], (const char *) "use-weight-table") == 0
				&& strcmp(attrs[i + 1], "true") == 0) {
		comparator->setUseWeightTable(true);
	}
	return retvalue;
}

void Project::Start(void *data, const char *el, const char **attrs) {
	Project *project = (Project *) data;
	string tmpDir = "/tmp";
	int i = 0;
	strcat(queue, "/");
	strcat(queue, (char *)el);
	queuesize += strlen((char *)el) + 1;
	//printf("Queue start: %s\n", queue);
	if (strcmp(queue, "/project") == 0) {
		i = 0;
		while (attrs && attrs[i]) {
			if (strcmp(attrs[i], (const char *) "name") == 0) {
				project->setName(attrs[i + 1]);
			} else if (strcmp(attrs[i], (const char *) "task") == 0) {
				if (strcmp(attrs[i + 1], (const char *) "linkage") == 0) {
					project->setTaskType(LINKAGE);
				} else if (strcmp(attrs[i + 1], (const char *) "deduplication")
						== 0) {
					project->setTaskType(DEDUPLICATION);
				} else {
					cerr << "Invalid project type: " << attrs[i + 1] << endl;
					exit(-1);
				}
			} else if (strcmp(attrs[i], (const char *) "tmp-dir") == 0) {
				tmpDir = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "version") == 0) {
				if (strcmp(attrs[i + 1], (const char *) "2.0") != 0) {
					throw FerapardaException("This version (2.0) accepts only configuration files with attribute version=\"2.0\"");
				}
			}
			i += 2;
		}
		project->setTmpDir(tmpDir);
		//parse_project(project, attrs);
	} else if (strcmp(queue, "/project/data-sources/data-source") == 0) {
		int i =0;
		string id = "";
		string file = "";
		string length = "";
		string fieldSep = ";";
		string rowSep = "\n";
		string type = "fixed";

		while (attrs && attrs[i]) {
			if (strcmp(attrs[i], (const char *) "id") == 0) {
				id = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "file") == 0) {
				file = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "record-length") == 0) {
				length = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "field-separator") == 0) {
				fieldSep = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "row-separator") == 0) {
				rowSep = attrs[i + 1];
			} else if (strcmp(attrs[i], (const char *) "type") == 0) {
				type = attrs[i + 1];
			}
			i += 2;
		}
		if (id != "" && file != "" && ((length != "" && type == "fixed") || (type == "delimited"))) {
			DataSource *ds;
			if (type == "delimited"){
				if (fieldSep == "\\t"){
					fieldSep = "\t";
				}
				if (rowSep == "\\n"){
					rowSep = "\n";
				}
				ds = new DelimitedRecordDataSource(id, file, fieldSep, rowSep);
			} else {
				int recordLength = atoi(length.c_str());
				if (recordLength > MAX_RECORD_SIZE || recordLength <= 0) {
					throw FerapardaException("Maximum record size exceeded or invalid size.");
				}
				ds = new FixedLengthRecordDataSource(id, file, recordLength);
			}
			project->AddDataSource(ds);
		} else {
			stringstream ss;
			ss << "Invalid datasource (id " << id << "): missing at least one required attribute." << length;
			throw FerapardaException(ss.str());
		}
	} else if (strcmp(queue, "/project/data-sources/data-source/fields/field")
			== 0) {
		if (attrs) {
			string name;
			string ft("");
			FieldType type = FIELD_STRING;
			int size = 0;
			int i = 0;
			float dw = 0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "name") == 0) {
					name = attrs[i + 1];
				} else if (strcmp(attrs[i], (const char *) "weight-table") == 0) {
					ft = attrs[i + 1];
				} else if (strcmp(attrs[i], (const char *) "default-weight")
						== 0) {
					dw = atof(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "size") == 0) {
					size = atoi(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "type") == 0) {
					if (strcmp(attrs[i + 1], (const char *) "date ymd") == 0) {
						type = FIELD_DATE_YMD;
					} else if (strcmp(attrs[i + 1], (const char *) "date mdy")
							== 0) {
						type = FIELD_DATE_MDY;
					} else if (strcmp(attrs[i + 1], (const char *) "date dmy")
							== 0) {
						type = FIELD_DATE_DMY;
					} else if (strcmp(attrs[i + 1], (const char *) "integer")
							== 0) {
						type = FIELD_INTEGER;
					} else if (strcmp(attrs[i + 1], (const char *) "string")
							== 0) {
						type = FIELD_STRING;
					}
				}
				i += 2;
			}
			if (name != "") {
				if (project->getOpenWeightTable() && ft != "") {
					project->getCurrentDataSource()->AddField(name, type, size,
							ft, dw);
				} else {
					project->getCurrentDataSource()->AddField(name, type, size);
				}
			}
		}
	} else if (strcmp(queue, "/project/blocking") == 0) {
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "type") == 0) {
					if (strcmp(attrs[i + 1], (const char *) "classic") == 0) {
						Block *block = BlockFactory::GetBlock(CLASSICAL);
						project->AddBlock(block);
					} else {
						cerr << "Blocking type " << attrs[i + 1]
								<< " not implemented" << endl;
						exit(-1);
					}
				}
				i += 2;
			}
		}
	} else if (strcmp(queue, "/project/deterministic-linkage") == 0) {
		Block *block = BlockFactory::GetBlock(DETERMINISTIC);
		project->AddDeterministic(block);
	} else if (strcmp(queue, "/project/exclude-pair") == 0) {
		Block *block = BlockFactory::GetBlock(EXCLUDE);
		project->AddExclude(block);
	} else if (strcmp(queue, "/project/blocking/conjunction") == 0) {
		Block *block = project->getCurrentBlock();
		block->AddConjunction(new BlockConjunction());
	} else if (strcmp(queue, "/project/deterministic-linkage/conjunction") == 0) {
		Block *block = project->getCurrentDeterministic();
		block->AddConjunction(new BlockConjunction());
	} else if (strcmp(queue, "/project/exclude-pair/conjunction") == 0) {
		Block *block = project->getCurrentExclude();
		block->AddConjunction(new BlockConjunction());
	} else if (strcmp(queue, "/project/blocking/conjunction/part") == 0
			|| strcmp(queue, "/project/deterministic-linkage/conjunction/part") == 0
			|| strcmp(queue, "/project/exclude-pair/conjunction/part") == 0 ) {

		BlockConjunction *conjunction;
		if (strcmp(queue, "/project/blocking/conjunction/part") == 0) {
			conjunction = project->getCurrentBlock()->getCurrentConjunction();
		} else if(strcmp(queue, "/project/deterministic-linkage/conjunction/part") == 0){
			conjunction = project->getCurrentDeterministic()->getCurrentConjunction();
		} else {
			conjunction = project->getCurrentExclude()->getCurrentConjunction();
		}
		string field = "";
		string transform = "";
		int size = -1;
		int start = -1;
		int maxSize = -1;
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "field-name") == 0) {
					field = attrs[i + 1];
				} else if (strcmp(attrs[i], (const char *) "transform") == 0) {
					transform = attrs[i + 1];
				} else if (strcmp(attrs[i], (const char *) "size") == 0) {
					size = atoi(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "transform-max-size")
						== 0) {
					maxSize = atoi(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "start") == 0) {
					start = atoi(attrs[i + 1]);
				}
				i += 2;
			}
		}
		conjunction->AddExpression(new Expression(field, transform, start, size, maxSize));
	} else if (strcmp(queue, "/project/blocking/conjunction/part/param") == 0 ||
			strcmp(queue, "/project/deterministic-linkage/conjunction/part/param") == 0 ||
			strcmp(queue, "/project/exclude-pair/conjunction/part/param") == 0 ) {

		Expression *expression;
		if (strcmp(queue, "/project/blocking/conjunction/part/param") == 0){
			expression = project->getCurrentBlock()->getCurrentConjunction()->getCurrentExpression();
		} else if (strcmp(queue, "/project/deterministic-linkage/conjunction/part/param") == 0){
			expression = project->getCurrentDeterministic()->getCurrentConjunction()->getCurrentExpression();
		} else {
			expression = project->getCurrentExclude()->getCurrentConjunction()->getCurrentExpression();
		}
		string name;
		string value;
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "name") == 0) {
					name = attrs[i + 1];
				} else if (strcmp(attrs[i], (const char *) "value") == 0) {
					value = attrs[i + 1];
				}
				i += 2;
			}
		}
		expression->AddParameter(name, value);
	} else if (strcmp(queue, "/project/classifier") == 0) {
		if (attrs) {
			ClassifierType type = FELLEGI_SUNTER;
			double minResultForMatch = 0.0, maxResultForUnmatch = 100.0;
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "type") == 0) {
					if (strcmp(attrs[i + 1], (const char *) "fs") == 0) {
						type = FELLEGI_SUNTER;
					}
				} else if (strcmp(attrs[i], (const char *) "match-min-result")
						== 0) {
					minResultForMatch = atof(attrs[i + 1]);
				} else if (strcmp(attrs[i],
						(const char *) "not-match-max-result") == 0) {
					maxResultForUnmatch = atof(attrs[i + 1]);
				}
				i += 2;
			}
			project->setClassifier(new FSClassifier(type, minResultForMatch,
					maxResultForUnmatch));
		}
	} else if (strcmp(queue, "/project/classifier/exact-string-comparator")
			== 0) {
		ExactStringComparator *comparator = new ExactStringComparator();
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				evaluate_common_attributes(comparator, attrs, i);
				i += 2;
			}
		}
		project->AddComparator(comparator);
	} else if (strcmp(queue, "/project/classifier/approx-string-comparator")
			== 0) {
		ApproximatedStringComparator *comparator =
				new ApproximatedStringComparator();
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (!evaluate_common_attributes(comparator, attrs, i)) {
					if (strcmp(attrs[i], (const char *) "function") == 0) {
						if (strcmp((char *)attrs[i + 1], "jaro") == 0) {
							comparator->setFunction(&jaro);
						} else if (strcmp((char *)attrs[i + 1], "winkler") == 0) {
							comparator->setFunction(&winkler);
						} else if (strcmp((char *)attrs[i + 1], "editdistance") == 0) {
							comparator->setFunction(&editdistance_score);
					} else {
							//TODO Implement double metaphone, edit distance, bigram, etc
							cerr << "Invalid function type " << attrs[i + 1]
									<< endl;
							exit(-1);
						}
					} if (strcmp(attrs[i], (const char *) "minValueToBeMatch") == 0) {
						comparator->setMinValueToBeMatch(atof(attrs[i + 1]));
					}
				}
				i += 2;
			}
		}
		project->AddComparator(comparator);
	} else if (strcmp(queue, "/project/classifier/encode-string-comparator")
			== 0) {
		EncodedStringComparator *comparator = new EncodedStringComparator();
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (!evaluate_common_attributes(comparator, attrs, i)) {
					if (strcmp(attrs[i], (const char *) "function") == 0) {
						if (strcmp((char *)attrs[i + 1], "nysiis") == 0) {
							comparator->setFunction(&nysiis);
						} else if (strcmp((char *)attrs[i + 1], "dmetaphone")
								== 0) {
							comparator->setFunction(&doubleMetaphone);
						} else {
							//TODO Implement double metaphone, edit distance, bigram, etc
							cerr << "Invalid function type " << attrs[i + 1]
									<< endl;
							exit(-1);
						}
					}
				}
				i += 2;
			}
		}
		project->AddComparator(comparator);
	} else if (strcmp(queue, "/project/classifier/date-comparator") == 0) {
		DateComparator *comparator = new DateComparator();
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (!evaluate_common_attributes(comparator, attrs, i)) {
					if (strcmp(attrs[i], (const char *) "minValueToBeMatch") == 0) {
						comparator->setMinValueToBeMatch(atof(attrs[i + 1]));
					}
				}
				i += 2;
			}
		}
		project->AddComparator(comparator);
	} else if (strcmp(queue, "/project/classifier/br-city-comparator") == 0) {
		BrazilianCityComparator *comparator = new BrazilianCityComparator();
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (!evaluate_common_attributes(comparator, attrs, i)) {
					if (strcmp(attrs[i], (const char *) "uState") == 0) {
						comparator->setUState(atof(attrs[i + 1]));
					} else if (strcmp(attrs[i], (const char *) "mState") == 0) {
						comparator->setMState(atof(attrs[i + 1]));
					}

				}
				i += 2;
			}
		}
		project->AddComparator(comparator);

	} else if (strcmp(queue, "/project/output") == 0) {
		string histogram;
		string candidates;
		string deterministic;
		float min = 0;
		float max = 0;
		OutputFormat format = PAIR;
		string file;
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], "histogram") == 0) {
				    histogram = attrs[i + 1];
				} else if (strcmp(attrs[i], "deterministic") == 0) {
					deterministic = attrs[i + 1];
				} else if (strcmp(attrs[i], "candidates") == 0) {
					candidates = attrs[i + 1];
				} else if (strcmp(attrs[i], "file") == 0) {
					file = attrs[i + 1];
				} else if (strcmp(attrs[i], "min") == 0) {
					min = atof(attrs[i + 1]);
				} else if (strcmp(attrs[i], "max") == 0) {
					max = atof(attrs[i + 1]);
				} else if (strcmp(attrs[i], "format") == 0) {
					if (strcmp(attrs[i + 1], "record") == 0) {
						format = RECORD;
					} else if (strcmp(attrs[i + 1], "excel") == 0) {
						format = EXCEL;
					}
				}
				i += 2;
			}
		}
		project->AddOutput(new Output(format, file, min, max, histogram, candidates, deterministic));
	} else if (strcmp(queue, "/project/cache") == 0) {
		CacheType cacheType = CACHE_RECORD;
		CachePolicy policy = LessRecentlyUsed;
		int cacheSize = 100;
		int intraReaders = 100;
		if (attrs) {
			int i =0;
			while (attrs[i]) {
				if (strcmp(attrs[i], (const char *) "size") == 0) {
					cacheSize = atoi(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "intra-readers")
						== 0) {
					intraReaders = atoi(attrs[i + 1]);
				} else if (strcmp(attrs[i], (const char *) "type") == 0) {
					if (strcmp(attrs[i + 1], "record") == 0) {
						cacheType = CACHE_RECORD;
					} else if (strcmp(attrs[i + 1], "block") == 0) {
						cacheType = CACHE_BLOCK;
					}
				} else if (strcmp(attrs[i], (const char *) "policy") == 0) {
					if (strcmp(attrs[i + 1], "lru") == 0) {
						policy = LessRecentlyUsed;
					} else if (strcmp(attrs[i + 1], "fifo") == 0) {
						policy = FirstInFirstOut;
					} else if (strcmp(attrs[i + 1], "lfu") == 0) {
						policy = LessFrequentlyUsed;
					}
				}
				i += 2;
			}
		}
		Cache<unsigned int, Record*> *cache = new Cache<unsigned int, Record *>(
				cacheType, true, true, policy);
		cache->setSize(cacheSize);
		Cache<unsigned int, Record*> *cache2 = NULL;
		if (project->getTaskType() == LINKAGE){
			cache2 = new Cache<unsigned int, Record *>(
				cacheType, true, true, policy);
			cache2->setSize(cacheSize);
		}
		//For optimization, use the indexes of fields instead of their names
		project->UpdateFieldsIndexes();

		project->setCache(cache, cache2);
		project->setIntraReadersCacheSize(intraReaders);
	}
}
void Project::End(void *data, const char *el) {
	queuesize -= strlen((char *)el) + 1;
	queue[queuesize] = '\0';
}
void Project::ListCaching() {
	cout << "\t Caching: " << endl;
	cout << "\t\t" << cache->ToString() << endl;
}
void Project::PrintConfiguration() {
	cout << "--------------------------" << endl;
	cout << "Project: " << name << endl;
	cout << "Type: " << (taskType == DEDUPLICATION ? "Deduplication"
			: "Linkage") << endl;
	cout << "--------------------------" << endl;
	ListDataSources();
	ListBlocking();
	ListCaching();
}
float Project::Compare(Record *rec1, Record *rec2) {
	float result = 0;
	this->resultVector.str("");
	for (Comparators::iterator it = comparators.begin(); it
			!= comparators.end(); ++it) {
		double partialResult = (*it)->Compare(rec1, rec2);
		result += partialResult;
		char buffer[10];
		sprintf(buffer, "%1.2f ", partialResult);
		this->resultVector << buffer;
	}
	return result;
}
