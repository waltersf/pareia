/*
 * AnthillUtils.h
 *
 *  Created on: 12/08/2009
 *      Author: walter
 */

#ifndef ANTHILLUTILS_H_
#define ANTHILLUTILS_H_

#define DECLARE_ANTHILL_FILTER(klass, var) 	klass *var; Project *project; project_parameters_t parameters; bool isInitOk = false;

#define INIT_ANTHILL_FILTER(klass, var, ahUtil) 								\
	int initFilter(void* work, int size) { 										\
		parameters = *((project_parameters_t *) work); 							\
		try {																	\
			project = new Project(parameters.configurationFile, true); 		\
			project->Init();											 		\
			var = new klass(ahUtil, project); 									\
			isInitOk = true;					 									\
		} catch(FerapardaException &ex) { 										\
			cerr << "--------------------------------------------------------" << endl; \
			cerr << "An exception had been threw in initFilter() " << ex.what() << endl;	\
			cerr << "--------------------------------------------------------" << endl;	\
		} catch(FerapardaException *ex) {										\
			cerr << "--------------------------------------------------------" << endl;	\
			cerr << "An exception had been threw in initFilter() " << ex->what() << endl;	\
			cerr << "--------------------------------------------------------" << endl;	\
		} catch(exception ex) {													\
			cerr << "An exception had been threw in initFilter() " << ex.what() << endl;	\
		} catch(exception *ex) {												\
			cerr << "An exception had been threw in initFilter() " << ex->what() << endl;	\
		} 																		\
		return 1;																\
	}

#define PROCESS_ANTHILL_FILTER(var) \
		int processFilter(void* work, int size) { 									\
			try {																	\
				if (isInitOk){var->Process();};return 1;					 		\
			} catch(FerapardaException &ex) { 										\
				cerr << "--------------------------------------------------------" << endl; \
				cerr << "An exception had been threw in processFilter() " << ex.Report() << endl;	\
				cerr << "--------------------------------------------------------" << endl;	\
			} catch(FerapardaException *ex) {										\
				cerr << "--------------------------------------------------------" << endl;	\
				cerr << "An exception had been threw in processFilter() " << ex->Report() << endl;	\
				cerr << "--------------------------------------------------------" << endl;	\
			} catch(exception ex) {													\
				cerr << "An exception had been threw in processFilter() "  << endl;	\
			} catch(exception *ex) {												\
				cerr << "An exception had been threw in processFilter() "  << endl;	\
			} 																		\
			return 1;																\
		}

#define FINALIZE_ANTHILL_FILTER(var)int finalizeFilter(void) {delete var; delete project; return 1;}

#define CREATE_ANTHILL_FILTER(klass, var, ahUtil) DECLARE_ANTHILL_FILTER(klass, var) \
		INIT_ANTHILL_FILTER(klass, var, ahUtil) \
		PROCESS_ANTHILL_FILTER(var) \
		FINALIZE_ANTHILL_FILTER(var)

#include <string>
#include "FilterDev.h"
using namespace std;
/**
 * Wrapper for type InputPortHandler used in Anthill.
 */
class AnthillInputPort {
public:
	AnthillInputPort() {
		this->name = "";
		this->in = 0;
	}
	~AnthillInputPort() {
	}
	AnthillInputPort(string name, InputPortHandler in) {
		this->name = name;
		this->in = in;
	}
	bool operator==(const AnthillInputPort &other) const {
		return this->name == other.name;
	}
	bool operator!=(const AnthillInputPort &other) const {
		return !(*this == other);
	}
	friend class AnthillUtil;
	InputPortHandler in;
	string GetName(){
		return name;
	}
private:
	string name;
};
class AnthillOutputPort {
public:
	AnthillOutputPort() {
		this->name = "";
		this->out = 0;
	}
	~AnthillOutputPort() {
	}
	AnthillOutputPort(string name, OutputPortHandler out) {
		this->name = name;
		this->out = out;
	}
	bool operator==(const AnthillOutputPort &other) const {
		return this->name == other.name;
	}
	bool operator!=(const AnthillOutputPort &other) const {
		return !(*this == other);
	}
	friend class AnthillUtil;
	OutputPortHandler out;
private:
	string name;
};

/**
 * Provide an interface (façade) between application's classes and the
 * underlying framework (AKA Anthill).
 * By using a façade, one could extend this class and create fake objects
 * (not mock) that may be used for testing filters.
 */
class AnthillUtil {
public:
	AnthillUtil() {
	}
	virtual ~AnthillUtil() {
	}
	virtual int Rank();
	virtual AnthillInputPort GetInputPort(string name);
	virtual AnthillOutputPort GetOutputPort(string name);
	virtual int Read(AnthillInputPort port, void *msg, int size);
	virtual int Send(AnthillOutputPort port, void *msg, int size);
	virtual int TotalOfReaders(AnthillOutputPort port);
	virtual int TotalOfWriters(AnthillInputPort port);
	virtual int TotalOfFilterInstances();
	virtual int Probe(AnthillInputPort port);
	virtual int Close(AnthillOutputPort port);
};

#endif /* ANTHILLUTILS_H_ */
