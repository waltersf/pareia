#ifndef LOGGING_H_
#define LOGGING_H_
#include "stlutil.h"
#include "ferapardaexception.h"
#include <iostream>
#include <sstream>
#include <string>
#include <memory>
#include <fstream>
#include <ext/hash_map>

using namespace std;
namespace Feraparda {
enum LoggingLevel {LOGDEBUG, LOGINFO, LOGWARN, LOGERROR, LOGFATAL, LOGDEFAULT, LOGSTAT};
#define LOGFPDEBUG(logger, msg) logger->Debug(msg, __FILE__, __LINE__) 	
#define LOGFPINFO(logger, msg) logger->Info(msg, __FILE__, __LINE__)
#define LOGFPSTAT(logger, msg) logger->Statistics(msg, __FILE__, __LINE__)
#define LOGFPERROR(logger, msg) logger->Error(msg, __FILE__, __LINE__)
/**
 * Class used to log activities in the system. A text can be record in file 
 * and/or in <code>stderr</code> and/or <code>stdout</code>. 
 * Levels control when a message will be record or not. There is a special 
 * level, called <code>LOGSTAT</code> used to get statistics about the
 * execution.
 */
class Logger {
private:
	string category;
	ostream *out;
	LoggingLevel level;

	Logger() {
	}
public:
	string getCategory() {
		return category;
	}
	Logger(string category, ostream *out_, LoggingLevel level) {
		this->category = category;
		this->out = out_;
		this->level = level;
	}
	/**
	 * Note that the logger SHOULD NOT close its output because it can be shared
	 * between the loggers! The LogFactory is responsible by closing the 
	 * outputs.
	 */
	~Logger() {
	}
	ostream& operator <<(string s) {
		(*this->out) << s;
		return *(this->out);
	}
	void Debug(string msg, string file, int line) {
		if (level == LOGDEBUG) {
			Print(msg, "DEBUG", file, line);
		}
	}
	void Info(string msg, string file, int line) {
		if (level <= LOGINFO) {
			Print(msg, "INFO", file, line);
		}
	}
	void Warn(string msg, string file, int line) {
		if (level <= LOGWARN) {
			Print(msg, "WARN", file, line);
		}
	}
	void Error(string msg, string file, int line) {
		if (level <= LOGERROR) {
			Print(msg, "ERROR", file, line);
		}
	}
	void Fatal(string msg, string file, int line) {
		if (level == LOGFATAL) {
			Print(msg, "FATAL", file, line);
		}
	}
	void Statistics(string msg, string file, int line) {
		if (level == LOGSTAT) {
			(*out) << msg << endl;
		}
	}
	void Print(string msg, string level, string file, int line) {
		(*out) << "[" << level << "] " << category << " [" << file << ": "
				<< line << "] " << msg << endl;
	}
	bool IsLevelEnabled(LoggingLevel level) {
		return (level == LOGSTAT) ? this->level == LOGSTAT : level
				>= this->level;
	}
	string ToString() {
		ostringstream o;
		o << this->category << " [" << level << "]";
		return o.str();
	}
};
typedef hash_map<string, Logger *, stu_comparator_t, stu_eqstr_t> Loggers;
typedef hash_map<string, ostream *, stu_comparator_t, stu_eqstr_t> Appenders;

class LogFactory {
private:
	Loggers loggers;
	Appenders appenders;
	LogFactory() {
		appenders["stdout"] = &cout;
		appenders["stderr"] = &cerr;
		loggers["ROOT"] =
						new Logger("ROOT", appenders["stdout"], LOGDEBUG);
	}
	static LogFactory *GetInstance() {
		static LogFactory instance;
		return &instance;
	}
public:
	~LogFactory() {
		Loggers::iterator it;
		for (it=loggers.begin(); it!= loggers.end(); ++it) {
			delete (*it).second;
		}
		Appenders::iterator s;
		for (s=appenders.begin(); s!= appenders.end(); ++s) {
			if (&cout != (*s).second && &cerr != (*s).second) {
				delete (*s).second;
			}
		}
	}
	static Logger *GetLogger(string category) {
		LogFactory *instance = GetInstance();
		if (instance->loggers.find(category) == instance->loggers.end()) {
			return instance->loggers["ROOT"];
		} else {
			Logger *logger = instance->loggers[category];
			if (logger == NULL){
				logger = instance->loggers["ROOT"];
			}
			return logger;
		}
	}
	/**
	 * Configures the logging factory using a property file. A property file 
	 * contains a line like:
	 * <code>category=logger,level</code>.
	 * To define a new logger, use a line like that:
	 * <code>appender.logger=path|stderr|stdout</code>.
	 */
	static void Configure(string propertiesFile) {
		ifstream config(propertiesFile.c_str());

		while (!config.eof()) {
			string line;
			config >> line;
			if (line.substr(0, 1) == "#"){
				//ignore, just a comment
			}else if (line.substr(0, 8) == "appender") {
				int pos = line.find("=", 9);
				string name = line.substr(9, pos - 9);
				string file = line.substr(pos + 1);

				LogFactory *fact = GetInstance();
				fact->appenders[name] = new ofstream(file.c_str());
			} else if (line != "" && line.substr(0, 1) != "#") { //# is comment
				int posEquals = line.find("=");
				int posComma = line.find(",");
				string name = line.substr(0, posEquals);
				string appender = line.substr(posEquals + 1, posComma
						- posEquals - 1);
				string level = line.substr(posComma + 1);
				LogFactory *fact = GetInstance();
				if (fact->appenders.find(appender) == fact->appenders.end()) {
					throw FerapardaException("Appender '" + appender + "' does not exist.");
				}
				
				//Remove previous logger definition
				if (fact->loggers.find(name) != fact->loggers.end()){
					Logger *aux = fact->loggers[name];
					delete aux;
				}
				if (level == "DEBUG") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGDEBUG);
				} else if (level == "INFO") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGINFO);
				} else if (level == "WARN") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGWARN);
				} else if (level == "ERROR") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGERROR);
				} else if (level == "FATAL") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGFATAL);
				} else if (level == "STAT") {
					fact->loggers[name] = new Logger(name, fact->appenders[appender], LOGSTAT);
				}
			}
		}
	}
	/**
	 * Creates a new logger, identified by <code>category</code>.
	 */
	static Logger *CreateLogger(string category, string outputName,
			LoggingLevel level) {
		ostream *out;
		if (outputName == "stdout") {
			out = &cout;
		} else if (outputName == "stderr") {
			out = &cerr;
		} else {
			out = new ofstream(outputName.c_str(), ios::app);
		}
		Logger *logger(new Logger(category, out, level));
		GetInstance()->loggers[category] = logger;
		return logger;
	}

};
}
#endif /*LOGGING_H_*/
