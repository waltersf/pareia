#ifndef FERAPARDAEXCEPTION_H_
#define FERAPARDAEXCEPTION_H_
using namespace std;
namespace Feraparda {
struct FerapardaException: exception {
private:
	string cause;
public:
	~FerapardaException() throw () {
	}
	FerapardaException(string cause) {
		this->cause = cause;
	}
	virtual const char *Report() const {
		return cause.c_str();
	}
	virtual const char * what() const throw () {
		return cause.c_str();
	}
};
}
#endif /*FERAPARDAEXCEPTION_H_*/
