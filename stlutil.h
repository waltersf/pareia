/*
 * Contains some utilities and structs used with STL
 */
#ifndef _STL_UTIL_
#define _STL_UTIL_
#include <cstring>
#include <sstream>
#include <ext/hash_map>
using namespace __gnu_cxx;
using namespace std;
/**
 * Comparator used in STL hash maps 
 */
struct stu_comparator_t {
  hash<const char*> H;
  inline size_t operator()(const std::string& s1) const {
	return H(s1.c_str());
  }
};
struct stu_eqstr_t {
  bool operator()(const std::string& s1, const std::string& s2) const {
	return s1 == s2;
  }
};
namespace __gnu_cxx {
  template<> struct hash< std::string > {
	size_t operator()( const std::string& x ) const {
	  return hash< const char* >()( x.c_str() );
	}
  };
}

typedef struct {
	unsigned int id1;
	unsigned int id2;
} KeyPair;

class PairComparator {
public:
	PairComparator(){}
	bool operator()(KeyPair c1, KeyPair c2) {
		return (c1.id2 < c2.id2) || 
			(c2.id2 == c1.id2 && c2.id1 < c1.id1);
	}
};
class BlockKeyComparator {
public:
	bool operator()(const char * c1, const char* c2) const{
		return strcmp(c1, c2) < 0;
	}
};
class Util { 
public:
	static string ToString(int number){
		ostringstream o;
		o << number;
		return o.str().c_str();
	}
	static string ToString(unsigned long number){
			ostringstream o;
			o << number;
			return o.str().c_str();
	}
	static string ToString(float number){
		ostringstream o;
		o << number;
		return o.str().c_str();
	}
	static string ToString(unsigned number){
		ostringstream o;
		o << number;
		return o.str().c_str();
	}

};

#endif
