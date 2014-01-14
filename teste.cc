#include "stlutil.h"
#include <iostream>
#include <string>
#include <ext/hash_map>
#include <map>
using namespace std;
using namespace __gnu_cxx;

class cool
{
  private: int serial;
  private: string longname;
  public: cool() {} // try to comment this out to see the error
  public: cool(int i, string s)
		  {
			serial = i;
			longname = s;
		  }  
  public: int get_serial() { return serial; }  
  public: string get_name() { return longname; }  
  public: void set_name(string n) { longname = n; }  
};  
typedef hash_map<string, cool *, stu_comparator_t, stu_eqstr_t> hashmap_t;
//typedef hash_map<string, cool *> hashmap_t;
int main2()
{
  char *five="Five";
  cool c(5,"Five Young Canibals");
  cool d(8,"Eighteen");

  //hash_map<char*, cool*> m;
  hashmap_t m;
  m[five]=&c;
  cool *e = new cool(5, "Sample");
  if(0 && m.find(five) != m.end()){
    cout << "Apagando ...";
     m.erase(five);
  }
  cout << "Feito "<<endl;
  cout << "Existe ? " << (m.find(five) != m.end()) << endl ;
  char *cem = "Cem";
  m[cem] = &d;
  //short for m.insert(m.begin,make_pair(const string("Eight"),d));

  hashmap_t::iterator i;
  for (i=m.begin(); i!= m.end(); ++i){
      cout << (*i).second->get_serial() << " " << (*i).second->get_name() << endl; 
  }
  char a;
  cout << "Ok?";
  cin >> a;
}  
struct ltstr
{
  bool operator()(const char* s1, const char* s2) const
  {
  	cout << "Comparando "  << s1 << " com " << s2 << endl;
	return strcmp(s1, s2) < 0;
  }
};

int main()
{
  map<const char*, int, ltstr> months;

  months["january"] = 31;
  months["february"] = 28;
  months["march"] = 31;
  months["april"] = 30;
  months["may"] = 31;
  months["june"] = 30;
  months["july"] = 31;
  months["august"] = 31;
  months["september"] = 30;
  months["october"] = 31;
  months["november"] = 30;
  months["december"] = 31;

  cout << "june -> " << months["june"] << endl;
  map<const char*, int, ltstr>::iterator cur  = months.find("june");
  map<const char*, int, ltstr>::iterator prev = cur;
  map<const char*, int, ltstr>::iterator next = cur;    
  ++next;
  --prev;
  cout << "Previous (in alphabetical order) is " << (*prev).first << endl;
  cout << "Next (in alphabetical order) is " << (*next).first << endl;
}

