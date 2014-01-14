#include "blocking.h"
#include "encode.h"
#include "datasource.h"
#include "stlutil.h"
#include <string>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <ext/hash_map>
#include "messagesfp.h"
#include <vector>
using namespace std;
using namespace __gnu_cxx;
using namespace Feraparda;

Block::~Block(){
  for(unsigned int i = 0; i < conjunctions.size(); i++){
	delete conjunctions[i];
  }
}
vector<string> ClassicalBlock::GenerateKeys(Record * record){
	return GenerateKeys(record, false);
}
vector<string> ClassicalBlock::GenerateKeys(Record * record, bool ignoreIfAnyIsEmpty){
  vector<string> keys;
    /*
     * The blocking predicate is a disjunction of conjunctions and
     * a conjunction is formed by expressions.
     * Each conjunction generates a blocking key.
     */
  for(unsigned int c = 0; c < conjunctions.size(); c++){
    string key = "";
    vector<Expression *> expressions = conjunctions[c]->getExpressions();
    for(unsigned int e = 0; e < expressions.size(); e++){
      string fieldValue = record->getField(expressions[e]->getField());
      if (fieldValue != ""){
        //Substring the field value
        int start = 0, len = fieldValue.size();
        if (expressions[e]->getStart() > 0 && start < len){
          start = expressions[e]->getStart();
        }
        if (expressions[e]->getSize() > 0 && expressions[e]->getSize() < len){
          len = expressions[e]->getSize();
        }
        //FIXME Add padding to keep key always with the same size
        try{
          if ((unsigned) (start + len) <= (unsigned) fieldValue.size()){
            fieldValue = fieldValue.substr(start, len);
          }
        }catch(exception &ex){
          cout << "-------------------------------------------------------------" << endl;
          cout << "Exception in substr. String length: " << fieldValue.size() << " Len: "  << len << " start: " << start << endl;
          cout << "-------------------------------------------------------------" << endl;
        }
        string transform = expressions[e]->getTransform();
        if (transform ==  "dmetaphone") {
          //FIXME: Needs to be refactored to C++ syntax
          char **metaphone = (char **) malloc(sizeof(char *) * 2);
          char* dup = strdup(fieldValue.c_str());

          int size = expressions[e]->getTransformMaxSize();
          if (size <= 0){
            size = 5;
          }
          doubleMetaphone((const char*) dup, metaphone, size);
          key += metaphone[0];
          key += metaphone[1];
          free(metaphone[0]);
          free(metaphone[1]);
          free(metaphone);
          free(dup);
        } else if (transform ==  "nysiis"){
          if (fieldValue.size() > 2){
			  char *nys = (char *) malloc(sizeof(char) * 50);
			  char *aux = (char *) malloc(sizeof(char) * fieldValue.size());
			  strcpy(aux, fieldValue.c_str());
			  int size = expressions[e]->getTransformMaxSize();
			  if (size <= 0){
				size = 5;
			  }
			  nysiis((const char*) aux, &nys, size);
			  key += nys;
			  free(aux);
			  free(nys);
          }
        } else if (transform == "hex"){
        	//Encodes field using hexadecimal. The field MUST fit in an integer!!
        	ostringstream sout;
        	int value = atoi(fieldValue.c_str());
        	sout << hex << value;
        	key += sout.str();
        } else if (transform == "soundex"){
        	int size = expressions[e]->getTransformMaxSize();
        	if (size <= 0){
        		size = 5;
        	}
			std::transform(fieldValue.begin(), fieldValue.end(),fieldValue.begin(), ::tolower);
        	char *aux = new char(size);
        	soundex((char *) fieldValue.c_str(), aux, size);
        	key += aux;
        	free(aux);
        } else if (transform == "brsoundex"){
        	int size = expressions[e]->getTransformMaxSize();
        	if (size <= 0){
        		size = 5;
        	}
        	char *aux = new char(size);
			std::transform(fieldValue.begin(), fieldValue.end(),fieldValue.begin(), ::tolower);
        	brsoundex((char *) fieldValue.c_str(), aux, size);
        	key += aux;
        	free(aux);
        } else {
          key += fieldValue;
        }
      } else if (ignoreIfAnyIsEmpty){
		  key = "";
		  break;
      } else {
       // cout << "Field VAZIO " << endl;
       //FIXME: Por que havia esse break aqui? break;
      }
    }
    if (key.size() > MAX_BLOCK_KEY_SIZE){
      keys.push_back(key.substr(0,MAX_BLOCK_KEY_SIZE)); //FIXME: Truncando para o valor de MAX_BLOCK_KEY_SIZE
    } else if(key.length()){
      keys.push_back(key); //FIXME: Como C++ cuida de strings alocadas na pilha guardadas num vetor?
    }

  }
  return keys;
}
BlockConjunction::~BlockConjunction(){
  for(unsigned int i = 0; i < expressions.size(); i++){
    delete expressions[i];
  }
}
Expression::~Expression(){
  hash_map<string, string>::iterator i;
  //for (i=parameters.begin(); i!= parameters.end(); ++i){
  //delete (*i).second;
  //}
}
void Block::ListConjunctions(string padding){
  string result = padding;
  for(unsigned i = 0; i < conjunctions.size() - 1; i++){
    result += "(" + conjunctions[i]->ToString() + ") OR \n"
      + padding + padding + padding;
  }
  result += "(" + conjunctions[conjunctions.size() -1]->ToString() + ")";
  cout << padding << result << endl;
}
string BlockConjunction::ToString(){
  string ret = "";
  for(unsigned int i = 0; i < expressions.size() - 1; i++){
    ret += expressions[i]->ToString() + " AND ";
  }
  ret += expressions[expressions.size() -1]->ToString();
  return ret;
}
string Expression::ToString(){
  return transform + "(" + field + ")";
}
