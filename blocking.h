#ifndef BLOCKING_H_
#define BLOCKING_H_
#include "datasource.h"
#include "stlutil.h"
#include <string>
#include <vector>
#include <ext/hash_map>
using namespace __gnu_cxx;
using namespace std;
namespace Feraparda {
	enum BlockType {CLASSICAL, SORTING, DETERMINISTIC, EXCLUDE};
	class Expression{
		private:
			string field;
			string transform;
			hash_map<string, string, hash<string> > parameters;
			int size;
			int start;
			int transformMaxSize;
		public:
			string ToString();
			Expression(string field_, string transform_, int start_, int size_, int max){
				this->field = field_;
				this->transform = transform_;
				this->size = size_;
				this->start = start_;
				this->transformMaxSize = max;
			}
			~Expression();
			void AddParameter(string k, string p){
				parameters[k] = p;
			}
			string getField(){return field;}
			string getTransform(){return transform;}
			int getSize(){return size;}
			int getStart(){return start;}
			int getTransformMaxSize(){return transformMaxSize;}
	};
	class BlockConjunction{
		private:
			vector<Expression*> expressions;
			Expression *currentExpression;
		public:
		    Expression *getCurrentExpression(){return currentExpression;}
			void AddExpression(Expression *c){expressions.push_back(c);currentExpression = c;}
			~BlockConjunction();
			string ToString();
			//FIXME: Could be "friend" of the class Block
			vector<Expression *> getExpressions(){return expressions;}
	};
	class Block{
		public:
			virtual ~Block();
			Block(BlockType t){type = t;}
			BlockType getType(){return type;}
			BlockConjunction *getCurrentConjunction(){return currentConjunction;}
			void AddConjunction(BlockConjunction *conjunction){
				conjunctions.push_back(conjunction);
				currentConjunction = conjunction;
			}
			/**
			 * Generate the keys used in the blocking stage.
			 */
			virtual vector<string> GenerateKeys(Record *) = 0;
			virtual vector<string> GenerateKeys(Record *, bool) = 0;
			void ListConjunctions(string);
		protected:
			vector<BlockConjunction*> conjunctions;
		private:
			BlockType type;
			BlockConjunction *currentConjunction;
	};
	class ClassicalBlock : public Block {
		public:
			ClassicalBlock(BlockType type) : Block(type){}
			virtual vector<string> GenerateKeys(Record *);
			virtual vector<string> GenerateKeys(Record *, bool);
	};
	class DeterministicBlock : public ClassicalBlock {
			public:
				DeterministicBlock(BlockType type) : ClassicalBlock(type){}
	};
	class ExcludeBlock : public ClassicalBlock {
		public:
			ExcludeBlock(BlockType type) : ClassicalBlock(type){}
	};
	class BlockFactory {
		public: static Block *GetBlock(BlockType type){
			if (type == CLASSICAL) {
				return new ClassicalBlock(type);
			} else if (type == DETERMINISTIC){
				return new DeterministicBlock(type);
			} else if (type == EXCLUDE){
				return new ExcludeBlock(type);
			} else {
				throw FerapardaException("Invalid blocking type");
			}
		}
	};
}
#endif /*BLOCKING_H_*/
