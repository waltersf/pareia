#ifndef CACHE_H_ 
#define CACHE_H_ 
#include "stlutil.h"
#include "logging.h"
#include <cassert>
#include <iostream>
#include <iterator>
#include <list>
#include <sstream>
#include <string>
#include <vector>
#include <ext/hash_map>
#include <sys/time.h>
using namespace __gnu_cxx;
using namespace std;
#define CACHE_MISS NULL
namespace Feraparda {
  enum CacheType {CACHE_RECORD, CACHE_BLOCK};
  enum CachePolicy {FirstInFirstOut, LessRecentlyUsed, LessFrequentlyUsed};
  /**
   * A class that represents the element to be stored in the cache.
   */
  class CacheEntry {
		private:
			void *cacheable;
			int hashKey;
			int hits;
		public:
			CacheEntry(void *cacheable, int hashKey){
				this->cacheable = cacheable;
				this->hashKey = hashKey;
				this->hits = 0;
			}
			int getHashKey(){return hashKey;}
			void *getCacheable(){return cacheable;}
			void setCacheable(void *cacheable){this->cacheable = cacheable;}
			void newHit(){hits++;}
			int getHits() {return hits;}
	};
  /**
   * A simple cache implementation with LRU, FIFO and LFU support 
   */
  template <typename K, typename Cacheable>
	class Cache{
	  	typedef list<CacheEntry *>::iterator ContentIterator;
		public:
			~Cache(){
				list<CacheEntry *>::iterator it;	
				for(it=keys.begin(); it != keys.end(); it++){
					CacheEntry *entry = *it;
					if (this->deleteContentInDestructor){
						delete (Cacheable) (*it)->getCacheable();
					}
					delete entry;
				}
				keys.clear();
				map.clear();
				//cout << "Cache overhead " << overhead * 0.000001 << " s\n";
			}
			void setDeleteOptions(bool deleteInDestructor, bool deleteOnOverflow){
					this->deleteContentInDestructor = deleteInDestructor;
					this->deleteContentOnOverflow = deleteOnOverflow;
			}
			/**
			 * Constructor.
			 * @param type Cache type
			 * @param deleteContentInDestructor Indicates if the content of cache will
			 * be deleted during destruction. Caution: never use this option equals to 
			 * <code>true</code> and execute a explicit <code>delete</code>!
			 * @param deleteContentOnOverflow Indicates that the content will be removed of the cache 
			 * when it becames full. Caution: never use this option equals to 
			 * <code>true</code> and execute a explicit <code>delete</code>!
			 * 
			 */
			Cache(CacheType type, bool deleteContentInDestructor, 
						bool deleteContentOnOverflow, CachePolicy policy){
			  this->type = type;
			  size = 0;
			  this->deleteContentInDestructor = deleteContentInDestructor;
			  this->deleteContentOnOverflow = deleteContentOnOverflow;
			  this->policy = policy;
			  Init();
			}
			void Init() {
				logger = LogFactory::GetLogger("cache");
				//overhead = 0;
				keyListSize = 0;
				debug = false;
			}
			Cache(){
				this->type = CACHE_RECORD;
				this->deleteContentInDestructor = true;
				this->deleteContentOnOverflow = true;
				Init();
				policy = LessRecentlyUsed;
			}
			/** 
			 * Defines the cache size
			 * @param size Size of cache. 
			 */
			void setSize(int size){this->size = size;}
			void Remove(K key){
					if (map.find(key) != map.end()){
							list<CacheEntry *>::iterator it = map.find(key)->second;
							keys.splice(keys.begin(), keys, it);

							CacheEntry *entry = Hit(key);
							if (deleteContentOnOverflow){
									delete (Cacheable) entry->getCacheable();
							}
					}

			}
			/**
			 * Put a value in the cache.
			 * @return <code>NULL</code> if the cache did not become full or 
			 * the content excluded from cache if it is full.
			 * @param key Key for the cache
			 * @param cacheable Element to be stored
			 */
			Cacheable Put(K key, Cacheable cacheable){
				//struct timeval start;
				//struct timeval end;
				//gettimeofday(&start, NULL);
				//cout << "[CACHE] Putting key " << key << " in the cache " << endl;
				if (size == 0){
						return NULL;
				}
				//Just a refresh in the cache
				if (map.find(key) != map.end()){
					CacheEntry *entry = Hit(key);
					if (deleteContentOnOverflow){
						delete (Cacheable) entry->getCacheable();
					}
					entry->setCacheable(cacheable);
					return NULL;
				}
				
				Cacheable ret = NULL;
				//checks if overflow
				if (keyListSize >= this->size && this->size > 0){
					//gettimeofday(&end, NULL);				
					//overhead += (end.tv_sec -start.tv_sec) * 1000000 + 
					//		(end.tv_usec -start.tv_usec);
					CacheEntry *entry = keys.back();
					keys.pop_back();
					keyListSize --;
					//remove from map where the data is stored
					map.erase(entry->getHashKey());
					ret = (Cacheable) entry->getCacheable();
					delete entry;
				}

				CacheEntry *entry = new CacheEntry(cacheable, key);
				entry->newHit();
				/* 
				 * Note that the first element in the list will be the last one
				 * to be removed. 
				 */
				list<CacheEntry *>::iterator it; 
				if (this->policy != LessFrequentlyUsed){
					keys.push_front(entry);
					it = keys.begin();
				} else {
					list<CacheEntry *>::iterator itLocate = keys.end();
					--itLocate;
					while (itLocate != keys.begin() && (*itLocate)->getHits() < entry->getHits()){
						--itLocate;
					}
					if (itLocate == keys.begin()){
						keys.push_front(entry);
					} else {
						keys.insert(itLocate, entry);
					}	
					it = itLocate;
				}
				keyListSize ++;
				map[key] = it;
				
				if (deleteContentOnOverflow){
					delete (Cacheable) ret;
					return NULL;
				} else { 
					return ret;
				}
				return NULL;
			}
			bool HasKey(K key){
				return (map.find(key) != map.end());
			}
			/**
			 * Retrieves an element from cache.
			 * @return <code>NULL</code> if the key doesn't exist in the cache or
			 * the element stored
			 * @param key Cache key
			 */
			Cacheable Get(K key) {
				if (map.find(key) != map.end()){
					hits ++;
					return (Cacheable) Hit(key)->getCacheable();
				} else {
					misses ++;
					if (logger->IsLevelEnabled(LOGSTAT)){
						LOGFPSTAT(logger, Util::ToString(key)  + " " + 
								Util::ToString((int) size + 1));
					}
					return NULL;
				}
			}
			/**
			 * Returns the value identified by <code>key</code>, but preserve
			 * the order (do not obey the police).
			 */
			Cacheable GetAndPreserve(K key){
				if (map.find(key) != map.end()){
					list<CacheEntry *>::iterator it = map.find(key)->second;
					return (Cacheable) (*it)->getCacheable();
				} else {
					return NULL;
				}
			}
			/**
			 * Returns the cache configuration as string.
			 * @return Returns the cache configuration as string.
			 */
			string ToString(){
				ostringstream o;
				o << size;
				string s = (type == CACHE_RECORD? "Stores Records": "Stores blocks");
				s += " Size: " + o.str();
				return s;
			}
			/**
			 * Returns a list of keys where the last ones were recently used.
			 */
			string DumpKeys(){
				ostringstream o;
				list<CacheEntry *>::iterator it;	
				for (it=keys.begin(); it!= keys.end(); ++it){
					o << (*it)->getHashKey() << " ";
				}
				return o.str();
			}
			void setPolicy(CachePolicy policy){this->policy = policy;}
		private:
			//long overhead;
			Logger *logger;
			/** Stores the key and references to the elements */
			hash_map<K, list<CacheEntry *>::iterator> map;
			/** Stores the elements and keeps the LRU policy*/
			list<CacheEntry *> keys;
			/*Controls the size of the list because its operation, size() cost a lot! */
			unsigned int keyListSize;
			/** Type of cache */
			CacheType type;
			/** Delete the content during on destruction? */
			bool deleteContentInDestructor;
			/** Delete the content when expiring content? */
			bool deleteContentOnOverflow;
			/** Size of cache */
			unsigned int size;
			/** Total of hits */
			int hits;
			/** Total of misses */
			int misses;
			/** Keeps information about stack distance */
			vector<int> stackDistance;
			/** Policy for cache expiration */
			CachePolicy policy;

			bool debug;
			/**
			 * Refresh the cache, putting the key in the tail of the list
			 */
			CacheEntry *Hit(K key){
				//Como usar tipo para template? Não consegui compilar esta declaração
				//hash_map<K, list<CacheEntry *>::iterator>::iterator hashIt = map.find(key);
				
				if (this->policy == LessRecentlyUsed) {
					//Put the element identified by key in front of list
					list<CacheEntry *>::iterator it = map.find(key)->second;
					if (logger!= NULL && logger->IsLevelEnabled(LOGSTAT)){
						LOGFPSTAT(logger, Util::ToString(key) + " " + 
								Util::ToString((int) distance(keys.begin(), it)));
					}
					keys.splice(keys.begin(), keys, it);
					return keys.front();
				} else if (this->policy == LessFrequentlyUsed) {
					list<CacheEntry *>::iterator it = map.find(key)->second;
					CacheEntry *entry = *it;
					keys.erase(it);
					//Realocate the entry
					list<CacheEntry *>::iterator itLocate = it;
				
					while (itLocate != keys.begin() && (*itLocate)->getHits() < entry->getHits()){
						--itLocate;
					}
					if (itLocate == keys.begin()){
						keys.push_front(entry);
					} else {
						keys.insert(itLocate, entry);
					}
					return entry;
				} else {
					//Nothing to reorganize.
					return *map.find(key)->second;
				}
			}
	};
	
}
#endif
