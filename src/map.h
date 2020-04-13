
// lang: CwC
#pragma once
#include "object.h"
#include "dataframe.h"
#include "keyvalue.h"

/*
#include "array.h"
#include "string.h"
#include "dataframe.h"
#include <assert.h>
*/


// scalable map class
class DFMap : public Object
{
public:
	// fields
	DFKeyValue **map;   // pointer points to the first pair of <key, value> in map
	size_t size;      	// size of map
	size_t max;       	// capacity of map. wnen size=max, max will be double in size
	
	// Constructor for Map: initialed with 2 KeyValue* with null value
	DFMap()
	{
		this->map = new DFKeyValue *[128];
		this->size = 0;
		this->max = 128;
		for (int i =0; i<128; i++)
		{
			this->map[i] = nullptr;
		}
	}

	// Destructor for Map
	~DFMap() { this->clear();	}

	// Returns the number of entries in this map
	size_t length() { return this->size; }

	// Removes all entries in this map
	void clear()
	{
		delete[] this->map;
		this->size = 0;
		this->max = 0;
	}

	////////////////////////////////////////////////////////////////////
	// Gets the value at a specific key: since the way to resolve the hashing
	// conflict is to save the conflicting pair to an empty spot in the map, 
	// we need to look for the correct pair by comparing pair->key and map[hv]->key
	DataFrame* get(Key* key)
	{
		size_t hv = getHashValue(key->key_);

		// if map[hv] is empty, return null
		if (!this->map[hv])	return nullptr;

		// otherwise, we need to see what key is in current mp[hv]
		char* curKey = this->map[hv]->key->key_;

		// if current key in map[hv] equals THE key, return mp[hv]
		// map[hv]->value->print_dataframe();
		if (!strcmp(curKey, key->key_)) return this->map[hv]->value;

		// otherwisre, we need to loop through map[] and find the pair with the input key
		for (int i=0; i<this->max; i++)
		{
			// comparing pair->key and map[i]->key
			if (this->map[i] && !strcmp(key->key_, map[i]->key->key_))
				return this->map[i]->value;
		}

		// if nothing has been found, ruturn null
		return nullptr;
	}

	//////////////////////////////////////////////////////////////////////////
	// Sets the value at the specified key to the given value
	// if map size is full, the map will be re-allocated with double the space
	void set(Key *key, DataFrame *value)
	{
		size_t hv, old_max;
	
		// if map is full, 
		// 1. double the space. 
		// 2. re-hash all pairs to the new, bigger map
		if (this->max == this->size) 						// current map is full
		{
			DFKeyValue **opp = new DFKeyValue *[2 * this->max];	// double space
			old_max = this->max;								// remember the old capacity
			this->max = 2 * this->max;							// update max to the new capacity

			// rehash content of current map to new map
			for (int i = 0; i < old_max; i++)
			{
				if (this->map[i])  							// only when map[i] has a pair
				{
					size_t hv = getHashValue(this->map[i]->key->key_);
					//printf("set(): rehashing: i=%d  hv=%d", i, hv);
					this->map[i]->key->printKey();
					//this->map[i]->value->print();
					//pln();
					addPairToMap(hv, map[i], opp);	// add map[i] to new map pointed by opp at index=hv
				}
			}
			
			// destroy the current map, and use the new, bigger map
			delete [] this->map;
			this->map = opp;
		}

		// now it's time to add pair(key, value) to the new map
		hv = getHashValue(key->key_);      			// get hash value by key
		DFKeyValue* pair = new DFKeyValue(key, value);
		addPairToMap(hv, pair, this->map);			// add pair to this->map at index=hv 
		this->size++;			   					// increase map size by 1
	}

	////////////////////////////////////////////////////////////////////
	// add pair to map[index]
	// in case of a hashing conflict, i.e. aMap[index] has a pair already,
	// loop through map[], find an empty spot i, and save pair to map[i]
	// otherwise, save pair to map[index]
	void addPairToMap(size_t index, DFKeyValue *pair, DFKeyValue** aMap)
	{
		if (aMap[index])  			// aMap[index] has a pair already
		{
			for (int i = 0; i < this->max; i++)
			{
				if (!aMap[i])   	// find empty spot at map[i]
				{	
					aMap[i] = pair;	// save pair to aMap[i]
					return;
				}
			}
		}
		else // otherwise
		{
			aMap[index] = pair;		// save pair to aMap[index]
			// printf("********* %d\n", aMap[index]->value);
		}
	}

	///////////////////////////////////////////////////////////////////////
	// Removes value at the specified key and returns the removed object
	// because of the way hashing value conflicts are handled, map[hashvalue(key)]
	// may not match the key. In the case of a mismatch, remove() has to loop through
	// the map to find it.
	DataFrame *remove(Key *key)
	{
		size_t hv = getHashValue(key->key_);	// get hash value for key
		DFKeyValue* pair = this->map[hv];   	// keep the key-value pair for return 

		// if map[hv] is empty, return null
		if (!pair) return nullptr;

		// otherwise, 1. check if map[hv]->key matches the passed in key
		// 2. if it matches, remove it 
		// 3. move a pair in map[] to map[hv] if the pair has hash value = hv
		if (!strcmp(pair->key->key_, key->key_))	// if current key == passed in key
		{
			this->map[hv] = nullptr; 				// remove the current may

			// loop through map to see if there exists another pair who has the same hv
			for (int i = 0; i < this->max; i++)
			{
				if (this->map[i]) 					// as long as map[i] has a value, do following
				{
					size_t curHv = getHashValue(this->map[i]->key->key_); // get hv for map[i]
					if (curHv == hv)					// check if map[i] has the same hv
					{
						this->map[hv] = this->map[i]; 	// move the new pair to map[hv]
						this->map[i] = nullptr;		  	// remove current pair
						this->size--;
						return pair->value;
					}
				}
			}
			this->size--; 		// decrease size
			return pair->value;
		}

		// if keys do not match at map[hv], loop through map[] to find the key
		for (int i = 0; i < this->max; i++)
		{
			pair = this->map[i];
			if (!strcmp(pair->key->key_, key->key_))	// if one is found, remove it and return
			{
				this->map[hv] = nullptr;
				this->size--;
				return pair->value;
			}
		}

		return nullptr;
	}

	// get hash value for given key k
	size_t getHashValue(char *k)
	{
        size_t hash = 0;
        for (size_t i = 0; i < strlen(k); ++i)
            hash = k[i] + (hash << 6) + (hash << 16) - hash;

		return (hash % this->max);
	}

	// Checks if this map is equal to another object
	bool equals(DFMap *o)
	{
		if (this->size != o->size) return 0; // return 0 if size is not the same

		if ((this->size==0) && (o->size==0)) return 1;  // return true if both are empty

		//printf("this-size=%lu   new-size=%lu\n", this->size(), newQ->size());
		// compare each element in this map and otherMap
		for (int i=0; i<this->max; i++)
		{
			if (this->map[i] != nullptr) 
			{
				if (!this->map[i]->equals(o->map[i])) return 0;
			}
		}

		return 1;
	};

	void printMap() 
   	{ 
        for (int i = 0; i < this->max; i++) 
		{
			//printf("in PrintMP()\n");
			if (this->map[i])
			{
				printf("map[%d] : key=", i);
				this->map[i]->key->printKey();
				printf(", value=");
				this->map[i]->value->print_dataframe();
				pln();
			}
    	}  
		pln();
	}
	void printMap(Key* k) 
   	{ 
        for (int i = 0; i < this->max; i++) 
		{
			if (!map[i])
				continue;

			if (!strcmp(this->map[i]->key->key_, k->key_) )
			{
				printf("map[%d] : key=", i);
				this->map[i]->key->printKey();
				printf(", value=");
				this->map[i]->value->print_dataframe();
				pln();
			}
    	}  
		pln();
	}
};

/////////////////////////////////////
// string-size_t map
class STMap : public Object
{
public:
	// fields
	STKeyValue **map; 	// pointer points to the first pair of <string, size_t> in map
	size_t size;    	// size of map
	size_t max;     	// capacity of map. wnen size=max, max will be double in size
	
	// Constructor for Map: initialed with 2 KeyValue* with null value
	STMap()
	{
		this->map = new STKeyValue*[8];
		this->size = 0;
		this->max = 8;
		for (int i=0; i<8; i++)
			this->map[i] = nullptr;
	}

	// Destructor for Map
	~STMap() { this->clear();	}

	// Returns the number of entries in this map
	size_t length() { return this->size; }

	// Removes all entries in this map
	void clear()
	{
		delete[] this->map;
		this->size = 0;
		this->max = 0;
	}

	////////////////////////////////////////////////////////////////////
	// Gets the value at a specific key: since the way to resolve the hashing
	// conflict is to save the conflicting pair to an empty spot in the map, 
	// we need to look for the correct pair by comparing pair->key and map[hv]->key
	size_t get(String* key)
	{
		size_t hv = getHashValue(key->c_str());  // hash the key
		if (!this->map[hv])	return 0; // if map[hv] is empty, return null

		//printf("====%d, size=%d\n", hv, size);
		//printf("key=%s, mkey=%s\n",key->c_str(), map[hv]->key_.c_str());
		//printf("value=%d\n", map[hv]->value_);
		// if current key in map[hv] equals THE key, return mp[hv]
		//if (key->equals(&this->map[hv]->key_)) return map[hv]->value_;
		if (!strcmp(key->c_str(), map[hv]->key_.c_str()))
			return map[hv]->value_;

		// otherwisre, we need to loop through map[] and find the pair with the input key
		for (int i=0; i<this->max; i++)
		{
			if (this->map[i] && key->equals(&map[hv]->key_))
				return this->map[i]->value_;
		}
		// if nothing has been found, ruturn null
		return 0;
	}

	//////////////////////////////////////////////////////////////////////////
	// Sets the value at the specified key to the given value
	// if map size is full, the map will be re-allocated with double the space
	void set(String *key, size_t value)
	{
		size_t hv, old_max;
	
		//printf("setting key=%s, val=%d\n", key->c_str(), value);
		// if map is full, 
		// 1. double the space. 
		// 2. re-hash all pairs to the new, bigger map
		if (this->max == this->size) 						// current map is full
		{
			STKeyValue **opp = new STKeyValue *[2 * this->max];	// double space
			old_max = this->max;								// remember the old capacity
			this->max = 2 * this->max;	
			
			for (int i=0; i<this->max; i++)						// update max to the new capacity
				opp[i] = nullptr;

			// rehash content of current map to new map
			for (int i = 0; i < old_max; i++)
			{
				if (this->map[i])  							// only when map[i] has a pair
				{
					size_t hv = getHashValue(this->map[i]->key_.c_str());
					addPairToMap(hv, map[i], opp);	// add map[i] to new map pointed by opp at index=hv
				}
			}
			
			// destroy the current map, and use the new, bigger map
			delete [] this->map;
			this->map = opp;
		}

		// now it's time to add pair(key, value) to the new map
		hv = getHashValue(key->c_str());      			// get hash value by key
		STKeyValue* pair = new STKeyValue(key, value);
		addPairToMap(hv, pair, this->map);			// add pair to this->map at index=hv 
		this->size++;			   					// increase map size by 1
	}

	////////////////////////////////////////////////////////////////////
	// add pair to map[index]
	// in case of a hashing conflict, i.e. aMap[index] has a pair already,
	// loop through map[], find an empty spot i, and save pair to map[i]
	// otherwise, save pair to map[index]
	void addPairToMap(size_t index, STKeyValue *pair, STKeyValue** aMap)
	{
		if (aMap[index])  			// aMap[index] has a pair already
		{
			for (int i = 0; i < this->max; i++)
			{
				if (!aMap[i])   	// find empty spot at map[i]
				{	
					aMap[i] = pair;	// save pair to aMap[i]
					return;
				}
			}
		}
		else // otherwise
		{
			aMap[index] = pair;		// save pair to aMap[index]
			// printf("********* %d\n", aMap[index]->value);
		}
	}

	// get hash value for given key k
	size_t getHashValue(char *k)
	{
        size_t hash = 0;
        for (size_t i = 0; i < strlen(k); ++i)
            hash = k[i] + (hash << 6) + (hash << 16) - hash;
		return (hash % this->max);
	}

	// Checks if this map is equal to another object
	bool equals(STMap *o)
	{
		if (this->size != o->size) return 0; // return 0 if size is not the same
		if ((this->size==0) && (o->size==0)) return 1;  // return true if both are empty

		// compare each element in this map and otherMap
		for (int i=0; i<this->max; i++)
		{
			if (this->map[i] != nullptr) 
				if (!this->map[i]->equals(o->map[i])) return 0;
		}
		return 1;
	};

	void printMap() 
   	{ 
        for (int i = 0; i < this->max; i++) 
		{
			if (this->map[i])
			{
				this->map[i]->key_.printString();
				printf("	value=%d\n", map[i]->value_);
			}
    	}  
	}
};

class Num : public Object {
public:
  size_t v = 0;
  Num() {}
  Num(size_t v) : v(v) {}
};

/*******************************************/

/**  Item_ are entries in a Map, they are not exposed, are immutable, own
 *   they key, but the value is external.  author: jv */
class Items_ {
public:
  Array keys_; 
  Array vals_; 

  Items_() : keys_(8), vals_(8) {}
  
  Items_(Object *k, Object * v) : keys_(8), vals_(8) {
  //  keys_.push_back(k);
   // vals_.push_back(v);
   keys_.append(k);
   vals_.append(v);
  }

  bool contains_(Object& k) {
    for (int i = 0; i < keys_.count(); i++) 
      if (k.equals(keys_.get(i)))
	  return true;
    return false;
  }

  Object* get_(Object& k) {
    for (int i = 0; i < keys_.count(); i++) 
      if (k.equals(keys_.get(i)))
	return vals_.get(i);
    return nullptr;
  }

  size_t set_(Object& k, Object* v) {
      for (int i = 0; i < keys_.count(); i++) 
          if (k.equals(keys_.get(i))) {
              //vals_.insert(i,v);
			  Object* vv = vals_.get(i);
			  vv = v;
              return 0;
          }
      // The keys are owned, but the key is received as a reference, i.e. not owned so we must make a copy of it. 
      keys_.append(k.clone());
      vals_.append(v);
      return 1;
  }

  size_t erase_(Object& k) {
      for (int i = 0; i < keys_.count(); i++) 
          if (k.equals(keys_.get(i))) {
              keys_.pop(i);
              vals_.pop(i);
              return 1;
          }
      return 0;
  }
};

/** A generic map class from Object to Object. Subclasses are responsibly of
 * making the types more specific.  author: jv */
class Map : public Object {
public:      
  size_t capacity_;
    // TODO this was not size of the map, but number of occupied item positions in the top level
  size_t size_ = 0;
  Items_* items_;  // owned

  Map() : Map(10) {}
  Map(size_t cap) {
    capacity_ = cap;
    items_ = new Items_[capacity_];
  }
  
  ~Map() { delete[] items_; }

  /** True if the key is in the map. */
  bool contains(Object& key)  { return items_[off_(key)].contains_(key); }
  
  /** Return the number of elements in the map. */
  size_t size()  {
      return size_;
  }

  size_t off_(Object& k) { return  k.hash() % capacity_; }
  
  /** Get the value.  nullptr is allowed as a value.  */
  Object* get_(Object &key) { return items_[off_(key)].get_(key); }

  /** Add item->val_ at item->key_ either by updating an existing Item_ or
   * creating a new one if not found.  */
  void set(Object &k, Object *v) {
    if (size_ >= capacity_)
        grow();
    size_ += items_[off_(k)].set_(k,v);
  }
  
  /** Removes element with given key from the map.  Does nothing if the
      key is not present.  */
  void erase(Object& k) {
    size_ -= items_[off_(k)].erase_(k);
  }
  
  /** Resize the map, keeping all Item_s. */
  void grow() {
      //LOG("Growing map from capacity " << capacity_);
      Map newm(capacity_ * 2);
      for (size_t i = 0; i < capacity_; i++) {
          size_t sz = items_[i].keys_.count();
          for (size_t j = 0; j < sz; j++) {
              Object* k = items_[i].keys_.get(j);
              Object* v = items_[i].vals_.get(j);
              newm.set(*k,v);
              // otherwise the values would get deleted (if the array's destructor was doing its job I found later:)
              items_[i].vals_.insert(j, nullptr);
          }
      }
      delete[] items_;
      items_ = newm.items_;
      capacity_ = newm.capacity_;
      assert(size_ == newm.size_);
      newm.items_ = nullptr;
  } 
}; // Map

class MutableString : public String {
public:
  MutableString() : String("", 0) {}
  void become(const char* v) {
    size_ = strlen(v);
    cstr_ = (char*) v;
    hash_ = hash_me();
  }
};


/***************************************************************************
 * 
 **********************************************************author:jvitek */

// String to integer number map
class SIMap : public Map {
public:
  SIMap () {}
  Num* get(String& key) { return dynamic_cast<Num*>(get_(key)); }
  void set(String& k, Num* v) { assert(v); Map::set(k, v); }
  void print()
  {
	  for (int i = 0; i < capacity_; i++)
	  {
		  int jSize = items_[i].keys_.count();
		  //printf("=====i=%d, size=%d\n",i, jSize);
		  for (int j = 0; j < jSize; j++)
		  {
			  String *key = (String *)(items_[i].keys_.get(j));
			  Num *val = (Num *)(items_[i].vals_.get(j));
			  printf("word=%s, count=%d\n", key->c_str(), val->v);
		  }
	  }
  }
};
