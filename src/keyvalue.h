// A1: Part 2
// lang: CwC
#pragma once
#include "object.h"
#include "string.h"
#include "dataframe.h"

//A key consists of a string and a home node. The key is used for searching, 
//the home node describes which KV store owns the data
class Key : public Object 
{
public:
  char* key_;   	// key
  size_t nodeId_;  // id of kv store

  // constructor
  Key(const char* s, int n)
  {
    this->key_ = new char[strlen(s)];
    strcpy(this->key_, s);
    this->nodeId_ = n;
  }

  Key() {}

  // copy constructor
  Key(Key* k) {
    this->key_ = new char[strlen(k->key_)];
    strcpy(this->key_, k->key_);
    this->nodeId_ = k->nodeId_;
  }

  // destructor
  ~Key() { if (this->key_) delete [] this->key_; }
  void setKey(char* kstring) { this->key_ = kstring; }
  void setNode(size_t id) { this->nodeId_ = id; }
  char* getKey() { return this->key_; }
  size_t getNode() { return this->nodeId_; }
  size_t home() { return this->nodeId_; }
  void printKey() {
    printf("printKey(): key=%s, nodeId = %d\n", this->key_, this->nodeId_);
  }

  size_t hash() {
      size_t hash = 0;
      for (size_t i = 0; i < strlen(key_); ++i)
          hash = key_[i] + (hash << 6) + (hash << 16) - hash;
      return hash;
  }
}; //Key

/*************************
 *  key and value pair of Key and dataframe
 * ***********************/
class DFKeyValue : public Object
{
public:
	Key *key;
	DataFrame* value;

	//DFKeyValue(Key *k, DataFrame *v) : key(k), value(v) {} 
  DFKeyValue(Key *k, DataFrame *v)
	{
		key = new Key(k);
    //this->key = k;
    this->value = v;
    //value = new DataFrame(v);
	}
  
	~DFKeyValue() {}
	Key *getKey() { return this->key; }           // get map key
	DataFrame *getValue() { return this->value; } // get map value
	size_t hash()	{ return this->key->hash();	}
}; // DFKeyValue

// string- size_t key-value pair
class STKeyValue : public Object
{
public:
	String key_;
	size_t value_;

	STKeyValue(String *k, size_t v) : key_(k->c_str()), value_(v) {}
	~STKeyValue() {}
	String *getKey() { return &this->key_; } // Gets the key of the pair
	size_t getValue() { return this->value_; } 	// Gets the value of the pair
	size_t hash() {	return this->key_.hash();	}
};

/*// KeyBuff class - array of keys
class KeyBuff: public Object {
public:
    Array keys_;

    KeyBuff(Key* key) {
        keys_.append(key);
    }

    Key* get(size_t idx) {
        return dynamic_cast<Key*>(keys_.get(idx));
    }

    void push_back(Key* key){
        keys_.append(key);
    }
};
*/
class KeyBuff : public Object {                                                  
  public:                                                                        
  Key* orig_; // external                                                        
  StrBuff buf_;                                                                  
                                                                                 
  KeyBuff(Key* orig) : orig_(orig), buf_(orig->getKey()) {}                               
  //KeyBuff(Key* orig) : orig_(orig) {}                               
                                                                                 
  KeyBuff& c(String &s) { buf_.c(s); return *this;  }                            
  KeyBuff& c(size_t v) { buf_.c(v); return *this; }                              
  KeyBuff& c(const char* v) { buf_.c(v); return *this; }                         
                                                                                 
  Key* get() {                                                          
    String* s = buf_.get();                                                  
    buf_.c(orig_->getKey());  //////////????????????????????????                                                    
    Key* k = new Key(s->steal(), orig_->home());                                 
    delete s;                                                                    
    return k;                                                                    
  }                                                                              
}; // KeyBuff     