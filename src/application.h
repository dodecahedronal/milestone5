//lang::CwC
#pragma once

#include "helper.h"
#include "object.h"
#include "string.h"
#include "map.h"
#include "keyvalue.h"
//#include "network.h"
#include "thread.h"
#include "network_ifc.h"

class DataFrame;

#define NUM_NODES 3

//////////////////////////////////////
// Each KV store has part of the distributed data. All of the networking
// and concurrency control is hidden here.  
class KVStore : public Object
{
public:
  DFMap map_;
  size_t nodeId_;
  Lock lock_;     // lock for atomic pop/push

  //constructor
  KVStore(size_t nodeId) : map_(), nodeId_(nodeId) {}

  // destructor
  //~KVStore(){ map_.clear();}
  ~KVStore(){ }

  // hash value v to key k
  void put(Key* k, DataFrame* v) {
    Key* key = new Key(k->key_, nodeId_);

    //if (nodeId_ != k->nodeId_) 
    //{
    //  printf("Given node ID %d does not match the current node %d\n", key->nodeId_, nodeId_);
    //  return;
    //}
    //this->lock_.lock();
    //map_.set(key, v);
    if (map_.get(k))
    {
      DataFrame *old = map_.remove(k);
      if (old)
        delete old;
    }
    map_.set(key, v);
    //printf("after put------------\n");
    //map_.printMap();
    //this->lock_.notify_all();
    //this->lock_.unlock();

    //delete key;
  } 

  // retrieve value by giving key k
  DataFrame* get(Key* k) {
      return map_.get(k);
  }
  /*  DataFrame* get(Key* k) {
    if (k->nodeId_ == this->nodeId_) 
      return map_.get(k);
    else
      return waitAndGet(k);
  }
*/
  // retrieve value by giving key k
  // TODO: networking
  DataFrame* waitAndGet(Key* k) 
  {

    lock_.lock(); // lock
    while (map_.get(k) == nullptr)
      lock_.wait(); // wait if get nothing
    lock_.unlock();
    return map_.get(k);
  }

  void printMap() {
    this->map_.printMap();
  }
  void printMap(Key* k) {
    this->map_.printMap(k);
  }
};

// from dataframe.h
DataFrame *DataFrame::fromArray(Key *key, KVStore *store, size_t size, double *data)
{
  Schema s;
  DataFrame *frame = new DataFrame(s);
  Column *col = new DoubleColumn();

  for (int i = 0; i < size; i++)
    col->push_back(data[i]);

  frame->add_column(col, new String("col"));
  store->put(key, frame);

  // store->printMap();

  return frame;
}

// from dataframe.h
DataFrame* DataFrame::fromScalar(Key* key, KVStore* store, double data)
{
  Schema s;
  DataFrame* frame = new DataFrame(s);
  Column *col = new DoubleColumn();
  col->push_back(data);

  frame->add_column(col, new String("col"));

  store->put(key, frame);

  return frame;
}


// abstractions for distributing data and user runs
class Application : public Object
{
public:
  size_t idx_;
  KVStore stores_;
  NetworkIfc* net_;

  // constructor
  Application(size_t idx, NetworkIfc* net) : idx_(idx), net_(net), stores_(idx) {}
  
  // return the node id
  int this_node() { return this->idx_; }

  // virtual run method. users will override the method like in Demo class
  virtual void run_() {}
};


