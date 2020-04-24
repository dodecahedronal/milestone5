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
int app_debug = 1;

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
    if (map_.get(k))
    {
      DataFrame *old = map_.remove(k);
      if (old)
        delete old;
    }
    map_.set(key, v);
  } 

  // retrieve value by giving key k
  DataFrame* get(Key* k) {
      return map_.get(k);
  }

  // retrieve value by giving key k
  // NOTE: not used
  /*DataFrame* waitAndGet(Key* k) 
  {

    lock_.lock(); // lock
    while (map_.get(k) == nullptr)
      lock_.wait(); // wait if get nothing
    lock_.unlock();
    return map_.get(k);
  }
  */
 
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

  //frame->add_column(col, new String("col"));
  frame->add_column(col);
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

  frame->add_column(col);

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
  
  // wait for dataframe with Key key to recieve, and save to kvstore
  DataFrame* waitAndGet(Key* key)
  {
    if (app_debug) printf("    Application::waitAndGet() with key=%s...\n", key->key_);
    //key->printKey();

    DataFrame *df = this->stores_.get(key);
    if (df) return df;

    bool done = false;
    while (!done)
    {
      Message *msg = this->net_->recv_msg();
      if (!msg)
        continue; // if the message is not arrives, wait
      if (msg->kind_ == MsgKind::Put)
      { // read message and save it to kvstore
        Put *putMsg = dynamic_cast<Put *>(msg);
        this->stores_.put(putMsg->key_, putMsg->data_);
      }
      df = (this->stores_.get(key));
      if (df)
        done = true;
    }
    return df;
  }

  // virtual run method. users will override the method like in Demo class
  virtual void run_() {}
};


