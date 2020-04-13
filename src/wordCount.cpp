//lang: CwC

#include <cctype>
#include "dataframe.h"
#include "procArgs.h"
#include "application.h"
#include "network_ifc.h"
#include "keyvalue.h"
#include "map.h"
#include "string.h"
#include "network_pseudo.h"

ProcArgs args;

/*************************************
 * inherits Rower class:
 * read in input file, and load each word as a row.
 * ***********************************/
class FileReader : public Writer {
public:
  static const size_t BUFSIZE = 1024;
  char *buf_;      // buffer for readins
  size_t end_ = 0; // length of readins
  size_t i_ = 0;   // current index of readins
  FILE *file_;     // file pointer of readins

  /*****
   *  Reads next word and stores it in the row. Actually read the word.
   *  While reading the word, we may have to re-fill the buffer  
   * */
  void visit(Row &r) override
  {
    assert(i_ < end_);
    assert(!isspace(buf_[i_]));
    size_t wStart = i_;
    while (true)
    {
      if (i_ == end_)
      {
        if (feof(file_))
        {
          ++i_;
          break;
        }
        i_ = wStart;
        wStart = 0;
        fillBuffer_();
      }
      if (isspace(buf_[i_])) break;
      ++i_;
    }
    buf_[i_] = 0;
    //String word(buf_ + wStart, i_ - wStart);
    String *word = new String(buf_ + wStart, i_ - wStart);
    word->stripStr(); // replace special characters in word. e.g. ,;.!?
    r.set(0, word);
    ++i_;
    skipWhitespace_();
  }

  /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
  bool done() override { return (i_ >= end_) && feof(file_); }

  /** Creates the reader and opens the file for reading.  */
  FileReader()
  {
    file_ = fopen(args.file_, "r");
    if (file_ == nullptr)
      printf("Cannot open file %s\n", args.file_);
    buf_ = new char[BUFSIZE + 1]; //  null terminator
    fillBuffer_();
    skipWhitespace_();
  }

  /** Reads more data from the file. */
  void fillBuffer_()
  {
    size_t start = 0;
    // compact unprocessed stream
    if (i_ != end_)
    {
      start = end_ - i_;
      memcpy(buf_, buf_ + i_, start);
    }
    // read more contents
    end_ = start + fread(buf_ + start, sizeof(char), BUFSIZE - start, file_);
    i_ = start;
  }

  /** Skips spaces.  Note that this may need to fill the buffer if the
        last character of the buffer is space itself.  */
  void skipWhitespace_()
  {
    while (true)
    {
      if (i_ == end_)
      {
        if (feof(file_))
          return;
        fillBuffer_();
      }
      if (!isspace(buf_[i_])) return;   // if the current character is not whitespace, we are done
      ++i_;                             // otherwise skip it
    }
  }
 };
 
/*****************************************
 * Adder inherits from Rower class:
 * count each word(row), and save words(keys) and counts(values) to map_
 ************************************/
class Adder : public Reader {
public:
  SIMap& map_;  // String to Num map;  Num holds an int
 
  Adder(SIMap& map) : map_(map)  {}
 
  void visit(Row& r) override {
    String* word = r.get_string(0);
    assert(word != nullptr);
    Num* num = map_.get(*word) ? map_.get(*word) : new Num();
    assert(num != nullptr);
    num->v++;
    map_.set(*word, num);  // save word and num to map_
    //return false;
  }
};
 
/********************************************
 * Summer inherits from Rower class:
 *  load word count pair to row
 * ******************************************/
class Summer : public Writer {
public:
  SIMap& map_;
  size_t i = 0;     //index of map_
  size_t j = 0;     // index of key array
  size_t seen = 0;  // counter of the seeing key
 
  Summer(SIMap& map) : map_(map) {}
 
  // find the next entry in map_
  void next() {
      if (i == map_.capacity_ ) return;
      // if key array is not done, see next key
      if ( (j+1) < map_.items_[i].keys_.count() ) {
          j++;
          ++seen; // keep counting with j till reach keys_.count()
      } else {  
          ++i;    // otherwise, go to next item in map_
          j = 0;  // reset key array index
          // if map_ is not finished, but there is no items in map_[i], skip it
          while( i < map_.capacity_ && map_.items_[i].keys_.count() == 0 )  i++;
          if (k()) ++seen; // see a key, increase seen by 1
      }
  }
 
 // return the jth key of map_[i]
  String* k() {
      if (i==map_.capacity_ || j == map_.items_[i].keys_.count()) return nullptr;
      return (String*) (map_.items_[i].keys_.get(j));
  }
 
  // return the jth value of map_[i]
  size_t v() {
      if (i == map_.capacity_ || j == map_.items_[i].keys_.count()) {
          assert(false); return 0;
      }
      return ((Num*)(map_.items_[i].vals_.get(j)))->v;
  }
 
  // put words(keys) and counts(values) in map_ to Row r
  void visit(Row& r) {
      //if (!k()) next();
      next();
      String &key = *k();
      size_t value = v();
      r.set(0, &key);
      r.set(1, (int)value);
      //next();
  }
 
 // print summer class map_
  void print() {
    for (int i=0; i<map_.capacity_; i++) {
      int jSize = map_.items_[i].keys_.count();
      for (int j=0;j<jSize;j++){
        String* key = (String*)(map_.items_[i].keys_.get(j));
        Num* val = (Num*)(map_.items_[i].vals_.get(j));
        printf("key=%s,val=%d\n",key->c_str(), val->v);
      }
    }
  }
  // loop through map_
  bool done() {return seen == map_.size(); }
};
 
/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount: public Application {
public:
  static const size_t BUFSIZE = 1024;
  Key keyData_;           // hold the key for orginal data from input file. 
  char keyResultStr_[10]; // hold the key for word count result dataframe
  SIMap all;
 
  // constructor with node id and network
  WordCount(size_t idx, NetworkIfc* net):
    Application(idx, net), keyData_("data", idx), keyResultStr_("result-") { }

  /******
   * master node 1.reads the input. 2.splits the data. 3.sends to each node. 4. call local_count & reduce()
   * ****/
  void run_() override {
    // register node 
    this->net_->register_node(this_node());

    // node 0 read in the input file, and split it by number of nodes. Then send them to other nodes.
    if (this_node() == 0) {
      FileReader fr;                       // read input file and save it to node 0's kvstore
      char colTypes[2];
      strcpy(colTypes, "S");
      DataFrame* df = DataFrame::fromVisitor(&keyData_, &this->stores_, colTypes, &fr);
      //this->stores_.printMap();

      Schema s;
      DataFrame *dfOut[args.numNodes_];   // create a dataframe for each node
      StringColumn *scol[args.numNodes_]; // each dataframe has a string column
      String *colName = new String("words");
      for (int i = 0; i < args.numNodes_; i++) scol[i] = new StringColumn();

      // loop through each word, and assign to each node in turns
      int j = 0;
      for (int i=0; i<df->nrows(); i++) {
        String* data = df->get_string(0, i);
        int jj = j % args.numNodes_;
        scol[jj]->push_back(new String(data->c_str()));
        j++;
      }

      // for each node, send the splited dataframe to each node
      for (int i = 0; i < args.numNodes_; i++)
      {
        dfOut[i] = new DataFrame(s);
        dfOut[i]->add_column(scol[i], colName);  
        //printf("====node %d======\n", i);
        //dfOut[i]->print_dataframe();
        if (i!= 0) {                          
          Put *putVal = new Put(this_node(), i, dfOut[i], &keyData_);   // create a put message
          this->net_->send_msg(putVal);                                 // send the message
        }
      }
      stores_.put(&keyData_, dfOut[0]);         // reset node 0 store with its share
    } // if (node0)
    local_count();                            // every node starts counting words in its own kvstore
    reduce();                                 // combine count result from all nodes
  }
 
  /*****
   * Returns a key for given node.  These keys are homed on master node 
   * which then joins them one by one. e.g. wc-map-0, wc-map-1, etc.
   * */
  Key* mk_key(size_t idx) {
      char keyStr[16];
      sprintf(keyStr, "%s%d", this->keyResultStr_, idx);
      Key* k = new Key(keyStr, idx);
      //printf("===Created key=%s\n", k->getKey());
      return k;
  }
 
  /******* 
   * Compute word counts on the local node and build a data frame. 
   * */
  void local_count() {
    if (this_node() == 0)
    {
      DataFrame *words0 = (this->stores_.get(&keyData_)); // get data from kvstore

      // count data in dataframe words0
      SIMap map;
      Adder add(map);
      words0->local_map(&add);  
      ////////delete words0;  // !!!!

      // filter out empty entries in map, and save the result map to kvstore
      Summer cnt(map);  
      char colTypes[5];
      strcpy(colTypes, "SI");
      DataFrame* d =  DataFrame::fromVisitor(mk_key(this_node()), &this->stores_, colTypes, &cnt);
      /////////delete d; /// what a mistake
      return;
    }

    // for all other nodes, wait to recieve its chunk of data, and save it to its own kvstore
    bool done = false;
    while (!done)
    {
      Message *msg = this->net_->recv_msg();
      if (!msg) continue;                           // if the message is not arrives, wait
      if ( msg->kind_ == MsgKind::Put) {            // read message and save it to kvstore
        Put *putMsg = dynamic_cast<Put *>(msg);
        this->stores_.put(putMsg->key_, putMsg->data_);
      }

      // get the data from kvstore. if successfully get it, count it and send result back to node 0.
      //DataFrame *words = (this->stores_.waitAndGet(&in));
      DataFrame *words = (this->stores_.get(&keyData_));
      if (words)
      {
        SIMap map;
        Adder add(map);
        words->local_map(&add);   // counting
        ///////////delete words;
        /******
        if (this_node() == 1)
        {
          printf("====node %d local_count() result===\n", this_node());
          map.print();
        }
        *****/
        Summer cnt(map);
        char colTypes[5];
        strcpy(colTypes, "SI");        
        DataFrame* d = DataFrame::fromVisitor(mk_key(this_node()), &this->stores_, colTypes, &cnt);

        // send counting result dataframe d with key "wc-map-[node id]" from [node id] to node 0 
        Put *putVal = new Put(this_node(), 0, d, mk_key(this_node()));
        this->net_->send_msg(putVal);
        done = true;
        ///////delete d; 
      }
    }
    
  } //local_count()

  /********
   *  Merge the dataframes of all nodes 
   * */
  void reduce() {
    int numNodeProcessed = 0;
    if (this_node() != 0) return;     // onlu node 0 need to run reduce)()

    SIMap map;
    Key* own = mk_key(0);             // create key for count result map
    DataFrame *d = this->stores_.get(own);
    if (d)
    {
      merger(d, map);
      numNodeProcessed++;
    }
    //printf("=====merger node 0 to map======\n");
    //map.print();

    // wait and get the counting result dataframe from all other nodes
    bool done = false;
    while (!done)
    {
      Message *msg = this->net_->recv_msg();
      if (!msg) continue;                           // if the message is not arrives, wait
      if ( msg->kind_ == MsgKind::Put) {            // read message and save it to kvstore
        Put *putMsg = dynamic_cast<Put *>(msg);
        //this->stores_.put(putMsg->key_, putMsg->data_);
        DataFrame *words = putMsg->data_;

        // once recieves the message, merge it to map
        if (words) {
          merger(words, map);
          numNodeProcessed++;
          if (numNodeProcessed == args.numNodes_) // once recieved from all nodes, done
            done = true;
        }
      }
    }
    printf("\n===============Word Counting Result======================\n");
    map.print();
    printf("==============reduce done, map size=%d===================\n\n",map.size_);
  }
  /******
   * collect count result from each node, and save them to map
   * ******/
  void merger(DataFrame* df, SIMap& m)
  {
    for (int i = 0; i < df->nrows(); i++)
    {
      String *word = df->get_string(0, i);
      int count = df->get_int(1, i);
      assert(word != nullptr);      
      Num *num = m.get(*word);
      if (num)
        num->v += count;
      else
      {
        Num *num = new Num();
        num->v = count;
        m.set(*word, num);
      }
    }
  }
};
/************************************************
 * from dataframe.h : save value read from FileReader fr to kv store 
 * with key key. 
 * **********************************************/
DataFrame* DataFrame::fromVisitor(Key* key, KVStore* store, char* colType,  FileReader* fr)
{
  // create dataframe with pass in type sch as schema column types
  Schema sch(colType);
  DataFrame* df = new DataFrame(sch);
  int idx = 0;
  while (!fr->done())
  {
    Row row(sch);
    fr->visit(row);
    df->add_row(row);
    //df->print_dataframe();
  }
    store->put(key, df);
    //key->printKey();

    return df;
}

DataFrame* DataFrame::fromVisitor(Key *key, KVStore* store, char* colType, Summer* summer)
{
    Schema sch(colType);
    DataFrame* df = new DataFrame(sch);

  // for each item in summer->map_ which contains the word and word count 
  while (!summer->done()) {
    Row row(sch);
    summer->visit(row);  // save key and value to row from summer->map_
    df->add_row(row);     // add the row to df
  }
  //df->print_dataframe();
  //store->printMap();
  store->put(key, df);    // save word/count result dataframe to store with key key
  return df;
}
/*******************************************************
 * for each word in the dataframe, count each word
 * ******************************************************/
DataFrame *DataFrame::local_map(Adder *a)
{
  Row r(this->get_schema());
  // for each row in this dataframe
  for (int i = 0; i < this->nrows(); i++)
  {
    this->fill_row(i, r);
    //printf("-------------------r(%d)=%s\n", i, r.get_string(0)->c_str());
    // call adder's visit() to increase the count of the same word
    a->visit(r);
  }
}
/*******************************************
 * for word count application
 * ******************************************/
void DataFrame::map(Adder &add)
{
  Row row(get_schema());
  for (int i = 0; i < nrows(); i++)
  {
    fill_row(i, row);
    add.visit(row);
  }
}
/*************************************
 * main
 * ***********************************/
int main(int argc, char **argv)
{ 
  // verify number of nodes
  args.parser(argc, argv);
  if (args.numNodes_ != 3) {
    printf("Number of Nodes must be 3.\n");
    exit(1);
  }
  if (!args.file_) {
    printf("[-file] must be given.\n");
    exit(1);
  }

  NetworkPseudo nt;
  std::thread th[args.numNodes_];

  // create 3 applications
  WordCount t_node0(0, &nt);
  WordCount t_node1(1, &nt);
  WordCount t_node2(2, &nt);

  // 3 threads to run 3 applications - one for each
  th[0] = std::thread(&WordCount::run_, &t_node0);
  th[1] = std::thread(&WordCount::run_, &t_node1);
  th[2] = std::thread(&WordCount::run_, &t_node2);

  // join threads
  for (int i = 0; i < 3; i++) th[i].join();
 
  return 0;
}

