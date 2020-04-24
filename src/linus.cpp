/**
 * The input data is a processed extract from GitHub.
 *
 * projects:  I x S   --  The first field is a project id (or pid).
 *                    --  The second field is that project's name.
 *                    --  In a well-formed dataset the largest pid
 *                    --  is equal to the number of projects.
 *
 * users:    I x S    -- The first field is a user id, (or uid).
 *                    -- The second field is that user's name.
 *
 * commits: I x I x I -- The fields are pid, uid, uid', each row represent
 *                    -- a commit to project pid, written by user uid
 *                    -- and committed by user uid',
 **/
//lang: CwC

#include <cctype>
//#include <sys/socket.h>
//#include <arpa/inet.h>
#include <netinet/in.h> 
//#include <unistd.h> 
#include "dataframe.h"
#include "procArgs.h"
#include "application.h"
#include "network_ifc.h"
#include "keyvalue.h"
#include "map.h"
#include "string.h"
//#include "network_pseudo.h"
#include "network_ip.h"
#include "sorer.h"

int linus_debug = 1;
ProcArgs args;

/**************************************************************************
 * A bit set contains size() booleans that are initialize to false and can
 * be set to true with the set() method. The test() method returns the
 * value. Does not grow.
 ************************************************************************/
class Set {
public:  
  bool* vals_;  // owned; data
  size_t size_; // number of elements

  /** Creates a set of the same size as the dataframe. */ 
  Set(DataFrame* df) : Set(df->nrows()) {}

  /** Creates a set of the given size. */
  Set(size_t sz) :  vals_(new bool[sz+1]), size_(sz) {
      for(size_t i = 0; i <= size_; i++)
          vals_[i] = false; 
  }

  ~Set() { delete[] vals_; }

  /** Add idx to the set. If idx is out of bound, ignore it.  Out of bound
   *  values can occur if there are references to pids or uids in commits
   *  that did not appear in projects or users.
   */
  void set(size_t idx) {
    if (idx > size_ ) return; // ignoring out of bound writes
    vals_[idx] = true;       
  }

  /** Is idx in the set?  See comment for set(). */
  bool test(size_t idx) {
    if (idx > size_) return true; // ignoring out of bound reads
    return vals_[idx];
  }

  size_t size() { return size_; }

  // return number of element with value = true
  int trueNumber()
  {
    int counter = 0;
    for (int i=0; i<=size_; i++) {
      if (vals_[i]==1) counter++;
    }
    return counter;
  }

  /** Performs set union in place. */
  void union_(Set& from) {
    for (size_t i = 0; i <= from.size_; i++)
      if (from.test(i)) set(i);
  }

  void printSet() {
    for (int i=0; i<=this->size_; i++) printf("      %d, %d\n", i, vals_[i]);
  }
};


/*******************************************************************************
 * A SetUpdater is a reader that gets the first column of the data frame and
 * sets the corresponding value in the given set.
 ******************************************************************************/
class SetUpdater : public Reader {
public:
  Set& set_; // set to update
  
  SetUpdater(Set& set): set_(set) {}

  /** Assume a row with at least one column of type I. Assumes that there
   * are no missing. Reads the value and sets the corresponding position.
   * The return value is irrelevant here. */
  //bool visit(Row & row) { set_.set(row.get_int(0));  return false; }
  void visit(Row & row) { 
    //printf(" ---------------get_int(0)=%d\n", row.get_int(0));
    set_.set(row.get_int(0));  
    return;
  }
};

/*****************************************************************************
 * A SetWriter copies all the values present in the set into a one-column
 * dataframe. The data contains all the values in the set. The dataframe has
 * at least one integer column.
 ****************************************************************************/
class SetWriter: public Writer {
public:
  Set& set_; // set to read from
  int i_ = 0;  // position in set

  SetWriter(Set& set): set_(set) { }

  /** Skip over false values and stop when the entire set has been seen */
  bool done() {
    while (i_ < set_.size_ && set_.test(i_) == false) ++i_; //skip all set elements with false value
    return i_ == set_.size_;
  }

  void visit(Row & row) { row.set(0, i_++); }
};

/***************************************************************************
 * The ProjectTagger is a reader that is mapped over commits, and marks all
 * of the projects to which a collaborator of Linus committed as an author.
 * The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the identifier of a project and the uids are the
 * identifiers of the author and committer. If the author is a collaborator
 * of Linus, then the project is added to the set. If the project was
 * already tagged then it is not added to the set of newProjects.
 *************************************************************************/
class ProjectsTagger : public Reader {
public:
  Set& uSet;          // set of collaborator 
  Set& pSet;          // set of projects of collaborators
  Set newProjects;    // newly tagged collaborator projects

  ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
    uSet(uSet), pSet(pSet), newProjects(proj) {}

  /** The data frame must have at least two integer columns. The newProject
   * set keeps track of projects that were newly tagged (they will have to
   * be communicated to other nodes). */
  void visit(Row & row) override {
    int pid = row.get_int(0);
    int uid = row.get_int(1);
    if (uSet.test(uid))
      if (!pSet.test(pid))
      {
        pSet.set(pid);
        newProjects.set(pid);
      }
    return;
  }
};

/***************************************************************************
 * The UserTagger is a reader that is mapped over commits, and marks all of
 * the users which commmitted to a project to which a collaborator of Linus
 * also committed as an author. The commit dataframe has the form:
 *    pid x uid x uid
 * where the pid is the idefntifier of a project and the uids are the
 * identifiers of the author and committer. 
 *************************************************************************/
class UsersTagger : public Reader {
public:
  Set& pSet;
  Set& uSet;
  Set newUsers;

  UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
    pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }

  void visit(Row &row) override
  {
    int pid = row.get_int(0);
    int uid = row.get_int(2);
    if (pSet.test(pid))
      if (!uSet.test(uid))
      {
        uSet.set(uid);
        newUsers.set(uid);
      }
    return;
  }
};

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
  int DEGREES = 4;                                 // How many degrees of separation form linus?
  int LINUS = 1;//4967;                                 // The uid of Linus (offset in the user df)
  //const char* PROJ = "datasets/projects.ltgt";
  //const char* USER = "datasets/users.ltgt";
  //const char* COMM = "datasets/commits.ltgt";
  const char* PROJ = "datasets/tproj.txt";
  const char* USER = "datasets/tuser.txt";
  const char* COMM = "datasets/tcom.txt";
  DataFrame* projects;                              //  pid x project name
  DataFrame* users;                                 // uid x user name
  DataFrame* commits;                               // pid x uid x uid 
  Set* uSet;                                        // Linus' collaborators
  Set* pSet;                                        // projects of collaborators

  Linus(size_t idx, NetworkIfc* net): Application(idx, net) {}

  /************** 
   * Compute DEGREES of Linus.  
   * */
  void run_() override {
    printf("=====run(): readInput()=====\n");
    readInput();

    for (size_t i = 0; i < DEGREES; i++) {
      printf("=====run(): step(%zu)=====\n", i);
      step(i);
    }
  }

  /******************
   * Node 0 reads three files, cointainng projects, users and commits, and
   *  creates thre dataframes. All other nodes wait and load the three
   *  dataframes. Once we know the size of users and projects, we create
   *  sets of each (uSet and pSet). We also output a data frame with a the
   *  'tagged' users. At this point the dataframe consists of only
   *  Linus. 
   * **/
  void readInput() {
    Key pK("projs", 0);           // key for projects
    Key uK("usrs", 0);            // key for users
    Key cK("comts", 0);           // key for commits

    // if it is node 0, 1. read input files and save to kvstore with key pK, uK and cK. 
    //  then 2. split data among other nodes, and send them out
    if (this_node() == 0) {
      pln("===Reading...\n");

      ///////// read input file projects, and save it to dataframe projects
      projects = DataFrame::fromFile((char*)PROJ, &pK, &this->stores_);
      p("    ").p(projects->nrows()).pln(" projects");
      //projects->print_dataframe();

      // send project dataframe to other nodes
      for (int i = 0; i < args.numNodes_; i++)
      {
        if (i!= 0) {           // send the splitted user data from node 0 to other nodes
          if (linus_debug) printf("  ===linus::run(): node 0 sending project data to node %d...\n", i);               
          Put *putVal = new Put(0, i, projects, &pK);   // create a put message
          this->net_->send_msg(putVal);                  // send the message
        }
      }

      ////////// read in user input file, and save it to dataframe users
      users = DataFrame::fromFile((char*)USER, &uK, &this->stores_);
      p("    ").p(users->nrows()).pln(" users");
      //users->print_dataframe();

      // send users dataframe to other nodes
      for (int i = 0; i < args.numNodes_; i++)
      {
        if (i!= 0) {              // send the dataframe from node 0 to other nodes
          if (linus_debug) printf("  ===linus::run(): node 0 sending user data to node %d...\n", i);               
          Put *putVal = new Put(0, i, users, &uK);  // create a put message
          this->net_->send_msg(putVal);                 // send the message
        }
      }

      ////////// read input file commits, and save it to dataframe commits
      commits = DataFrame::fromFile((char*)COMM, &cK, &this->stores_);
       p("    ").p(commits->nrows()).pln(" commits");
      //commits->print_dataframe();
       
       // split commits data////////////////
      Schema sch;
      DataFrame *dfComm[args.numNodes_];   // create a dataframe for each node
      IntColumn* mcol0[args.numNodes_];     // commits dataframe column0
      IntColumn* mcol1[args.numNodes_];     // commits dataframe column1
      IntColumn* mcol2[args.numNodes_];     // commits dataframe column2

      // allocate memory for all columns
      for (int i = 0; i < args.numNodes_; i++) {
        mcol0[i] = new IntColumn();
        mcol1[i] = new IntColumn();
        mcol2[i] = new IntColumn();
      }
      for (int i=0; i<commits->nrows(); i++) {
        int j = i % args.numNodes_;
        mcol0[j]->push_back(commits->get_int(0, i));  //get column0, and save to column mcol0
        mcol1[j]->push_back(commits->get_int(1, i));  //get column1, and save to column mcol1
        mcol2[j]->push_back(commits->get_int(2, i));  //get column2, and save to column mcol2
      }
      // for each node, 1. create commits dataframe. 2. add columns to dataframe 3.send to other node
      for (int i = 0; i < args.numNodes_; i++)
      {
        dfComm[i] = new DataFrame(sch);
        dfComm[i]->add_column(mcol0[i]);  
        dfComm[i]->add_column(mcol1[i]);  
        dfComm[i]->add_column(mcol2[i]);  
        if (i!= 0) {           // send the split commit data from node 0 to other nodes
          if (linus_debug) printf("  ===linus::run(): node 0 sending commit data to node %d...\n", i);               
          Put *putVal = new Put(0, i, dfComm[i], &cK);   // create a put message
          this->net_->send_msg(putVal);                  // send the message
        }
      }
      stores_.put(&cK, dfComm[0]);       // reset node 0 store with its share
      commits = dfComm[0];

       ///////// create dataframes with 0 user key as users-0-0, and save it to kvstore
       printf("   ===saving linus to kvstore...\n");
       Key* user0K = new Key("users-0-0", 0);
       DataFrame* df = DataFrame::fromScalarInt(user0K, &this->stores_, LINUS);

      /////// send zero user's key(linus) to other nodes
      for (int i = 1; i < args.numNodes_; i++)
      {
        if (linus_debug) printf("  ===linus::run(): node 0 sending user0 data to node %d...\n", i); 
        Put *putVal = new Put(0, i, df, user0K); //send df from node 0 to node i with key user0K
        this->net_->send_msg(putVal);            // send the message
      }
    }
    else
    {
      printf("======== node %d =====\n", this_node());
       projects = dynamic_cast<DataFrame*>(this->waitAndGet(&pK));
       users = dynamic_cast<DataFrame*>(this->waitAndGet(&uK));
       commits = dynamic_cast<DataFrame*>(this->waitAndGet(&cK));
    }
    uSet = new Set(users);
    pSet = new Set(projects);

    // for debugging, print out the split data
    if (linus_debug)
    {
      printf("  ===userss :\n");
      users->print_dataframe();

      printf("  ===projects :\n");
      projects->print_dataframe();

      printf("  ===commits :\n");
      commits->print_dataframe();
    }
 }
/*********************
 * constuct a key string. e.g. user-0-0
 * */
 Key* mk_key(size_t stage, size_t idx, const char* str) {
      char keyStr[16];
      sprintf(keyStr, "%s-%zu-%zu", str, stage, idx);
      Key* k = new Key(keyStr, idx);
      //printf("===Created key=%s\n", k->getKey());
      return k;
  }

 /************************************************
  *  Performs a step of the linus calculation. It operates over the three
  *  datafrrames (projects, users, commits), the sets of tagged users and
  *  projects, and the users added in the previous round. 
  * ****/
  void step(int stage) {
    p("Stage ").pln(stage);
    Key* uK = mk_key(stage, 0, "users");      // create user key for each stage(step)
  
    // wait and get the dataframe with all the users added on in the previous round
    DataFrame *newUsers =  dynamic_cast<DataFrame *>(this->waitAndGet(uK));
    if (linus_debug) {
      printf("\n    recieved new dataframe:\n");
      newUsers->print_dataframe();
    }
    //users->print_dataframe();
    //projects->print_dataframe();
    //commits->print_dataframe();

    // 1. mark project set with user information ///////////////////
    // use users dataframe to create set delta with bit set as false for all entries
    Set delta(users);
    if (linus_debug) { printf("    delta set:\n"); delta.printSet();}

    // now change the entry in delta as true if the entry's index is in newUsers
    SetUpdater upd(delta); 
    newUsers->map(upd);           // marking entry in delta
    //delete newUsers;
    if (linus_debug) { printf("    upd set:\n"); upd.set_.printSet(); }

    // change pSet entry to true if the entry's index is a project that true users in delta work on
    ProjectsTagger ptagger(delta, *pSet, projects);
    commits->local_map(&ptagger);         // marking all projects touched by delta

    // merge updated project set from all nodes, and save the project index in dataframe
    // then send the updated dataframe to all nodes
    merge(ptagger.newProjects, "projects", stage);

    // obtain the updated project in bit set pSet
    pSet->union_(ptagger.newProjects); 
    if (linus_debug) { printf("    ===union()... pSet:\n"); pSet->printSet(); }

    // 2. mark user set with project information ///////////////////
    if (linus_debug) { printf("     before tagging newProject set:\n"); ptagger.newProjects.printSet(); }
    if (linus_debug) { printf("     before tagging user set:\n"); uSet->printSet(); }
    UsersTagger utagger(ptagger.newProjects, *uSet, users);
    commits->local_map(&utagger);
    if (linus_debug) { printf("     after tagging newProject set:\n"); ptagger.newProjects.printSet(); }
    if (linus_debug) { printf("     after tagging user set:\n"); uSet->printSet(); }

    merge(utagger.newUsers, "users", stage + 1);
    uSet->union_(utagger.newUsers);
    if (linus_debug)
    {
      printf("    after merge() and union() uSet:\n");
      uSet->printSet();
    }
    p("    after stage ").p(stage).pln(":");
    p("        tagged projects: ").pln(pSet->trueNumber()); //pln(pSet->size());
    p("        tagged users: ").pln(uSet->trueNumber());  //pln(uSet->size());
  }

  /** Gather updates to the given set from all the nodes in the systems.
   * The union of those updates is then published as a dataframe. The key
   * used for the output is of the form "name-stage-0" where name is either
   * 'users' or 'projects', stage is the degree of separation being
   * computed.
   */
  void merge(Set &set, char const *name, int stage)
  {
    if (linus_debug) printf("   =====merge() ...\n");
    if (this_node() == 0)
    {
      // loop through each node, and collect updates for key nK, and update passed in set
      for (size_t i = 1; i < args.numNodes_; ++i)
      {
        Key *nK = mk_key(stage, i, name);
        DataFrame *delta = dynamic_cast<DataFrame *>(this->waitAndGet(nK));
        p("    received delta of ").p(delta->nrows()).p(" elements from node ").pln(i);
        if (linus_debug) delta->print_dataframe();

        // update passed in set with collected data
        if (linus_debug) { printf("     upd set before update:\n"); set.printSet();}
        SetUpdater upd(set);
        delta->map(upd);  
        if (linus_debug) { printf("     upd set after update:\n"); upd.set_.printSet();}
        //delete delta;
      }

      // need to save project update to kvstore
      Key *k = mk_key(stage, 0, name);   // make a key first
      if (linus_debug) printf("    key=%s\n", k->key_);
      SetWriter writer(set);            // setup set in setWriter
      char colType[2];
      strcpy(colType, "I");

      // now, transform marked element in set to index, and save in dataframe and kvstore with key k
      DataFrame *df = DataFrame::fromVisitor(k, &this->stores_, colType, &writer);
      if (linus_debug) { printf("     touched projects:\n"); df->print_dataframe();}

      // send the result to other nodes
      if (linus_debug) printf("     sending touched projects to other nodes with key=%s\n", k->key_);
      for (size_t i = 1; i < args.numNodes_; i++)
      {
        Put *putVal = new Put(0, i, df, k); // create a put message
        this->net_->send_msg(putVal);       // send
      }
    }
    else
    {
      // prepare set writer with marked project elements
      //p("    sending ").p(set.size()).pln(" elements to master node");
      SetWriter writer(set);
      Key *k = mk_key(stage, this_node(), name);
      if (linus_debug) printf("     kek=%s\n", k->key_);
      char colType[2];
      strcpy(colType, "I");

      // transform marks in set writer to marked element index, 
      // and save index in a 1 column dataframe and kvstore with key k
      DataFrame *df_writer = DataFrame::fromVisitor(k, &this->stores_, colType, &writer);
      if (linus_debug) df_writer->print_dataframe();

      // send the dataframe with project element index result to node 0
      Put *putVal = new Put(this_node(), 0, df_writer, k); // create a put message
      this->net_->send_msg(putVal);                        // send the message

      // wait and get counting result from node 0
      Key *mK = mk_key(stage, 0, name);
      DataFrame *merged = dynamic_cast<DataFrame *>(this->waitAndGet(mK));
      p("    receiving ").p(merged->nrows()).pln(" merged elements");
      if (linus_debug) merged->print_dataframe();

      // merge updated result to passed in set
      SetUpdater upd(set);
      merged->map(upd);
      //delete merged;
    }
  }
}; // end Linus

/************************************
 * static method of DataFrame : read in file filename, and save to KVstore store with Key key
 ***/
DataFrame* DataFrame::fromFile(char* filename, Key *key, KVStore* store)
{
  printf("  file name =%s\n", filename);
  Sorer *sorer = new Sorer(filename);

  // get number of columns in the input file
  int numOfCol = sorer->calcNumberOfColumns();
  if (linus_debug) printf("   numOfCol= %d\n", numOfCol);

  // it's time to find column type
  COL_TYPE *colType = sorer->getColTypes();
  if (linus_debug)
    for (int i = 0; i < numOfCol; i++)
      printf("    column %d: type=%d\n", i, colType[i]);

  // build dataframe
  DataFrame *df = sorer->loadDataframe();
  store->put(key, df);
  //df->print_dataframe();
  return df;
}

/*************************************
 * static method of DataFrame : Save scalar integer data to KVstore store with Key key
 ***/
DataFrame* DataFrame::fromScalarInt(Key* key, KVStore* store, int data)
{
  Schema s;
  DataFrame* frame = new DataFrame(s);
  Column *col = new IntColumn();
  col->push_back(data);
  frame->add_column(col);
  store->put(key, frame);
  return frame;
}

/*************************************
 * static method of DataFrame : Visit a row and update the Set upd accordingly
 ***/
void DataFrame::map(SetUpdater &upd)
{
  Row row(get_schema());
  for (int i = 0; i < nrows(); i++)
  {
    fill_row(i, row);
    upd.visit(row);
  }
}

/*************************************
 * static method of DataFrame : Visit a row and update the ProjectsTagger a with the user Set information
 ***/
void DataFrame::local_map(ProjectsTagger *a)
{
  Row r(this->get_schema());
  // for each row in this dataframe
  for (int i = 0; i < this->nrows(); i++)
  {
    this->fill_row(i, r);
    a->visit(r);
  }
}

/*************************************
 * static method of DataFrame : Visit a row and update the UsersTagger a with the projects Set information
 ***/
void DataFrame::local_map(UsersTagger *a)
{
  Row r(this->get_schema());
  // for each row in this dataframe
  for (int i = 0; i < this->nrows(); i++)
  {
    this->fill_row(i, r);
    a->visit(r);
  }
}

/*************************************
 * static method of DataFrame : SetWriter writer is a visitor transformed into a dataframe with a colType column 
 * that is then saved into the store with Key key
 ***/
DataFrame* DataFrame::fromVisitor(Key *key, KVStore* store, char* colType, SetWriter* writer)
{
  Schema sch(colType);
  DataFrame *df = new DataFrame(sch);

  // for each item in summer->map_ which contains the word and word count
  while (!writer->done())
  {
    Row row(sch);
    writer->visit(row); // save key and value to row from summer->map_
    df->add_row(row);   // add the row to df
  }
  //df->print_dataframe();
  //store->printMap();
  store->put(key, df); // save word/count result dataframe to store with key key
  return df;
}

/*************************************
 * main
 * ***********************************/
int main(int argc, char **argv)
{ 
  // verify number of nodes
  args.parser(argc, argv);
  args.checkArgs();

  sockaddr_in addr;
  NetworkIp nt(addr);
  if (args.nodeId_ == 0)
    nt.server_init(args.nodeId_, args.myPort_);
  else 
  {
    args.checkClientArgs();
    nt.client_init(args.nodeId_, args.myPort_, args.masterIp_, args.masterPort_);
  }

  Linus Linus(args.nodeId_, &nt);
  Linus.run_();
  printf("Linus 4 degree finished\n");

  return 0;
}
