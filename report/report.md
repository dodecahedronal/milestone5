Introduction:
------------
The eau2 system is a query system for user-provided SOR data files. The maximum data file size is 100GB. The data is loaded in memory, then distributed over n processors. 

Though this submission is being turned in for Milestone 5, we have not completed the code for the "7 degrees of Linus" portion and instead focused on our technical debt from last week's project. The distributed key value store implementation and distributed word count functionality have been completed. Thus far it can only operate on a pseudo-network and not yet on an actual network. 

Architecture: 
---------------
There are 3 layers in the system design.
1. KV Store Layer
    * The networking and concurrency control are hidden in this layer
    * Each KV store has part of the data with the ability to exchange information to another KV store

2. Abstractions Layer for Distributing Data Frames
    * Loads data from data file by using dataframe calls
    * Distributes data to KV stores by calling KV store methods
    * Executes user queries and returns results
    
3. Application Layer
    * This layer holds the implementation for the functionality detailed in the abstraction layer and allows the users to run queries 


Implementation: 
---------------
The application (client interface) can operate on dataframes, either to put them into or get them from the key value store. Every node in the pseudo-network has a single key value store. Each key value store has a map of key-dataframe pairs. 

string.h
    1. String class - Modifications
        - Added a stripStr() method to get rid of punctuation in strings. 

keyvalue.h
    1. Key class
        A key consists of a string and a home node. The key is used for searching, and the home node keeps track of which KV store owns the data.

        a.) field members:
            char* key_;   // key
            size_t nodeId_;  // id of kv store

        b.) methods:
            void setKey(char* kstring)  // set field member key_ with kstring
            void setNode(size_t id)     // set field member nodeId_ with id
            char* getKey()              // get value of key_
            size_t getNode()            // get value of nodeId_
            size_t has()                // hash the key string

    
    2. DFKeyValue class 
        A keyvalue pair consisting of a Key key and a DataFrame value. 

        a.) field members: 
            Key *key;         // the type of the key is the Key class
	        DataFrame* value; // the type of the value is the DataFrame class

        b.) methods: 
            // Gets the key of the pair
	        Key *getKey()

	        // Gets the value of the pair
	        DataFrame *getValue()

            // hashes the key using the hash method from the Key class
            size_t hash()

application.h
    1. KVStore class
        Each KV store has part of the distributed data. The networking and the concurrency control are hidden in this class.

        a.) field members:
            DFMap map_;  // use map to save (key, value) pair, where value is a dataframe
            size_t nodeId_;   // id for node
        
        b.) methods:

            // set field member key_ with k
            void setKey(Key* k)             

            // hash value v to the current KVStore in map_
            void put(Key* k, DataFrame* v) 

            // retrieve value with key k, and return the value
            DataFrame* get(Key& k)     
            
            // retrieve value with key k through socket connection to other node if the 
            //information is not in current KVStore, and return the value
            // currently not implemented 
            DataFrame* WaitAndGet(Key& k)   

            // helper method for debugging
            void printMap()


    2. Application class
        Abstract class for distributing data and user runs

        a.) field members:
            size_t idx_;        // node id
            KVStore kstore_;    // KV store with node id = idx_
        
        b.) methods:

            // return the node id
            int this_node() { return this->idx_; }

            // virtual run method. Users will override the method, like in the Demo class
            virtual void run_() {}

dataframe.h
    1. DataFrame class - Modifications
         A DataFrame is a table composed of columns of equal length. Each column holds values of the same type (I, S, B, F). A dataframe has a schema that describes it.

        a.) field members:
            Schema schema_;             // describes the dataframe
            Column** columns_;          // columns in the dataframe
            const int NUM_THREAD=3;     // number of threads, if processing 
                                        // the dataframe with 
                                        // multiprocessers; currently set 
                                        // to 3

        b.) methods - updated:
            // Puts the given array of data of size size into the specified KVStore store at Key key. 
            // Returns the data as a dataframe.
            DataFrame* fromArray(Key *key, KVStore *store, size_t size, double *data)

            // Puts the given single data value into the specified KVStore store at Key key. 
            // Returns the data as a dataframe. 
            DataFrame* fromScalar(Key* key, KVStore* store, double data);

            // save value read from FileReader fr to kv store with key key. 
            static DataFrame* fromVisitor(Key* in, KVStore* store, char* colType,  FileReader* fr);

            // save value from Summer to store with key key
            static DataFrame* fromVisitor(Key *key, KVStore* store, char* colType, Summer* summer);

            // for each word in the dataframe, count each word
            DataFrame *local_map(Adder *a);

            // add data to the Adder
            void map(Adder &add);

    2. IntColumn
        Holds primitive int values, unwrapped.

        a.) fied members:
            IntArray vals_; // holds integer type data in an IntArray

        b.) methods - updated
            // constructor with a given array
            IntColumn(size_t sz, int* arr) : Column((size_t)sz, 'I') 
            
            // constructor with deserializer
            IntColumn(Deserializer& d) : Column('I'), vals_(d) { }

            // serializer
            void serialize(Serializer& s)


    3. DoubleColumn
        Holds primitive double values, unwrapped.

        a.) field members: 
            DoubleArray vals_; // holds double type data in a DoubleArray

        b.) methods - updated: 
            // constructor with a given array
            DoubleColumn(size_t sz, double* arr) : Column((size_t)sz, 'F') 

            // constructor with deserializer
            DoubleColumn(Deserializer& d) : Column('F'), vals_(d)

            // serializer
            void serialize(Serializer& s)

    4. BoolColumn
        Holds primitive bool values, unwrapped.

        a.) field members: 
            BoolArray vals_; // holds bool type data in a BoolArray

        b.) methods - updated: 
            // constructor with a given array
            BoolColumn(size_t sz, bool* arr) : Column((size_t)sz, 'B') 
            
            // constructor with deserializer
            BoolColumn(Deserializer& d) : Column('B'), vals_(d)

            // serializer
            void serialize(Serializer& s) 

    5. StringColumn
        Holds string pointers. The strings are external.  Nullptr is a valid value.
    
        a.) field members: 
            StringArray vals_; // holds String type data in a StringArray

        b.) methods - updated: 
            // constructor with a given array
            StringColumn(size_t sz, char** arr) : Column((size_t)sz, 'S') 

            // constructor with deserializer
            StringColumn(Deserializer& d) : Column('S'), vals_(d)

            // serializer
            void serialize(Serializer& s)

map.h
    1. Map class - Modifications
        A scalable map class that contains a map of type DFKeyValue . 

        a.) field members: 
            // pointer points to the first pair of <key, value> in the map
	        DFKeyValue **map;
	        size_t size;      	// size of map
	        size_t max;       	// capacity of map. wnen size=max, max will be double in size
	
        b.) methods - updated: 
	        // Gets the value at a specific key: the way to resolve the hashing
	        // conflict is to save the conflicting pair to an empty spot in the map
            // the key type has been changed to Key class
	        DataFrame* get(Key* key)

        	// Sets the value at the specified key to the given value
	        // if map size is full, the map will be re-allocated with double the space
            // the key type has been changed to Key class
	        void set(Key *key, DataFrame *value)

serializer.h
    1. Serializer
        Serializes primitive data types

        a.) field members:
            char* sdata_ = new char[1024];  //data serialized so far
            size_t length_ = 0;             //number of char serialized

        b.) methods:
            // serialize for size_t type data
            void serialize_size_t(size_t& data) 

            // serialize for double type data
            void serialize_double(double& data) 

            // serialize for int type data
            void serialize_int(int& data) 

            // serialize for bool type data
            void serialize_bool(bool& data) 

            // serialize for char* type data
            void serialize_chars(char* data, size_t len)

            // prints the serialized data of type size_t 
            // starting from given index startFrom
            void print_size_t(int startFrom)

            // prints the serialized data of type char
            // starting from given index startFrom
            void print_char(int startFrom)

    2. Deserializer
        Deserializes primitive data types

        a.) field members:
            char* ddata_;       // data that needs to be deserialized
            size_t dlen_ = 0;   // number of already deserialized chars

        b.) methods: 
            // deserializes size_t type data, then returns it
            size_t deserialize_size_t() 

            // deserializes double type data, then returns it
            double deserialize_double() 

            // deserializes int type data, then returns it
            int deserialize_int() 

            // deserializes bool type data, then returns it
            bool deserialize_bool() 

            // deserializes char* type data, then returns it
            char* deserialize_chars(size_t strLen)

            // deserializes char* type data, then passes the deserialized data back to the caller
            void deserialize_chars(char* data, size_t strLen)

            // prints to-be deserialized data of type size_t
            // used for debugging
            void print_size_t(int startFrom, int len)

            // prints to-be deserialized data of type char
            // used for debugging
            void print_char(int startFrom, int len)

message.h
    1. Message
        A class with sender/receiver information. 

        a.) field members: 
            MsgKind kind_;  // the message kind
            size_t sender_; // the index of the sender node
            size_t target_; // the index of the receiver node
            size_t id_;     // an id t unique within the node

        b.) methods:
            // constructor with Deserializer d
            Message(Deserializer& d) 

            // return true if this message object is as the same as the other object
            virtual bool equals(Message* other) 

            // serialize a message
            virtual void serialize(Serializer& s)
    
            // deserialize: return a message
            static Message* deserialize(Deserializer& d);

            // print the content of the message for debbugging
            virtual void printMessage() 

    2. Ack
        acknowledge message class which inherits Message class

        a.) field members:
            none
        
        b.)
            // constructor
            Ack(Deserializer& d) : Message(d) {}

    3. Status
        status message class which inherits Message class

        a.) field members:
            String* msg_;   // describe message status

        b.) methods:
            // constructor with deserializer
            Status(Deserializer &d) : Message(d)

            // Serialize
            void serialize(Serializer &s)

            // return true if this status object is as same as other object
            bool equals(Status* other)

    4. Register
        Register class which inherits Message class

        a.) field members:
            sockaddr_in client_;  // client socket address structure
            size_t port_;           // client port

        b.) methods:
            // constructor: initialize client_ and port_ with given deserializer
            Register(Deserializer& d) : Message(d) 

            // condtructor with given sockaddr_in address and port number
            Register(sockaddr_in& addr, size_t port)

            // serialize 
            void serialize(Serializer& s)

            // return true if this Register object is as the same as the other object
            bool equals(Register* other) 

    5. Directory 
        This class has information for the registered clients. It inherits from the Message class.

        a.) field members:
            size_t client_;   // number of clients ?
            size_t * ports_;  // port numbers
            String ** addresses_;  // ip addresses

        b.) methods
            // constructor with a deserializer
            Directory(Deserializer& d) : Message(d)

            // serialize the class
            void serialize(Serializer &s)

    6. Put
        This class has information for Put type messages. It inherits from the Message class.

        a.) field members:
            DataFrame* data_;
            Key* key_;

        b.) methods:
            // constructor
            Put(size_t from, size_t to, DataFrame* df, Key* k) 

            serialize and deserialize will be implemented in the future.
 
network_ifc.h
    1. NetworkIfc
        An abstract class for network interfacing

        a.) field members: 
            N/A

        b.) methods:
            // register node
            virtual void register_node(size_t idx)

            // return node index
            virtual size_t index()

            // send message msg
            virtual void send_msg(Message* msg)

            // wait for a message to arrive
            virtual Message* recv_msg()

network_pseudo.h
    1. MessageQueue
        An array that uses lock for atomic push/pop

        a.) field members:
            Array mq_;      //message queue
            Lock lock_;     // lock for atomic pop/push

        b.) methods:
            // constructor: empty q
            MessageQueue() : mq_() {}

            // push message msg to q atomically
            void push(Message* msg)

            // pop amessage from q
            Message* pop() 

    2. MessageQueueArray
        An array of message queues

        a.) field members:
            N/A

        b.) methods: 
            // get the ith element in qArray
            MessageQueue *get(size_t i) 

    3. NetworkPseudo
        A communication layer where each node is represented by a thread

        a.) field members: 
            STMap threads_;         // map thread string -> size_t
            Lock threads_lock_;     // lock for threads
            MessageQueueArray qs_;  // array of message queues, one per thread

        b.) methods: 
            //map index to thread ID
            void register_node(size_t index)

            // send message msg <=> push to q
            void send_msg(Message* msg) 

            // receive message and return <=> pop from q
            Message* recv_msg() override 


wordCount.cpp
    1. FileReader
        inherits Rower class:
        read in input file, and load each word as a row.

        a.) field members:
            char * buf_;
            size_t end_ = 0;
            size_t i_ = 0;
            FILE * file_;

        b.) methods: 
            // Reads next word and stores it in the row. Actually read the word.
            // While reading the word, we may have to re-fill the buffer 
            void visit(Row & r)

            // Returns true when there are no more words to read.  There is nothing more to read if we are at the end of the buffer and the file has all been read.
            bool done() 
 
            // Constructor
            // Creates the reader and opens the file for reading.
            FileReader() 
 
            // Reads more data from the file.
            void fillBuffer_() 

            // Skips spaces. Note that this may need to fill the buffer if the last character of the buffer is space itself.
            void skipWhitespace_() 
 
    2. Adder
        Adder inherits from Rower class:
        count each word(row), and save words(keys) and counts(values) to map_

        a.) field members:
            SIMap& map_;  // String to Num map;  Num holds an int

        b.) methods: 
            void visit(Row& r)

    3. Summer
        Summer inherits from Rower class:
        load word count pair to row

        a.) field members:
            SIMap& map_;
            size_t i = 0;     //index of map_
            size_t j = 0;     // index of key array
            size_t seen = 0;  // counter of the seeing key

        b.) methods: 
            // find the next entry in map_
             void next() 

            // return the jth key of map_[i]
            String* k()
 
            // return the jth value of map_[i]
            size_t v() 

            // put words(keys) and counts(values) in map_ to Row r
            void visit(Row& r) 
 
    4. WordCount
        Calculate a word count for given file:
        1) read the data (single node)
        2) produce word counts per homed chunks, in parallel
        3) combine the results

        a.) field members: 
            static const size_t BUFSIZE = 1024;
            Key in;         // hold the key for orginal data from input file. 
            KeyBuff kbuf;   // hold the key for word count result dataframe
            SIMap all;

        b.) methods: 
            // The master nodes reads the input, then all of the nodes count.
            void run_() 

            // Returns a key for given node.  These keys are homed on master node which then joins them one by one. e.g. wc-map-0, wc-map-1, etc.*/
            Key* mk_key(size_t idx)
 
            // Compute word counts on the local node and build a data frame.
            void local_count() 

            // Merge the dataframes of all nodes
            void reduce() 

            // Merge dataframe df with map m
            void merger(DataFrame* df, SIMap& m)


Use cases:
----------
##### Milestone 1
// declare a schema
Schema sch;

// declare a dataframe using the above schema
Dataframe df(Sch);

// parse the command line arguments
args.parser(argc, argv);

// declare a file parser
// the parser is used to parse the sor file
FileParser* pser = new FileParser();

// use the file parser to get each cell from the input file
// get value in arow where column=col_id
char* s = pser->getValueByColumn(col_id, arow);

// once you get the cell value, you add it to a row as follows:
      // set value s to row at column=col_id
      if the column type is BOOLEAN, then row.set(col_id, (bool)atoi(s))
      else if the column type is INT, then row.set(col_id, (int)atoi(s))
      else if the column type is FLOAT, then row.set(col_id, (float)atof(s))
      else if the column type is STRING, then row.set(col_id, new String(s))

// now add the row to the dataframe
    df.add_row(row);

//finally print the dataframe 
df.print_dataframe();

##### Milestone 2
From milestone 2 directory's test.cpp: 
```cpp
// test serialize/deserialze using serializer/deserializer for the Message class
void test_message_serialize_1() 
{
    Serializer s;

    // serialize char
    char* x = new char[7];
    strcpy(x, "lalala");
    s.serialize_chars(x, 6);
    s.print_char(0);

    // serialize size_t
    size_t y = 100;
    s.serialize_size_t(y);
    s.print_size_t(0);

    // serialize char again
    strcpy(x, "haha");
    s.serialize_chars(x, 4);
    s.print_char(14);

}
```

```cpp
// run some serialization/deserialization use cases for the status class
void test_message_serialize_2() 
{
    Serializer s;

    // create a Status object with msg_ = "hahaha"
    Message* m = new Status(new String("hahaha"));

    // set other values in Status
    m->kind_ = MsgKind::Put;
    m->sender_ = 3;
    m->target_ = 4;
    m->id_ = 50;

    // serialize Status m
    m->serialize(s);

    // now, we will deserialize the just serialized data in serializer s
    // user the serializer data to construct the deserlializer
    Deserializer d(s.sdata_, s.length_);

    // get field member values of parent class Message
    size_t x1 = d.deserialize_size_t();
    size_t x2 = d.deserialize_size_t();
    size_t x3 = d.deserialize_size_t();
    size_t x4 = d.deserialize_size_t();

    // get field member values of Status class msg_
    Status* st = dynamic_cast<Status*> (m);
    char* ret = d.deserialize_chars(st->msg_->size_);
    printf("kind=%zd, send=%zd, target=%zd, id=%zd\n", x1,x2,x3,x4);
    d.print_char(40, 6);
    d.print_size_t(0, 46);
}
```

```cpp
// test the serialization/deserialization for the Register class
void test_register_1() 
{
    Serializer s;

    // create a register object 
    sockaddr_in addr;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    size_t port = 8080;
    Message* m = new Register(addr, port);

    // set other values in register message
    m->kind_ = MsgKind::Put;
    m->sender_ = 3;
    m->target_ = 4;
    m->id_ = 50;

    // serialize Status m
    m->serialize(s);

    // now, we deserialize the just serialized data in serializer s
    // 1. use the serializer data to construct the deserlializer
    Deserializer d(s.sdata_, s.length_);

    // 2. construct Status object m2 with d
    Message* m2 = new Register(d);

    // 2. check if m=m2
    if (m->equals(m2)) printf("** Register serialize/deserialize succeed! **\n");
    else printf("** Register serialize/deserialize failed! **\n");

}
```

From test_serialize.cpp:
```cpp
// test string column
void test_string_col() 
{
  if (debug) printf("== string serialize test starts=======\n");
  Schema s;
  DataFrame df(s);

  String* cname;
  char colName[16];

  // create a frame with 5 columns and 3 rows
 for (int i = 0; i < 5; i++)
  {
      sprintf(colName, "col-%d", i);
      cname = new String(colName);
      StringColumn *col = new StringColumn();
      char* tmp1 = new char[32];
      strcpy(tmp1, "1a");

      char* tmp2 = new char[32];
      strcpy(tmp2, "2bb");

      char* tmp3 = new char[32];
      strcpy(tmp3, "3ccc");

      col->push_back(tmp1);      // first row 
      col->push_back(tmp2);     // second row
      col->push_back(tmp3);    // third row
      df.add_column(col, cname);
  }
  if (debug) df.print_dataframe();  

  // get column 1, and serialize it
  StringColumn* col = df.get_string_col(1);
  if (debug) col->printColumn();

   // serialize 
  Serializer ser;
  col->serialize(ser);

  
printf("----length=%d---1\n", ser.length_);
  // use the serializer to create a new Column object
  Deserializer d(ser.sdata_, ser.length_); 
printf("-------2\n");  
  StringColumn* new_col = new StringColumn(d);
printf("-------3\n");
  if (col->equals(new_col)) printf("** StringColumn serialize/deserialize succeed! **\n\n");
  else printf("** StringColumn serialize/deserialize failed! **\n\n");
}
```

##### Milestone 3
From main.cpp
```cpp
NetworkPseudo nt;
std::thread th[args.numNodes_];

// create 3 applications
TestSum t_producer(0, &nt);
TestSum t_counter(1, &nt);
TestSum t_checker(2, &nt);

// 3 threads to run 3 applications - one for each
th[0] = std::thread(&TestSum::run_, &t_producer);
th[1] = std::thread(&TestSum::run_, &t_counter);
th[2] = std::thread(&TestSum::run_, &t_checker);

// join threads
for (int i = 0; i < 3; i++) th[i].join();
```

##### Milestone 4
From the main function in wordCount.cpp
```cpp
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
```

##### Milestone 5
See Milestone 4. 


Open questions: 
---------------
None at the moment. 


Status: 
----------
##### Milestone 1
Merged code from some previous homework assignments. 

The following has been done: 
1. Read input file and load to a dataframe
2. Wrote new classes: Key, KVStore, Application
3. Simplified some old code
4. Installed valgrind. (brew install valgrind)


##### Milestone 2
1. Implemented serialization/deserialization for Message, Status, ACK, Register, and part of Directory. 
2. Implemented serialization/deserialization for all Column types. 
3. Implemented DataFrame::fromArray() and DataFrame::fromScalar() methods to load data to the KVStore. 
4. Updated Map and Array classes to support the above implementation. 
5. Changed String key to Key key in Map. 

TODO: 
    1. Further testing needs to be done for serialization of Directory types. 
    2. Add socket programming in order to implement connections between nodes.
    3. More work needs to be done on memory cleaning. 

#### Milestone 3
1. Implemented network_ifc.h and network_pseudo.h (thanks to Professor Vitek's video lectures). 
2. Added the Put class to message.h 
3. Successfully ran the Demo code snippet from the Milestone 1 assignment, as required. 

TODO:
    1. network_ip.h 
    2. wait_and_get() may need to be improved (?)
    3. As recommended by Professor Ferd in his comments for Milestone 2:
        - Make Key immutable
        - Organize our code into subdirectories (we were having trouble with this)
        - Make KVStore its separate header file
    4. Look further into valgrind if time allows. 

#### Milestone 4
1. Implemented wordCount.cpp but using network_pseudo rather than network_ip due to time limitations
2. Fixed bugs in the provided code
3. Restructured the directory according to the assignment specifications
4. Code runs occasionally on a Windows machine, but consistently crashes on a Mac

TODO:
    1. Get it to work...
    2. Implement network_ip.h
    3. Previously listed, uncompleted TODO tasks

#### Milestone 5
1. Added a stripStr() method to string.h in order to strip punctuation from strings. This allows the visit() method from FileReader to parse only the word content of strings. 
2. Rewrote some of the methods in wordCount.cpp, such as mk_key(). 
3. Fixed a myriad of bugs! For example: 
    - The original provided code deleted dataframes too early in several places, causing the program to crash. 
    - The merger() function used to sum words up incorrectly. 
4. Organized code into subdirectories. 

TODO:
    1. Most importantly, as previously stated, implement network_ip.h
    2. Previously listed, uncompleted TODO tasks


Compile and run
-------------------
1. To compile:
    make build

2. To run:
    make test

3. To utilize valgrind:
    make valgrind
    



