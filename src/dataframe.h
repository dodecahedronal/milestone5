//lang::CwC
#pragma once

#include <stdarg.h>
#include <thread>
#include "object.h"
#include "string.h"
#include "array.h"

class IntColumn;
class BoolColumn;
class DoubleColumn;
class StringColumn;
class KVStore;
class Key;
class FileReader;
class Adder;
class Summer;

/**************************************************************************
 * Column ::
 *
 * Represents one column of a data frame which holds values of a single type.
 * This abstract class defines methods overriden in subclasses. There is
 * one subclass per element type. Columns are mutable. Equality is pointer
 * equality. */
class Column : public Object
{
public:
    char columnType_;

    Column();
    Column(char type) : columnType_(type) {}
    Column(size_t size, char type)
    {
        this->columnType_ = type;
    }
    ~Column(){}

    /** Type converters: Return the same column under its actual type,
     *  or nullptr if of the wrong type.  */
    virtual IntColumn *as_int() { return nullptr; }
    virtual BoolColumn *as_bool() { return nullptr; }
    virtual DoubleColumn *as_double() { return nullptr; }
    virtual StringColumn *as_string() { return nullptr; }

    /** Type appropriate push_back methods. Calling the wrong method is
     * undefined behavior. **/
    virtual void push_back(int val) {}
    virtual void push_back(bool val) {}
    virtual void push_back(double val) {}
    virtual void push_back(String *val) {}

    // Returns the number of elements in the column.
    virtual size_t size() { }

    // return true if two columns are equal
    virtual bool equals() {}

    //Return the type of this column as a char: 'S', 'B', 'I' and 'F'.
    char get_type() { return this->columnType_; }
};

/*************************************************************************
 * IntColumn::
 * Holds primitive int values, unwrapped.
 */
class IntColumn : public Column
{
public:
    IntArray vals_;

    // empty constructor
    IntColumn() : Column((size_t) 0, 'I'), vals_() {
       // printf("IntColumn::IntColumn() starts---\n");
       // printf("    intArray size=%d\n", this->vals_.count());
       // printf("IntColumn::IntColumn() ends---\n");
    }

    // constructor with a given array
    IntColumn(size_t sz, int* arr) : Column((size_t)sz, 'I') 
    {
        for(int i =0; i<sz; i++) {
            //printf("arr[%d] = %d\n", i, arr[i]);
            this->vals_.data_[i] = arr[i];
        }
        this->vals_.size_ = sz;
    }

     // constuctor with (#of elements, element1, element2...)  
    IntColumn(int n, ...) : Column((size_t)n, 'I')     
    {
        va_list values;      // hold arguments
        va_start(values, n); // variable argument processing starts

        for (int i = 0; i < n; i++)
        {
            // append current argement to vals_ array.
            this->vals_.append(va_arg(values, int));
        }

        va_end(values); // variable argument processing ends
        //this->columnSize_++;
    }

    // destructor to delete this->vals_
    ~IntColumn() { 
        //this->vals_.~IntArray(); 
    } 

    // constructor with deserializer
    IntColumn(Deserializer& d) : Column('I'), vals_(d) { }

    // serializer
    void serialize(Serializer& s) {
        this->vals_.serialize(s);
    }

    // get this->vals_[idx]. 
    int get(size_t idx) 
    { 
        //printf("====idx=%d val=%d\n",idx, this->vals_.get(idx));
        return this->vals_.get(idx); 
    }

    // return current column
    IntColumn *as_int() { return this; }

    // insert val to this->vals_[idx]
    void set(size_t idx, int val) { this->vals_.insert(idx, val); }

    // push val to this->vals_[size_]
    void push_back(int val) 
    { 
        this->vals_.append(val); 
        //this->columnSize_++;
    }

    // return number of elements in current column
    size_t size() { return this->vals_.count(); }

    // print column content
    void printColumn()
    {
        // printf("IntColumn::printColumn() starts---\n  ");
        this->vals_.printArr();
        // printf("IntColumn::printColumn() endss---\n");
    }

    // return true if two columns are equal
    bool equals(IntColumn* other)
    {
        if (this->columnType_ != other->columnType_) return false;
        return (this->vals_.equals(&other->vals_));
    }
};

// Other primitive column classes are similar...
/*************************************************************************
 * DoubleColumn::
 * Holds primitive double values, unwrapped.
 */
class DoubleColumn : public Column
{
public:
    DoubleArray vals_;

    // empty constructor
    DoubleColumn() : Column((size_t) 0, 'F'), vals_() {}

    // constructor with a given array
    DoubleColumn(size_t sz, double* arr) : Column((size_t)sz, 'F') 
    {
        for(int i =0; i<sz; i++) {
            //printf("arr[%d] = %f\n", i, arr[i]);
            this->vals_.data_[i] = arr[i];
        }
        this->vals_.size_ = sz;
    }

    // constuctor with (#of elements, element1, element2...)  
    DoubleColumn(int n, ...) : Column((size_t)n, 'F')
    {
        va_list values;             //hold arguments
        va_start(values, n);        // variable argument processing starts
        // append each argement to vals_ array.
        for (int i = 0; i < n; i++)
        {
            //this->vals_.append(va_arg(values, double));
            this->vals_.append(va_arg(values, double));
        }
        va_end(values);             // variable argument processing ends
    }

    // destructor to delete this->vals_
    ~DoubleColumn() { this->vals_.~DoubleArray(); } 

    // constructor with deserializer
    DoubleColumn(Deserializer& d) : Column('F'), vals_(d) { }

    // serializer
    void serialize(Serializer& s) {
        this->vals_.serialize(s);
    }

    // get this->vals_[idx]
    double get(size_t idx) { return this->vals_.get(idx); }

    // return current column
    DoubleColumn* as_double() { return this; }

    // insert val to this->vals_[idx]
    void set(size_t idx, double val) { this->vals_.insert(idx, val); }

    // push val to this->vals_[size_]
    void push_back(double val)
    { 
        this->vals_.append(val); 
        //this->columnSize_++;
    }

    // return number of elements in current column
    size_t size() { return this->vals_.count(); }

    // print column content
    void printColumn()
    {
        //printf("DoubleColumn::printColumn() starts---\n  ");
        this->vals_.printArr();
        //printf("DoubleColumn::printColumn() endss---\n");
    }

    // return true if two columns are equal
    bool equals(DoubleColumn* other)
    {
        if (this->columnType_ != other->columnType_) return false;
        return (this->vals_.equals(&other->vals_));
    }
};

/*************************************************************************
 * BoolColumn::
 * Holds primitive bool values, unwrapped.
 */
class BoolColumn : public Column
{
public:
    BoolArray vals_;

     // empty constructor
    BoolColumn() : Column((size_t) 0, 'B'), vals_() {}

    // constructor with a given array
    BoolColumn(size_t sz, bool* arr) : Column((size_t)sz, 'B') 
    {
        for(int i =0; i<sz; i++) {
            //printf("arr[%d] = %d\n", i, arr[i]);
            this->vals_.data_[i] = arr[i];
        }
        this->vals_.size_ = sz;
    }

    // constuctor with (#of elements, element1, element2...)
    BoolColumn(int n, ...) : Column((size_t) 0, 'B')
    {
        va_list values;             //hold arguments
        va_start(values, n);        // variable argument processing starts
        // append each argement to vals_ array.
        for (int i = 0; i < n; i++) { this->vals_.append(va_arg(values, int)); }
        va_end(values); // variable argument processing ends
    }

    // destructor to delete this->vals_
    ~BoolColumn() { this->vals_.~BoolArray(); } 

    // constructor with deserializer
    BoolColumn(Deserializer& d) : Column('B'), vals_(d) { }

    // serializer
    void serialize(Serializer& s) {
        this->vals_.serialize(s);
    }

    // get this->vals_[idx]
    bool get(size_t idx) { return this->vals_.get(idx); }

    // return current column
    BoolColumn *as_bool() { return this; }

    // insert val to this->vals_[idx]
    void set(size_t idx, bool val) { this->vals_.insert(idx, val); }

    // push val to this->vals_[size_]
    void push_back(bool val) 
    { 
        this->vals_.append(val); 
        //this->columnSize_++;
    }

    // return number of elements in current column
    size_t size() { return this->vals_.count(); }

    // print column content
    void printColumn()
    {
        //printf("BoolColumn::printColumn() starts---\n  ");
        this->vals_.printArr();
        //printf("BoolColumn::printColumn() endss---\n");
    }

    // return true if two columns are equal
    bool equals(BoolColumn* other)
    {
        printf("===== type=%c   o=%c\n", this->columnType_, other->columnType_);
        if (this->columnType_ != other->columnType_) return false;
        return (this->vals_.equals(&other->vals_));
    }
};

/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
class StringColumn : public Column
{
public:
    StringArray vals_;

    // empty constructor
    StringColumn() : Column((size_t) 0, 'S'), vals_() {}

    // constructor with a given array
    StringColumn(size_t sz, char** arr) : Column((size_t)sz, 'S') 
    {
        //printf("StringColumn(): sz=%d\n", sz);
        for(int i =0; i<sz; i++) {
            //printf("arr[%d]=%s \n", i, arr[i]);
            this->vals_.append(new String(arr[i]));
        }
        this->vals_.size_ = sz;

        // take care of memory recycling
        for (int i=0; i<sz; i++)
            if (arr[i]) delete [] arr[i];
        if (arr) delete [] arr;
    }

   
    // constuctor with (#of elements, element1, element2...)
    StringColumn(int n, ...) : Column((size_t) 0, 'S')
    {
        va_list values;      //hold arguments
        va_start(values, n); // variable argument processing starts
        // append current argement to vals_ array.
        for (int i = 0; i < n; i++) 
        { 
            char* astring = va_arg(values, char*);      // the arguments is char* type
            String* astringObj = new String(astring);   // new String type for appending to this->vals_
            this->vals_.append(astringObj);
        }
        va_end(values); // variable argument processing ends
    }

    // destructor
    ~StringColumn() { this->vals_.~StringArray(); }

    // constructor with deserializer
    StringColumn(Deserializer& d) : Column('S'), vals_(d) { }

    // serializer
    void serialize(Serializer& s) {
        this->vals_.serialize(s);
    }

    // return this string culumn pointer
    StringColumn *as_string() { return this; }

    /** Returns the string at idx; undefined on invalid idx.*/
    String *get(size_t idx) { return this->vals_.get(idx); }

    /** Acquire ownership fo the string. */
    void set(size_t idx, String *val) { this->vals_.insert(idx, val); }

    // append one more element val to the column
    void push_back(String *val)
    {
        this->vals_.append(val);
        //this->columnSize_++;
    }

    void push_back(char *val)
    {
        String* strVal = new String(val);
        this->vals_.append(strVal);
        //this->columnSize_++;
    }

    // return size of array
    size_t size() { return this->vals_.count(); }

    // print column content
    void printColumn()
    {
        //printf("StringColumn::printColumn() starts---\n  ");
        this->vals_.printArr();
        //printf("StringColumn::printColumn() endss---\n");
    }

    // return true if two columns are equal
    bool equals(StringColumn* other)
    {
        if (this->columnType_ != other->columnType_) return false;
        return (this->vals_.equals(&other->vals_));
    }

};

/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame. the schema
 * knows the number of columns and number of rows, the type of each column;
 * optionally, columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */
class Schema : public Object
{
public:
    size_t  ncolumns_;      // number of columns
    size_t  nrows_;         // number of rows
    char*   colType_;       // column type
    StringArray* colName_;  // column name
    StringArray* rowName_;

    /** Copying constructor */
    Schema(Schema &from)
    { 
        if (from.width() != 0)
        {
            this->colType_ = new char[from.width()+1];
            strcpy(this->colType_, from.colType_);
            this->ncolumns_ = from.ncolumns_;
            this->nrows_ = from.nrows_;
      
            // initialize colName_[] and rowName_[]
            this->colName_ = new StringArray();
            this->rowName_ = new StringArray();
            this->colName_->extend(from.colName_);
            this->rowName_->extend(from.rowName_);
        }
    }

    /** Create an empty schema **/
    Schema() 
    { 
        this->colType_ = nullptr;
        this->ncolumns_ = 0;
        this->nrows_ = 0;
        this->colName_ = new StringArray();
        this->rowName_ = new StringArray();
    }

    /** Create a schema from a string of types. A string that contains
    * characters other than those identifying the four type results in
    * undefined behavior. The argument is external, a nullptr is
    * undefined. **/
    Schema(const char *types)
    {
        // if types is empty, do nothing
        if (types == nullptr) return;
    //printf("--%s\n", types);
        this->colName_ = new StringArray();
        this->rowName_ = new StringArray();

        // if one of the column type is not IFBS, then do nothing
        for (int i=0; i<strlen(types); i++)
        {
            if ((types[i]!='I') && 
                (types[i]!='F') && 
                (types[i]!='B') &&
                (types[i]!='S') ) 
            return;
            String* cname = new String("\0");
            this->colName_->append(cname);
        }

        //otherwise, create schema(column types, #of columns, #of rows, column names, row names)
        int len = strlen(types);
        this->colType_ = new char[len+1];
        strcpy(this->colType_, types);
        this->ncolumns_ = len;
        this->nrows_ = 0;

        //printf("--type=%s nc=%d nr=%d\n", this->colType_, this->ncolumns_, this->nrows_);
    }

    /** Add a column of the given type and name (can be nullptr), name
    * is external. */
    void add_column(char typ, String *name)
    {
        // copy current column type to tmp
        char* tmp = new char[this->ncolumns_+2]; // index starts from 0=> size+1, hold '\0'=>+1
        for (int i=0; i<this->ncolumns_; i++)
            tmp[i] = this->colType_[i];

        // add to tmp[] the type of the new column to be added
        tmp[this->ncolumns_] = typ;           

        // terminate colType_ string
        tmp[this->ncolumns_+1] = '\0';
        
        // delete old colType_, start using new colType with newly added column
        delete [] this->colType_;               // delete current colType_
        this->colType_ = tmp;                   // use the new allocated colType
        this->ncolumns_++;                      // increase number of columns by one
         if (name) this->colName_->append(name); // set column name

       //printf("Schema::add_columns():ading=%c, type string=%s\n", typ, this->colType_);
    }

    /** Add a row with a name (possibly nullptr), name is external. */
    void add_row(String *name)
    {
        if (name) this->rowName_->append(name);
        this->nrows_++;
    }

    /** Return name of row at idx; nullptr indicates no name. An idx >= width
     *  is undefined. */
    String *row_name(size_t idx) { return this->rowName_->get(idx); }

    /** Return name of column at idx; nullptr indicates no name given.
     *  An idx >= width is undefined.*/
    String *col_name(size_t idx) { return this->colName_->get(idx);}

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) 
    { 
        //printf("===type(%d)=%c\n", idx, this->colType_[idx]);
        return this->colType_[idx];
    }

    /** Given a column name return its index, or -1. */
    int col_idx(const char *name)
    {
        String* ithColName;
        // go through this->colName_ array to fina a match
        for (int i=0; i<this->ncolumns_; i++)
        {
            ithColName = this->col_name(i);
            if ( !strcmp(ithColName->cstr_, name) ) return i;
        }
        // if nothing has been found, then return -1
        return -1; 
    }

    /** Given a row name return its index, or -1. */
    int row_idx(const char *name)
    {
        String* ithRowName;
        for (int i=0; i<this->nrows_; i++)
        {
            ithRowName = this->row_name(i);
            if ( !strcmp(ithRowName->cstr_, name) ) return i;
        }
        return -1;
    }

    /** The number of columns */
    size_t width() { return this->ncolumns_;}

    /** The number of rows */
    size_t length() { return this->nrows_; }

    void print_schema()
    {
        printf("Schema::print_schema() starts----\n");
        printf("    nrow=%d ncol=%d\n", (int)this->length(), (int)this->width() );
    
        // print row names if there is any
        for (int i=0; i<this->length(); i++)
        {
            if (this->row_name(i))
                printf("    row-name[%d]=%s ", i, this->row_name(i)->c_str());
        }
        pln();
        for (int i=0; i<this->width(); i++)
        {
            if (this->col_name(i))
                printf("    col[%d]=%s type[%d]=%c ", i, this->col_name(i)->c_str(), i, this->col_type(i));
        }
        pln();
        printf("Schema::print_schema() ends----\n");
    }
};

/*****************************************************************************
 * Fielder::
 * A field vistor invoked by Row.
 */
class Fielder : public Object
{
public:
    /** Called before visiting a row, the argument is the row offset in the
    dataframe. */
    virtual void start(size_t r);

    /** Called for fields of the argument's type with the value of the field. */
    virtual void accept(bool b);
    virtual void accept(double f);
    virtual void accept(int i);
    virtual void accept(String *s);

    /** Called when all fields have been seen. */
    virtual void done();
};

/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally, a dataframe holds data in columns.
 * Rows have pointer equality.
 */
class Row : public Object
{
public:
    Schema& schema_;
    void** data_;
    size_t index_;

    /** Build a row following a schema. */
    Row(Schema &scm) : schema_(scm) {
        data_ = new void*[this->schema_.width()];
        for (int i=0; i<schema_.width(); i++)
        {
            if (schema_.col_type(i) == 'I')  data_[i] = new int;
            else if (schema_.col_type(i) == 'B') data_[i] = new bool;
            else if (schema_.col_type(i )== 'F') data_[i] = new double;
            else if (schema_.col_type(i) == 'S') data_[i] = nullptr;
        }
        this->index_ = 0; 
        //printf("===row constructor:nr=%d  nc=%d\n", this->schema_.length(), schema_.width());
    }

    /** Setters: set the given column with the given value. */
    void set(size_t col, int val)   { *( (int *)this->data_[col] ) = val; }
    void set(size_t col, double val) { *( (double *)this->data_[col] ) = val; }
    void set(size_t col, bool val)  { *( (bool *)this->data_[col] ) = val; }
    /** Acquire ownership of the string. */
    void set(size_t col, String *val) { this->data_[col] = val; }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx)    { this->index_ = idx; }
    size_t get_idx()          { return this->index_; }

    /** Getters: get the value at the given column. If the column is not
     *  of the requested type, the result is undefined. */
    int get_int(size_t col) { return *((int *)this->data_[col]); }
    bool get_bool(size_t col)  { return *((bool *)this->data_[col]); }
    double get_double(size_t col)  { return *((double *)this->data_[col]); }
    String *get_string(size_t col) { return (String *)this->data_[col]; }

    /** Number of fields in the row. */
    size_t width() { return this->schema_.width(); }

    /** Type of the field at the given position. An idx >= width is
     *  undefined. */
    char col_type(size_t idx) { return this->schema_.col_type(idx); }

    /** Given a Fielder, visit every field of this row.
     * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder &f) {
        char ctype;
        f.start(idx);
        for (int i=0; i<this->schema_.width(); i++)
        {
            ctype = this->schema_.col_type(i);
            if (ctype == 'I') f.accept(get_int(idx));
            if (ctype == 'B') f.accept(get_bool(idx));
            if (ctype == 'F') f.accept(get_double(idx));
            if (ctype == 'S') f.accept(get_string(idx));
        }
        f.done();
    }

    void print_row()
    {
        char ctype;
        int nr = this->schema_.length(); //number of rows
        int nc = this->schema_.width();  // number of columns

        printf("Row::print_row() starts---\n");
        this->schema_.print_schema();
        for (int i=0; i<nr; i++)
        {
            for (int j=0; j<nc; j++)
            {
                ctype = this->schema_.col_type(i);
                if (this->data_[i])
                {
                    if (ctype == 'I')
                        printf("    (%d,%d)=%d  ", (int)this->index_, i, this->get_int(i));
                    if (ctype == 'B')
                        printf("    (%d,%d)=%d  ", (int)this->index_, i, this->get_bool(i));
                    if (ctype == 'F')
                        printf("    (%d,%d)=%f  ", (int)this->index_, i, this->get_double(i));
                    if (ctype == 'S')
                        printf("    (%d,%d)=%s  ", (int)this->index_, i, this->get_string(i)->c_str());
                }
            }
            pln();
        }
        printf("Row::print_row() ends---\n");
    }
};

/*******************************************************************************
 *  Rower::
 *  An interface for iterating through each row of a data frame. The intent
 *  is that this class should subclassed and the accept() method be given
 *  a meaningful implementation. Rowers can be cloned for parallel execution.
 */
class Rower : public Object
{
public:
    /** This method is called once per row. The row object is on loan and
      should not be retained as it is likely going to be reused in the next
      call. The return value is used in filters to indicate that a row
      should be kept. */
    Rower(){}
    ~Rower(){}
    virtual bool accept(Row &r) { return true; }

    /** Once traversal of the dataframe is complete, the rowers that were
      split off will be joined. There will be one join per split. The
      original object will be the last to be called join on. The join method
      is reponsible for cleaning up memory. */
    virtual void join_delete(Rower *other) {}
};

/*******************************************************************************
 *  Writer::
 */
class Writer : public Object
{
public:
    Writer(){}
    ~Writer(){}
    virtual void visit(Row &r) {}
    virtual bool done() {return true;}
};
/*******************************************************************************
 *  Reader::
 */
class Reader : public Object
{
public:
    Reader(){}
    ~Reader(){}
    //virtual bool visit(Row &r) { return true;}
    virtual void visit(Row &r) { return;}
};
/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is a table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
class DataFrame : public Object
{
public:
    Schema schema_;
    Column** columns_;
    const int NUM_THREAD=3;
    
    /** Create a dataframe with the same columns as the given df but no rows */
    DataFrame(DataFrame* df) 
    { 
        // use df's schema to set up 
        DataFrame(df->get_schema());
    }

    /** Create a dataframe from a schema and columns. Results are undefined if
     * the columns do not match the schema. */
    DataFrame(Schema &schema) : columns_(nullptr)
    {
        // initialize dataframe schema(ncolumns_, colType_, colName_, rowName, nrows_)
        int ncol = schema.width();

        if (ncol <= 0)
        {
            this->schema_.ncolumns_ = ncol;              // set ncolumns_
            this->schema_.colType_ = nullptr;
            this->schema_.nrows_ = 0;
        }
        else
        {
            this->schema_.ncolumns_ = ncol;              // set ncolumns_
            this->schema_.colType_ = new char[ncol + 1]; // allocate space for colType_
            int i = 0;
            while (i < ncol) // for each column,
            {
                this->schema_.colType_[i] = schema.col_type(i);     // set colType_
                this->schema_.colName_->append(schema.col_name(i)); // set colName_
                this->schema_.rowName_->append(schema.row_name(i)); // set rowNmae_
                i++;
            }

            this->schema_.colType_[i] = '\0';       // remember to terminate colType
            this->schema_.nrows_ = schema.length(); // set nrows

            columns_ = new Column *[ncol];

            for (int i = 0; i < ncol; i++)
            {
                char t = schema_.col_type(i);
                switch (t)
                {
                case 'I':
                    this->columns_[i] = new IntColumn(this->schema_.length());
                    //printf("new col------------len=%d coltype=%c\n", this->schema_.length(), this->columns_[i]->get_type());
                    break;
                case 'F':
                    this->columns_[i] = new DoubleColumn(this->schema_.length());
                    break;
                case 'B':
                    this->columns_[i] = new BoolColumn(this->schema_.length());
                    break;
                case 'S':
                    this->columns_[i] = new StringColumn(this->schema_.length());
                    break;
                }
            }
        }
    }

    /** Returns the dataframe's schema. Modifying the schema after a dataframe
     * has been created is undefined behavior. */
    Schema &get_schema() { return this->schema_; }

    /** Adds a column to this dataframe and updates the schema. the new column
     * is external, and appears as the last column of the dataframe. The
     * name is optional and external. A nullptr column is undefined. */
    void add_column(Column *col, String *name)
    {
        char newType = col->get_type();      // get the to-be-added column's type
        size_t ncol = this->schema_.width(); // will add new col to (ncol+1)-th column

        //printf("df::add_column()starts----\n");
        //this->get_schema().print_schema();

        if (ncol > 0)
        {
            // update columns: create a ncol length Column, copy, then delete old one.
            Column **newCols = new Column *[ncol+1];
            for (int i = 0; i < ncol; i++)
            {
                newCols[i] = this->columns_[i]; // copy existing columns to new
            }
 
            if (this->columns_) {
                delete [] this->columns_; // terminate current columns pointer
            }

            this->columns_ = newCols;    // start to use new one

            this->columns_[ncol] = col; // add col to data frame

            // update schema
            this->schema_.ncolumns_++;              // increase column count
            this->schema_.colName_->append(name);   // set the new column name
            //this->schema_.colType_[ncol] = newType; // set column type for the new column
            char* newColType = new char[ncol+2];
            strcpy(newColType, this->schema_.colType_);
            newColType[ncol] = newType;
            newColType[ncol+1] = '\0';
            delete [] this->schema_.colType_;
            this->schema_.colType_ = newColType;
        }
        else 
        {   // update column
            this->columns_ = new Column*[1];
            this->columns_[ncol] = col; // add col to data frame

            // update schema
            this->schema_.nrows_ = col->size();
            this->schema_.ncolumns_ = 1;
            
            this->schema_.colType_ = new char[1 + 1];
            this->schema_.colType_[0] = newType;    // set column type for the new column
            this->schema_.colType_[1] = '\0';       // remember to terminate colType
            this->schema_.colName_->append(name);   // set the new column name  
        }
 
        //this->get_schema().print_schema();
        // printf("df::add_column()ends----\n\n");
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, or requesting the wrong type is undefined.*/
    int get_int(size_t col, size_t row) 
    { 
        // make sure return from integer array
        IntColumn* column = this->columns_[col]->as_int();
        return column->get(row);
    }

    //return the bool value at (row, col)
    bool get_bool(size_t col, size_t row)
    {
        // make sure to return from a bool array
        BoolColumn* column = this->columns_[col]->as_bool();
        return column->get(row);
    }

    //return the double value at(row, col)
    double get_double(size_t col, size_t row)
    {
        // make sure to return from a double array
        DoubleColumn* column = this->columns_[col]->as_double();
        return column->get(row);
    }

    //return the string value ar (row, col)
    String *get_string(size_t col, size_t row)
    {
        // make sure to return from a string array
        StringColumn* column = this->columns_[col]->as_string();
        return column->get(row);
    }

    /** Return the offset of the given column name or -1 if no such col exists. */
    int get_col_index(String &col)
    {
        // get number of columns in current df
        size_t width = this->schema_.width(); 

        // for each column name, get its name from schema column name array and perform a comparison
        String* curColumnName;
        for (int i=0; i<width; i++)
        {
            curColumnName = this->schema_.col_name(i);
            if (curColumnName->equals(&col)) return i; 
        }
        return -1; //if nothing is found.
    }

    // return column array for column number = col
    IntColumn* get_int_col(int col) 
    {
        return this->columns_[col]->as_int();
    }

    // return column array for column number = col
    BoolColumn* get_bool_col(int col) 
    {
        return this->columns_[col]->as_bool();
    }

    // return column array for column number = col
    DoubleColumn* get_double_col(int col) 
    {
        return this->columns_[col]->as_double();
    }

    // return column array for column number = col
    StringColumn* get_string_col(int col) 
    {
        return this->columns_[col]->as_string();
    }

    /** Return the offset of the given row name or -1 if no such row. */
    int get_row(String &row)
    {
        // get number of rows in current df
        size_t len = this->schema_.length(); 

        // for each row name, get its name from schema rowName array and perform a comparison
        String* curRowName;
        for (int i=0; i<len; i++)
        {
            curRowName = this->schema_.row_name(i);
            if (curRowName->equals(&row)) return i; 
        }
        return -1; //if nothing is found.
    }

    /** Set the value at the given column and row to the given value.
     * If the column is not of the right type or the indices are out of
     * bounds, the result is undefined. */
    void set_int(size_t col, size_t row, int val)
    {
        // set value to int colum
        IntColumn *column = this->columns_[col]->as_int();
        column->set(row, val);
    }

    // set bool value at (row, col)
    void set_bool(size_t col, size_t row, bool val)
    {
        // set value to bool column
        BoolColumn *column = this->columns_[col]->as_bool();
        column->set(row, val);
    }

    //set double value at (row, col)
    void set_double(size_t col, size_t row, double val)
    {
        // set value to double column
        DoubleColumn *column = this->columns_[col]->as_double();
        column->set(row, val);
    }

    //set string value at (row, col)
    void set_string(size_t col, size_t row, String val)
    {
        // set value to string column
        StringColumn *column = this->columns_[col]->as_string();
        column->set(row, &val);
    }

    /** Set the fields of the given row object with values from the columns at
     * the given offset. If the row is not from the same schema as the
     * dataframe, results are undefined.
     */
    void fill_row(size_t idx, Row &row)
    {
        size_t rnumber = row.get_idx();
        int numberCol = row.width();
        for (int col=0; col<numberCol; col++)
        {
            char ctype = this->schema_.col_type(col);
            switch (ctype)
            {
            case 'I':
            {
                //IntColumn *column = this->columns_[idx]->as_int();
                //row.set(col, column->get(rnumber));
                row.set(col, get_int(col, idx));
                break;
            }
            case 'B':
            {
                //BoolColumn *column = this->columns_[idx]->as_bool();
                //row.set(col, column->get(rnumber));
                row.set(col, get_bool(col, idx));
                break;
            }
            case 'F':
            {
                //doubleColumn *column = this->columns_[idx]->as_double();
                //row.set(col, column->get(rnumber));
                row.set(col, get_double(col, idx));
                break;
            }
            case 'S':
            {
                //StringColumn *column = this->columns_[idx]->as_string();
                //row.set(col, column->get(rnumber));
                row.set(col, get_string(col, idx));
                break;
            }
            default:
                printf("Error(DataFrame::fill_row()): unknow column type %c\n", ctype);
                exit(1);
            }
        }
    }

    /** Add a row at the end of this dataframe. The row is expected to have
     *  the right schema and be filled with values, otherwise it's undefined behavior. */
    void add_row(Row &row)
    {
        // get number of columns in this data frame
        int numberCol = this->schema_.width();
        int idx = row.get_idx();

        // loop through each column, add each element in row to its corresponding column
        for (int i=0; i<numberCol; i++)
        {
            char ctype = this->schema_.col_type(i);     // get i-th column type
            //printf("*********df::add_row(): rowid=%d, column=%d ctype=%c---\n", idx, i, ctype);
            switch (ctype)
            {
            case 'I':
            {   IntColumn *column = this->columns_[i]->as_int();
                //printf("push val=%d\n", row.get_int(i));
                column->push_back(row.get_int(i));
                break;
            }
            case 'B':
            {    BoolColumn *column = this->columns_[i]->as_bool();
                column->push_back(row.get_bool(i));            
                break;
            }
            case 'F':
            {    DoubleColumn *column = this->columns_[i]->as_double();
                column->push_back(row.get_double(i));        
                break;
            }
            case 'S':
            {   StringColumn *column = this->columns_[i]->as_string();
                column->push_back(row.get_string(i));
                break;
            }
            default:
                printf("Error(DataFrame::add_row()): unknow column type %c\n", ctype);
                exit(1);
            }
        }
        this->get_schema().nrows_++;
    }

    /** The number of rows in the dataframe. */
    size_t nrows() { return this->schema_.length(); }

    /** The number of columns in the dataframe.*/
    size_t ncols() { return this->schema_.width(); }

    /** Visit rows in order */
    void map(Rower &r) {
        Row row(get_schema());
        for (int i=0;i<nrows(); i++) {
            fill_row(i, row);
            r.accept(row);
        }
    }

    // callback function for pmap()
    void pmap_thread_callback(int thread_id, Rower*r)
    {
        //printf(" t_id=%d\n", thread_id);
        Row row(get_schema());

        // for each row, fill data to row, and pass the data to accept() for processing
        for (int i = 0; i < this->nrows(); i++)
        {
            if (i % NUM_THREAD == thread_id)    // split jobs among threads
            {
                this->fill_row(i, row);     // dataframe fills the row with its data
                r->accept(row);             // rower calls its method accept() with the row data
            }
        }
    }

    // multi-thread processing on each row with given rower
    void pmap(Rower &rower)
    {
        // create NUM_THREAD threads
        std::thread t[NUM_THREAD];

        // declare NUM_THREAD rowers for each thread t[] to use
        Rower* ra[NUM_THREAD];

        // Now, create the rowers and pass them to thread callback pmap_thread_call(nt, rower)
        for (int nt = 0; nt < NUM_THREAD; nt++)
        {
           //ra[nt] = new Rower(rower);
           // clone the passed in rower, and assign one for each thread
           ra[nt] = (Rower*) rower.clone();
           t[nt] = std::thread(&DataFrame::pmap_thread_callback, this, nt, ra[nt]);
        }

        // wait for all threads to finish, then combine results from each thread
        for (int nt = 0; nt < NUM_THREAD; nt++){
             t[nt].join();
             rower.join_delete(ra[nt]);
        }
    }

    /** Create a new dataframe, constructed from rows for which the given Rower
     * returned true from its accept method. */
    DataFrame *filter(Rower &r)
    {
        DataFrame* nf = new DataFrame(this->get_schema());
        Row row(this->get_schema());
        for (int i=0; i<this->nrows(); i++)
        {
            for (int j=0; j<this->ncols(); j++)
                this->fill_row(j, row);

            if (r.accept(row))
                nf->add_row(row);
        }

        return nf;
    }

    void print_dataframe()
    {
        int nr = this->nrows();  // number of rows
        int nc = this->ncols();  // number of columns

        //printf("DataFrame::print_dataframe() starts----\n");
        //this->get_schema().print_schema();
        for (int i=0; i<nr; i++)
        {   //printf("row=%d---nr=%d nc=%d\n", i, nr, nc);
            for (int j=0; j<nc; j++)
            {
                //char ctype = this->columns_[j]->get_type();
                char ctype = this->schema_.col_type(j);
                if (ctype == 'I')
                {   //printf("j=%d\n", j);
                    IntColumn *c = this->columns_[j]->as_int();
                    printf("    (%d,%d)=%d  ", i, j, c->get(i));
                } 
                else if (ctype == 'B')
                {
                    BoolColumn *c = this->columns_[j]->as_bool();
                    printf("    (%d,%d)=%d  ", i, j, c->get(i));
                } 
                else if (ctype == 'F')
                {
                    DoubleColumn *c = this->columns_[j]->as_double();
                    printf("    (%d,%d)=%.2f  ", i, j, c->get(i));
                } 
                else if (ctype == 'S')
                {
                    StringColumn *c = this->columns_[j]->as_string();
                    printf("    (%d,%d)=%s  ", i, j, c->get(i)->c_str());
                }
            }
            pln();
        }
        //printf("DataFrame::print_dataframe() ends----\n");
    }

    /**
     * fromArray and fromScalar are declared here but defined in application.h
     * in order to avoid interdependencies of the include statements.
     */
    static DataFrame* fromArray(Key* key, KVStore* store, size_t size, double* data);
    static DataFrame* fromScalar(Key* key, KVStore* store, double data);
    static DataFrame* fromVisitor(Key* in, KVStore* store, char* colType,  FileReader* fr);
    static DataFrame* fromVisitor(Key *key, KVStore* store, char* colType, Summer* summer);
    DataFrame *local_map(Adder *a);
    void map(Adder &add);
};


