//lang::CwC
#include <string.h>
#include <stdio.h>
#include "object.h"
#include "dataframe.h"

enum COL_TYPE {BOOLEAN, INT, FLOAT, STRING};
int sorer_debug = 0;

//class Sorer : public Object
class Sorer : public Object {
public:
    FILE *fp_;              // file pointer for the input file
    int numberOfColumns_;   // number of columns in the input file
    int numberOfRows_;      // number of rows in the input file
    Schema sch_;            // schema for dataframe
    COL_TYPE *colType_;     // used for keeping each column type
    //DataFrame df;

    // constructor: open file to read
    Sorer(char* fname) { 
        fp_ = fopen(fname, "r"); 
        numberOfColumns_ = 0;
    }

    // destructor: close file
    ~Sorer() { fclose(fp_); }
    
    /////////////////////////////////////////////////////
    // calc the length of s
    int getStringLen(char *s)
    {
        int i = 0;
        while (s[i] != '\0') i++;
        if (sorer_debug)  printf("\t getStringLen(%s) = %d\n", s, i);
        return i;
    }
    //////////////////////////////////////////////////
    // get the number of <> in string s (number of columns)
    int getColNumber(char *s)
    {
        int i = 0;
        int ret = 0;
        int foundL = 0; // if "<" is found
        int foundR = 0; // if ">" is found

        if (sorer_debug) printf("\t getColNumber(): s=%s", s);
        while (s[i] != '\0')
        {
            if ((foundL > 1) || (foundR > 1))
                return -99999; // wrong format: multi < or >. i.e <<<<,then exit
            if (s[i] == '<')
                foundL++; // mark once for <
            if (s[i] == '>')
                foundR++;                   // mark once for >
            if (foundL == 1 && foundR == 1) // if both < and > are marked once
            {
                ret++;      // found one column
                foundL = 0; // reset foundL and foundR for next column
                foundR = 0;
            }
            i++;
        }
        if (sorer_debug)  printf("\t getColNumer(): ret= %d\n", ret);
        return ret;
    }

    ////////////////////////////////////////////
    // decide number of column for schema
    //use only the first 500 rows to find number of columns from the longest row
    int calcNumberOfColumns() {
        char arow[2048];
        int i = 0;
        int ncol = 0; // counter of column numbers
        int max = 0;  // max column #

        // read in file row by row, and for each row counts its columns, keep the max
        while (((fgets(arow, sizeof(arow), fp_)) != NULL) && (i < 500))
        {
            //printf("at row %d: %s|", i, arow);
            ncol = this->getColNumber(arow);
            if (ncol < 0)  exit(1); // error in input data format
            if (ncol > max)  max = ncol;
            i++;
        }
        this->numberOfColumns_ = max;
        return this->numberOfColumns_;
    }

    //////////////////////////////////////////
    // find data type for each column from the input file
    COL_TYPE* getColTypes() {
        COL_TYPE type;
        char arow[2048];
        this->colType_ = new COL_TYPE[numberOfColumns_]; // used for keeping each column type

        // initialize to BOOLEAN for each column
        for (int i = 0; i < numberOfColumns_; i++) colType_[i] = BOOLEAN;

        // loop through each row, get the column type for each value
        // then decide a type for each column in the end
        int nrows = 0;                                  // counting number of rows
        fseek(fp_, 0, SEEK_SET);                         // reset fp to file beginning
        while ((fgets(arow, sizeof(arow), fp_)) != NULL) // for each row in file
        {
            for (int i=0; i < numberOfColumns_; i++) // for each column in the row
            {
                char *s = this->getValueByColumn(i, arow); // get the column value
                if (s && (s[0] != '\0'))                   // if value is not empty
                {
                    type = this->getStringType(s); // get type for s
                    if (sorer_debug) printf("value=%s type=%d\n", s, type);
                }
                // among the same column, if one value is string, the whole column type is string
                // if there is no string, but there is one double, then it has to be double.
                // if no string, no fload, but there is one int, then it has to be int.
                // according to the COL_TYPE in enumeration set, we have the order:
                // boolean < int < double < string, hence the following,
                if (colType_[i] < type) colType_[i] = type;
            }
            // counting number of rows in the input file
            nrows++;
        }
        this->numberOfRows_ = nrows;
        return colType_;
    }

    ////////////////////////////////////////////////////
    // get the col-th column value in row
    char *getValueByColumn(int col, char *row)
    {
        int i = 0;
        int ncol = 0;
        // first to find the left bracket < for col-th column
        // count the number of <, stop at col-th
        while ((ncol <= col) && (row[i] != '\0'))
        {
            if (row[i] == '<') ncol++;
            i++;
        }

        if (row[i] == '\"') i++; // get ride of " mark
        // second, to find the right bracket >
        // at the same time, record each char from < to >
        char *ret = new char[1024]; // to keep the string value from < to >
        int j = 0;                         // index for ret[]
        while ((row[i] != '>') && (row[i] != '\0'))
        {
            ret[j] = row[i]; // save each char in row from < to > to ret[]
            i++;
            j++;
        }
        if (ret[j - 1] == '\"') ret[j - 1] = '\0';
        else  ret[j] = '\0';

        if (sorer_debug) printf("       ret = %s, j=%d\n", ret, j);
        return ret;
    }

    //////////////////////////////////////////////////
    // get type of s. i.e. (BOOLEAN, INT, FLOAT or STRING)
    COL_TYPE getStringType(char *s)
    {
        COL_TYPE t = BOOLEAN;
        int floatFlag = 0; // use to remember that "." has occurred
        int i = 0;

        // if s has value 0 or 1 with length 1, then it's a BOOLEAN
        if ((strlen(s) == 1) && ((s[0] == '0') || (s[0] == '1')))
            return BOOLEAN;
        if ((s[0] == '+') || (s[0] == '-'))
            i = 1; // skip + and - if they are the first char
        int length = getStringLen(s);
        while (i < length)
        {
            if ((isalpha(s[i]) || isspace(s[i])) && (s[i] != '.')) //if a letter occures in any place, it's a string
                return STRING;

            if (s[i] == '.')
                floatFlag = 1; //if "." occures, remember it. Wait to see more
            else if (!floatFlag)
                t = INT; // if no "." has ever occured, it's INT
            i++;
        }
        if (floatFlag)
            t = FLOAT; // now we see every bit, it has to be a FLOAT
        return t;
    }

    ////////////////////////////////////
    // build schema
    void buildSchema() {
        for (int i = 0; i < numberOfColumns_; i++)
        {
            if (colType_[i] == BOOLEAN)
                this->sch_.add_column('B');
            else if (colType_[i] == INT)
                this->sch_.add_column('I');
            else if (colType_[i] == FLOAT)
                this->sch_.add_column('F');
            else if (colType_[i] == STRING)
                this->sch_.add_column('S');
        }
    }

    Schema* getSchema() { return &this->sch_; }

    ////////////////////////////////////////////////////
    // load input file to dataframe
    DataFrame *loadDataframe()
    {
        buildSchema();
        if (sorer_debug) this->sch_.print_schema();

        DataFrame* df = new DataFrame(this->sch_);
        Row row(df->get_schema());
        char arow[2048];
        // load input file to dataframe df row by row
        fseek(fp_, 0, SEEK_SET);         // move file pointer to the beginning
        for (int i = 0; i < this->numberOfRows_; i++) // for each row
        {
            // get a line from the file
            fgets(arow, sizeof(arow), fp_);

            // loop through each column in the row, set each element to row, then add to df
            for (int col_id = 0; col_id < this->numberOfColumns_; col_id++)
            {
                // get value in row=i, column=col_id
                char *s = this->getValueByColumn(col_id, arow);

                // set value s to row at column=col_id
                if (colType_[col_id] == BOOLEAN)
                    row.set(col_id, (bool)atoi(s));
                else if (colType_[col_id] == INT)
                    row.set(col_id, (int)atoi(s));
                else if (colType_[col_id] == FLOAT)
                    row.set(col_id, (double)atof(s));
                else if (colType_[col_id] == STRING)
                    row.set(col_id, new String(s));
            }
 
            // finally add row to dataframe
            row.set_idx(i);
            df->add_row(row);
        }
        return df;
    }
};
