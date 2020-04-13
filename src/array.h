// lang::CwC
#pragma once
#include <cstddef>
#include "object.h"
#include "serializer.h"

/*
 * A resizeable array class
 * @author Jennifer Ai and Helen Liu
 */
class Array : public Object {
public:
   	Object **data_;	// pointer to first Object in the array
	size_t size_;   // current size of the array
	size_t max_;	// max capacity of the array

    // Empty constructor
    Array()
    {
	    this->data_ = new Object *[2]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = 2;
    }


    Array(size_t n)
    {
	    this->data_ = new Object *[n]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = n;
    }


    // Deep copy of other array
    Array(const Array* arr)
    {
        // allocate space for data[] with the other array's size
        this->data_ = new Object* [arr->size_];

        // loop through the other array to copy each element to data[i]
        for (int i=0; i<arr->size_; i++)
        {
            this->data_[i] = arr->data_[i];
        }
        // update size and max
        this->size_ = arr->size_;
        this->max_ = this->size_;
    }

    // Clear contents of this before this is freed
    ~Array() { this->clear(); }

    // Returns hash code for this
    size_t hash() 
    { 
        size_t h = 0;
        // sum up each data[i]'s hash value
        for (int i=0; i<size_; i++)  h += data_[i]->hash()+1;
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(const Object* obj)
    {
		const Array* newArr = dynamic_cast<const Array*>(obj);   // cast o to Array*
        if (!newArr) return 0;  // return false if dynamic cast fails
      	if (this->size_ != newArr->size_) return 0;  // return 0 if size is not the same
		if ((this->size_==0) && (newArr->size_==0))  return 1; // return true if both array are empty
    
		// compare each element in this Array and newArr 
		for (int i=0; i<this->size_; i++)
			if ( !this->data_[i]->equals(newArr->data_[i]) ) return 0;

		return 1;          // otherwise , return true
    }

    // Returns number of elements in this
    size_t count() { return this->size_; }

    // Return the object at the index or null
    Object* get(size_t idx) 
    { 
        //printf("array.get(): size=%lu   idx=%lu\n", this->size_, idx);
        if (idx < this->size_) return this->data_[idx];
        return nullptr;
    }

    // Append a copy of the object to the end of this and return this
    Array* append(Object* obj)
    {
        // if data[] is full, 1. double the space. 2. copy content to new array
		if (this->max_ == this->size_)                      // current arry is full
		{ 	
			Object** opp = new Object *[2*this->max_];      // double space
			this->max_ = 2*this->max_;                      //the array capacity is doubled

			//copy content of current array to the newArr
			for (int i=0; i<this->size_; i++) opp[i] = this->data_[i];
	
			// destory the current array, and use the new bigger array
			delete [] this->data_;
			this->data_ = opp;
		} 
		this->data_[this->size_] = obj;         // now, append o into array
		this->size_++;   			            // increase array size by 1
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    size_t index(const Object* obj)
    {
        for (size_t i=0; i<this->size_; i++)
        {
            if ( this->data_[i]->equals(const_cast<Object*>(obj))) 
                return i;
        }
        return -1;
    }

    // Appends a copy of all items of the other array to this and returns this
    Array* extend(const Array* arr)
    {
        // append each arr's element to THIS array
        for (int i=0; i<arr->size_; i++)
        {
            this->append(arr->data_[i]);
        }
        return this;
    }

    // Insert a copy of the object at the index if index from [0, this->count()] and return this
    Array* insert(size_t idx, Object* obj)
    {
        // if idx is not meaningful, do nothing and just return this
        if ( ((int)idx<0) || ((int)idx>this->count()) ) return this;

        // if the current data[] if full, double its capacity
		if (this->max_ == this->size_) 
		{ 	
			Object** newDataArr = new Object *[2*this->max_];   //double space
			this->max_ = 2*this->max_;                          //double the array capacity
  
            //printf("--insert():max=%d, idx=%d", max_, idx);
			//copy content of current data[0..idx-1] to the new data array
			for (int i=0; i<idx; i++)
			{
				newDataArr[i] = this->data_[i];
			}

            // insert obj to new data[idx]
            newDataArr[idx] = obj;             

            // copy content of current data[idx..size] to the newDataArr[]
            for (int i=idx; i<this->count(); i++)
            {
                newDataArr[i+1] = this->data_[i];
            }
			//destory the current data array, and use the new bigger data array
			delete [] this->data_;
			this->data_ = newDataArr;
		} 
        else 
        {
            // if current data[] has enough space, just insert
            // first, move data[idx..size] right 1 position
            for (int i=this->count(); i>idx; i--)
            {
                this->data_[i] = this->data_[i-1];
            }
            // now, insert obj
            this->data_[idx] = obj;
        }
		this->size_++;   			// increase array size by 1
        return this;
    }

    // Return and remove object at the index or return null
    Object* pop(size_t idx)
    {
        // only when idx is within this->count(), do pop
        if ( ((int)idx>=0) && ((int)idx<this->count()) )
        {
            // rembers the going to be removed object
            Object* ret = new Object();
            ret = this->data_[idx];

            // shift data[idx..size] left one position. i.e. remove data[idx]
            for (int i=idx; i<this->count()-1; i++)
            {
                this->data_[i] = this->data_[i+1];
            }
            this->size_--;
            return ret;
        }
        // otherwise, return null
        return nullptr;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    Array* remove(const Object* obj)
    {
        //printf("remove() start--------\n");
        size_t idx = this->index(obj);     // get the index of obj (1st occurrence)
        if (idx != -1)  this->pop(idx);     // pop the element with index
        return this;
    }

    // Delete contents of this
    void clear()
    {
        //printf("clear():---\n");
        if (this->data_) delete [] this->data_;
        this->data_ = nullptr;
        this->size_ = 0;
        this->max_ = 0;
    }

    // add this method to print out elements in data[]
   void printArr() 
   { 
        for (int i = 0; i < this->size_; i++) 
        {
            printf("%s  ", this->data_[i]->c_str());
            //this->data_[i]->print();
        }
        pln();
    }  
};

/////////////////////////////////////////////////////////////////////////
// Resizeable array with amortized appending to end of list and constant
// time retrieval
class IntArray : public Object {
public:
   	int *data_;	// pointer to first Object in the array
	size_t size_;   // current size of the array
	size_t max_;	// max capacity of the array

    // Empty constructor
    IntArray()
    {
	    this->data_ = new int [2]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = 2; 

        for (int i=0; i<this->size_; i++) this->data_[i] = 0;
    }

    // Deep copy of other array
    IntArray(const IntArray* arr)
    {
        // allocate space for data[] with the other array's size
        this->data_ = new int [arr->size_];

        // loop through the other array to copy each element to data[i]
        for (int i=0; i<arr->size_; i++)
        {
            this->data_[i] = arr->data_[i];
        }
        // update size and max
        this->size_ = arr->size_;
        this->max_ = this->size_;
    }

    // Clear contents of this before this is freed
    ~IntArray() { this->clear(); }

    // constructor with deserializer
    IntArray(Deserializer& d) {
        this->size_ = d.deserialize_size_t();
        this->max_ = d.deserialize_size_t();

        this->data_ = new int[this->max_];
        for (int i=0; i<this->size_; i++) {
            this->data_[i] = d.deserialize_int();
            //printf("i=%d, x=%d\n", i, data_[i]);
        }
    }

    // serializer
    void serialize(Serializer& s) {
        s.serialize_size_t(this->size_);
        s.serialize_size_t(this->max_);
        for (int i=0; i<this->size_; i++) {
            s.serialize_int(this->data_[i]);
        }
    }

    // Returns hash code for this
    size_t hash() 
    { 
        size_t h = 0;

        // sum up each data[i]'s hash value
        for (int i=0; i<size_; i++)
            //h += this->data_[i]->hash();
            h += this->data_[i];
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(const Object* obj)
    {
        //printf("---intarray.equals() begins----\n");
		const IntArray* newArr = dynamic_cast<const IntArray*>(obj);   // cast o to Array*
       
       // return false if case fail.
        if (!newArr) 
        {
            //printf("cast failed\n");
            return 0;  // return false if dynamic cast fails
        }
    
        // return false if size is not the same
    	if (this->size_ != newArr->size_) 
        {
            //printf("size not same: %lu != %lu\n", this->size_, newArr->size_);
            return 0;  // return 0 if size is not the same
        }
       
        // return true if both array are empty
		if ((this->size_==0) && (newArr->size_==0)) 
        {
            //printf("all size = 0\n");
            return 1;
        }
       
        //printf("equals(): size=%lu  = %lu\n", this->size_, newArr->size_);
		// compare each element in this Array and newArr 
		for (int i=0; i<this->size_; i++)
		{
            //printf("===%d", i);
            // return false if sees one element is different
			if ( !(this->data_[i]==newArr->data_[i])) return 0;
		}
        // otherwise , return true
		return 1;        
    }

    // Returns number of elements in this
    size_t count() { return this->size_; }

    // Return the object at the index or null
    int get(size_t idx) 
    { 
        //printf("array.get(): size=%lu   idx=%lu\n", this->size_, idx);
        if (idx < this->size_) return this->data_[idx];
        return 0;
    }

    // Append a copy of the object to the end of this and return this
    IntArray* append(int obj)
    {
        // if data[] is full, 1. double the space. 2. copy content to new array
		if (this->max_ == this->size_)                      // current arry is full
		{ 	
			int* opp = new int [2*this->max_];      // double space
			this->max_ = 2*this->max_;                      //the array capacity is doubled

			//copy content of current array to the newArr
			for (int i=0; i<this->size_; i++)
			{
				opp[i] = this->data_[i];
			}
			// destory the current array, and use the new bigger array
			delete [] this->data_;
			this->data_ = opp;
		} 
		this->data_[this->size_] = obj;         // now, append o into array
		this->size_++;   			            // increase array size by 1
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    size_t index(const int obj)
    {
        for (size_t i=0; i<this->size_; i++)
        {
            if ( this->data_[i] == obj)  return i;
        }
        return -1;
    }

    // Returns true if this contains int else false
    bool contains(const int obj)
    {
        if (index(obj) == -1) return 0;
        return 1; 
    }

    // Appends a copy of all items of the other array to this and returns this
    IntArray* extend(const IntArray* arr)
    {
        // append each arr's element to THIS array
        for (int i=0; i<arr->size_; i++)
        {
            this->append(arr->data_[i]);
        }
        return this;
    }

    // Insert a copy of the int at the index if index from [0, this->count()] and return this
    IntArray* insert(size_t idx, int obj)
    {
        // if idx is not meaningful, do nothing and just return this
        if ( ((int)idx<0) || ((int)idx>this->count()) ) return this;

        // if the current data[] if full, double its capacity
		if (this->max_ == this->size_) 
		{ 	
			int* newDataArr = new int [2*this->max_];   //double space
			this->max_ = 2*this->max_;                          //double the IntArray capacity
  
            //printf("--insert():max=%d, idx=%d", max_, idx);
			//copy content of current data[0..idx-1] to the new data array
			for (int i=0; i<idx; i++)
			{
				newDataArr[i] = this->data_[i];
			}

            // insert obj to new data[idx]
            newDataArr[idx] = obj;             

            // copy content of current data[idx..size] to the newDataArr[]
            for (int i=idx; i<this->count(); i++)
            {
                newDataArr[i+1] = this->data_[i];
            }
			//destory the current data array, and use the new bigger data array
			delete [] this->data_;
			this->data_ = newDataArr;
		} 
        else 
        {
            // if current data[] has enough space, just insert
            // first, move data[idx..size] right 1 position
            for (int i=this->count(); i>idx; i--)
            {
                this->data_[i] = this->data_[i-1];
            }
            // now, insert obj
            this->data_[idx] = obj;
        }
		this->size_++;   			// increase array size by 1
        return this;
    }

    // Return and remove int at the index or return null
    int pop(size_t idx)
    {
        // only when idx is within this->count(), do pop
        if ( ((int)idx>=0) && ((int)idx<this->count()) )
        {
            // rembers the going to be removed object
            int ret;
            ret = this->data_[idx];

            // shift data[idx..size] left one position. i.e. remove data[idx]
            for (int i=idx; i<this->count()-1; i++)
            {
                this->data_[i] = this->data_[i+1];
            }
            this->size_--;
            return ret;
        }
        // otherwise, return null
        return 0;
    }

    // Removes first occurance of int from this if this contains it and
    // return this
    IntArray* remove(const int obj)
    {
        //printf("remove() start--------\n");
        size_t idx = this->index(obj);     // get the index of obj (1st occurrence)
        if (idx != -1)  this->pop(idx);     // pop the element with index
        return this;
    }

    // Inplace reverse of this and return this
    IntArray* reverse()
    {
        int i = 0;
        int j = this->count()-1;
        int tmp;

        // i start from beginning of array, and j start from the end of array
        while (i != j)
        {
            // swap data[i] and data[j]
            tmp = this->data_[i];
            this->data_[i] = this->data_[j];
            this->data_[j] = tmp; 
            i++;        // increase i
            j--;        // decrease j
        }
        return this;
    }

    // Delete contents of this
    void clear()
    {
        //printf("clear():---\n");
        if (this->data_) delete [] this->data_;
        this->data_ = nullptr;
        this->size_ = 0;
        this->max_ = 0;
    }

    // add this method to print out elements in data[]
   void printArr() 
   { 
        for (int i = 0; i < this->size_; i++) 
            printf("int[%d]=%d ", i, this->data_[i]);
        pln();
    }  

    // serialize

};

/////////////////////////////////////////////////////////////////////////
// Resizeable array with amortized appending to end of list and constant
// time retrieval
class DoubleArray : public Object {
public:
   	double *data_;	// pointer to first Object in the array
	size_t size_;   // current size of the array
	size_t max_;	// max capacity of the array

    // Empty constructor
    DoubleArray()
    {
	    this->data_ = new double [2]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = 2;

        for (int i=0; i<this->size_; i++) this->data_[i] = 0.0;
    }

    // Deep copy of other array
    DoubleArray(const DoubleArray* arr)
    {
        // allocate space for data[] with the other array's size
        this->data_ = new double [arr->size_];

        // loop through the other array to copy each element to data[i]
        for (int i=0; i<arr->size_; i++)
        {
            this->data_[i] = arr->data_[i];
        }
        // update size and max
        this->size_ = arr->size_;
        this->max_ = this->size_;
    }

    // Clear contents of this before this is freed
    ~DoubleArray() { this->clear(); }

    // constructor with deserializer
    DoubleArray(Deserializer& d)  {
        this->size_ = d.deserialize_size_t();   // get size
        this->max_ = d.deserialize_size_t();    // get max_
        
        // allocate space for data_
        this->data_ = new double[this->max_];

        // get data[i] one by one
        for (int i=0; i<this->size_; i++)
            this->data_[i] = d.deserialize_double();
    }


    // serializer
    void serialize(Serializer& s) {
        s.serialize_size_t(this->size_);
        s.serialize_size_t(this->max_);
        for (int i=0; i<this->size_; i++) {
            s.serialize_double(this->data_[i]);
        }
    }

    // Returns hash code for this
    size_t hash() 
    { 
        size_t h = 0;

        // sum up each data[i]'s hash value
        for (int i=0; i<size_; i++)
            //h += this->data_[i]->hash();
            h += this->data_[i];
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(const Object* obj)
    {
        //printf("---intarray.equals() begins----\n");
		const DoubleArray* newArr = dynamic_cast<const DoubleArray*>(obj);   // cast o to Array*
       
       // return false if case fail.
        if (!newArr) 
        {
            //printf("cast failed\n");
            return 0;  // return false if dynamic cast fails
        }
    
        // return false if size is not the same
    	if (this->size_ != newArr->size_) 
        {
            //printf("size not same: %lu != %lu\n", this->size_, newArr->size_);
            return 0;  // return 0 if size is not the same
        }
       
        // return true if both array are empty
		if ((this->size_==0) && (newArr->size_==0)) 
        {
            //printf("all size = 0\n");
            return 1;
        }
       
        //printf("equals(): size=%lu  = %lu\n", this->size_, newArr->size_);
		// compare each element in this Array and newArr 
		for (int i=0; i<this->size_; i++)
		{
            //printf("===%d", i);
            // return false if sees one element is different
			if ( !(this->data_[i]==newArr->data_[i])) return 0;
		}
        // otherwise , return true
		return 1;        
    }

    // Returns number of elements in this
    size_t count() { return this->size_; }

    // Return the object at the index or null
    double get(size_t idx) 
    { 
        //printf("array.get(): size=%lu   idx=%lu\n", this->size_, idx);
        if (idx < this->size_) return this->data_[idx];
        return 0;
    }

    // Append a copy of the object to the end of this and return this
    DoubleArray* append(double obj)
    {
        // if data[] is full, 1. double the space. 2. copy content to new array
		if (this->max_ == this->size_)                      // current arry is full
		{ 	
			double* opp = new double [2*this->max_];      // double space
			this->max_ = 2*this->max_;                      //the array capacity is doubled

			//copy content of current array to the newArr
			for (int i=0; i<this->size_; i++)
			{
				opp[i] = this->data_[i];
			}
			// destory the current array, and use the new bigger array
			delete [] this->data_;
			this->data_ = opp;
		} 
		this->data_[this->size_] = obj;         // now, append o into array
		this->size_++;   			            // increase array size by 1
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    int index(const double obj)
    {
        for (size_t i=0; i<this->size_; i++)
        {
            if ( this->data_[i] == obj)  return i;
        }
        return -1;
    }

    // Returns true if this contains int else false
    bool contains(const double obj)
    {
        if (index(obj) == -1) return 0;
        return 1; 
    }

    // Appends a copy of all items of the other array to this and returns this
    DoubleArray* extend(const DoubleArray* arr)
    {
        // append each arr's element to THIS array
        for (int i=0; i<arr->size_; i++)
        {
            this->append(arr->data_[i]);
        }
        return this;
    }

    // Insert a copy of the int at the index if index from [0, this->count()] and return this
    DoubleArray* insert(size_t idx, double obj)
    {
        // if idx is not meaningful, do nothing and just return this
        if ( ((int)idx<0) || ((int)idx>this->count()) ) return this;

        // if the current data[] if full, double its capacity
		if (this->max_ == this->size_) 
		{ 	
			double* newDataArr = new double [2*this->max_];   //double space
			this->max_ = 2*this->max_;                          //double the IntArray capacity
  
            //printf("--insert():max=%d, idx=%d", max_, idx);
			//copy content of current data[0..idx-1] to the new data array
			for (int i=0; i<idx; i++)
			{
				newDataArr[i] = this->data_[i];
			}

            // insert obj to new data[idx]
            newDataArr[idx] = obj;             

            // copy content of current data[idx..size] to the newDataArr[]
            for (int i=idx; i<this->count(); i++)
            {
                newDataArr[i+1] = this->data_[i];
            }
			//destory the current data array, and use the new bigger data array
			delete [] this->data_;
			this->data_ = newDataArr;
		} 
        else 
        {
            // if current data[] has enough space, just insert
            // first, move data[idx..size] right 1 position
            for (int i=this->count(); i>idx; i--)
            {
                this->data_[i] = this->data_[i-1];
            }
            // now, insert obj
            this->data_[idx] = obj;
        }
		this->size_++;   			// increase array size by 1
        return this;
    }

    // Return and remove int at the index or return null
    double pop(size_t idx)
    {
        // only when idx is within this->count(), do pop
        if ( ((int)idx>=0) && ((int)idx<this->count()) )
        {
            // rembers the going to be removed object
            double ret;
            ret = this->data_[idx];

            // shift data[idx..size] left one position. i.e. remove data[idx]
            for (int i=idx; i<this->count()-1; i++)
            {
                this->data_[i] = this->data_[i+1];
            }
            this->size_--;
            return ret;
        }
        // otherwise, return null
        return 0.0;
    }

    // Removes first occurance of int from this if this contains it and
    // return this
    DoubleArray* remove(const int obj)
    {
        //printf("remove() start--------\n");
        size_t idx = this->index(obj);     // get the index of obj (1st occurrence)
        if (idx != -1)  this->pop(idx);     // pop the element with index
        return this;
    }

    // Inplace reverse of this and return this
    DoubleArray* reverse()
    {
        int i = 0;
        int j = this->count()-1;
        double tmp;

        // i start from beginning of array, and j start from the end of array
        while (i != j)
        {
            // swap data[i] and data[j]
            tmp = this->data_[i];
            this->data_[i] = this->data_[j];
            this->data_[j] = tmp; 
            i++;        // increase i
            j--;        // decrease j
        }
        return this;
    }

    // Delete contents of this
    void clear()
    {
        //printf("clear():---\n");
        if (this->data_) delete [] this->data_;
        this->data_ = nullptr;
        this->size_ = 0;
        this->max_ = 0;
    }

    // add this method to print out elements in data[]
   void printArr() 
   { 
        for (int i = 0; i < this->size_; i++) 
            printf("int[%d]=%f ", i, this->data_[i]);
        pln();
    }  
};

///////////////////////////////////////////////////////////////////////
// Resizeable array with amortized appending to end of list and constant
// time retrieval
class BoolArray : public Object {
public:
   	bool *data_;	// pointer to first Object in the array
	size_t size_;   // current size of the array
	size_t max_;	// max capacity of the array

    // Empty constructor
    BoolArray()
    {
	    this->data_ = new bool [2]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = 2;
        for (int i=0; i<this->size_; i++) this->data_[i] = 0;
    }

    // Deep copy of other array
    BoolArray(const BoolArray* arr)
    {
        // allocate space for data[] with the other array's size
        this->data_ = new bool [arr->size_];

        // loop through the other array to copy each element to data[i]
        for (int i=0; i<arr->size_; i++)
        {
            this->data_[i] = arr->data_[i];
        }
        // update size and max
        this->size_ = arr->size_;
        this->max_ = this->size_;
    }

    // Clear contents of this before this is freed
    ~BoolArray() { this->clear(); }

    // constructor with deserializer
    BoolArray(Deserializer& d) {
        this->size_ = d.deserialize_size_t();
        this->max_ = d.deserialize_size_t();

        this->data_ = new bool[this->max_];
        for (int i=0; i<this->size_; i++) {
            this->data_[i] = d.deserialize_bool();
            //printf("==== data[%d] = %d\n", i, data_[i]);
        }
    }

    // serializer
    void serialize(Serializer& s) {
        s.serialize_size_t(this->size_);
        s.serialize_size_t(this->max_);
        for (int i=0; i<this->size_; i++) {
            s.serialize_bool(this->data_[i]);
        }
    }

    // Returns hash code for this
    size_t hash() 
    { 
        size_t h = 0;

        // sum up each data[i]'s hash value
        for (int i=0; i<size_; i++)
            //h += this->data_[i]->hash();
            h += this->data_[i];
        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(const Object* obj)
    {
        //printf("---intarray.equals() begins----\n");
		const BoolArray* newArr = dynamic_cast<const BoolArray*>(obj);   // cast o to Array*
     
       // return false if case fail.
        if (!newArr) 
        {
            //printf("cast failed\n");
            return 0;  // return false if dynamic cast fails
        }
  
        // return false if size is not the same
    	if (this->size_ != newArr->size_) 
        {
            //printf("size not same: %lu != %lu\n", this->size_, newArr->size_);
            return 0;  // return 0 if size is not the same
        }
      
        // return true if both array are empty
		if ((this->size_==0) && (newArr->size_==0)) 
        {
            //printf("all size = 0\n");
            return 1;
        }
    
        //printf("equals(): size=%lu  = %lu\n", this->size_, newArr->size_);
		// compare each element in this Array and newArr 
		for (int i=0; i<this->size_; i++)
		{
            //printf("===%d", i);
            // return false if sees one element is different
			if ( !this->data_[i]==newArr->data_[i]) return 0;
		}
        // otherwise , return true
		return 1;        
    }

    // Returns number of elements in this
    size_t count() { return this->size_; }

    // Return the object at the index or null
    bool get(size_t idx) 
    { 
        //printf("array.get(): size=%lu   idx=%lu\n", this->size_, idx);
        if (idx < this->size_) return this->data_[idx];
        return 9;
    }

    // Append a copy of the object to the end of this and return this
    BoolArray* append(bool obj)
    {
        // if data[] is full, 1. double the space. 2. copy content to new array
		if (this->max_ == this->size_)                      // current arry is full
		{ 	
			bool* opp = new bool [2*this->max_];      // double space
			this->max_ = 2*this->max_;                      //the array capacity is doubled

			//copy content of current array to the newArr
			for (int i=0; i<this->size_; i++)
			{
				opp[i] = this->data_[i];
			}
			// destory the current array, and use the new bigger array
			delete [] this->data_;
			this->data_ = opp;
		} 
		this->data_[this->size_] = obj;         // now, append o into array
		this->size_++;   			            // increase array size by 1
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    size_t index(const bool obj)
    {
        for (size_t i=0; i<this->size_; i++)
        {
            if ( this->data_[i] == obj)  return i;
        }
        return -1;
    }

    // Returns true if this contains int else false
    bool contains(const bool obj)
    {
        if (index(obj) == -1) return 0;
        return 1; 
    }

    // Appends a copy of all items of the other array to this and returns this
    BoolArray* extend(const BoolArray* arr)
    {
        // append each arr's element to THIS array
        for (int i=0; i<arr->size_; i++)
        {
            this->append(arr->data_[i]);
        }
        return this;
    }

    // Insert a copy of the int at the index if index from [0, this->count()] and return this
    BoolArray* insert(size_t idx, bool obj)
    {
        // if idx is not meaningful, do nothing and just return this
        if ( ((int)idx<0) || ((int)idx>this->count()) ) return this;

        // if the current data[] if full, double its capacity
		if (this->max_ == this->size_) 
		{ 	
			bool* newDataArr = new bool [2*this->max_];   //double space
			this->max_ = 2*this->max_;                          //double the IntArray capacity
  
            //printf("--insert():max=%d, idx=%d", max_, idx);
			//copy content of current data[0..idx-1] to the new data array
			for (int i=0; i<idx; i++)
			{
				newDataArr[i] = this->data_[i];
			}

            // insert obj to new data[idx]
            newDataArr[idx] = obj;             

            // copy content of current data[idx..size] to the newDataArr[]
            for (int i=idx; i<this->count(); i++)
            {
                newDataArr[i+1] = this->data_[i];
            }
			//destory the current data array, and use the new bigger data array
			delete [] this->data_;
			this->data_ = newDataArr;
		} 
        else 
        {
            // if current data[] has enough space, just insert
            // first, move data[idx..size] right 1 position
            for (int i=this->count(); i>idx; i--)
            {
                this->data_[i] = this->data_[i-1];
            }
            // now, insert obj
            this->data_[idx] = obj;
        }
		this->size_++;   			// increase array size by 1
        return this;
    }

    // Return and remove int at the index or return null
    bool pop(size_t idx)
    {
        // only when idx is within this->count(), do pop
        if ( ((int)idx>=0) && ((int)idx<this->count()) )
        {
            // rembers the going to be removed object
            bool ret;
            ret = this->data_[idx];

            // shift data[idx..size] left one position. i.e. remove data[idx]
            for (int i=idx; i<this->count()-1; i++)
            {
                this->data_[i] = this->data_[i+1];
            }
            this->size_--;
            return ret;
        }
        // otherwise, return null
        return 0;
    }

    // Removes first occurance of int from this if this contains it and
    // return this
    BoolArray* remove(const bool obj)
    {
        //printf("remove() start--------\n");
        size_t idx = this->index(obj);     // get the index of obj (1st occurrence)
        if (idx != -1)  this->pop(idx);     // pop the element with index
        return this;
    }

    // Inplace reverse of this and return this
    BoolArray* reverse()
    {
        int i = 0;
        int j = this->count()-1;
        bool tmp;

        // i start from beginning of array, and j start from the end of array
        while (i != j)
        {
            // swap data[i] and data[j]
            tmp = this->data_[i];
            this->data_[i] = this->data_[j];
            this->data_[j] = tmp; 
            i++;        // increase i
            j--;        // decrease j
        }
        return this;
    }

    // Delete contents of this
    void clear()
    {
        //printf("clear():---\n");
        if (this->data_) delete [] this->data_;
        this->data_ = nullptr;
        this->size_ = 0;
        this->max_ = 0;
    }

    // add this method to print out elements in data[]
   void printArr() 
   { 
        for (int i = 0; i < this->size_; i++) 
            printf("bool[%d]=%d ", i, this->data_[i]);
        pln();
    }  
};

class StringArray : public Object {
public:
   	String **data_;	// pointer to first Object in the array
	size_t size_;   // current size of the array
	size_t max_;	// max capacity of the array

    // Empty constructor
    StringArray()
    {
	    this->data_ = new String *[2]; // allocate initial space for data
		this->size_ = 0;
		this->max_ = 2;
    }

    // Deep copy of other array
    StringArray(const StringArray* arr)
    {
        // allocate space for data[] with the other array's size
        this->data_ = new String* [arr->size_];

        // loop through the other array to copy each element to data[i]
        for (int i=0; i<arr->size_; i++)
        {
            this->data_[i] = arr->data_[i];
        }
        // update size and max
        this->size_ = arr->size_;
        this->max_ = this->size_;
    }

    // Clear contents of this before this is freed
    ~StringArray() { this->clear(); }

    // constructor with deserializer
    StringArray(Deserializer& d) {
        this->size_ = d.deserialize_size_t();
        this->max_ = d.deserialize_size_t();
printf("--------4 ---- size=%d, max=%d\n", size_, max_);        
        this->data_ = new String*[this->max_];
        for (int i=0; i<this->size_; i++) {
            this->data_[i] = new String(d);
            printf("====data-size=%d, data[%d]=%s\n", data_[i]->size_, i, data_[i]->cstr_);
        }
    }

    // serializer
    void serialize(Serializer& s) {
        s.serialize_size_t(this->size_);
        s.serialize_size_t(this->max_);
        for (int i=0; i<this->size_; i++) {
            this->data_[i]->serialize(s);
        }
    }


    // Returns hash code for this
    size_t hash() 
    { 
        size_t h = 0;

        // sum up each data[i]'s hash value
        for (int i=0; i<size_; i++)
            h += data_[i]->hash()+1;

        return h;
    }

    // Returns true if this equals the other object else false
    bool equals(const Object* obj)
    {
        //printf("---array.equals() begins----\n");
		const StringArray* newArr = dynamic_cast<const StringArray*>(obj);   // cast o to Array*
       
       // return false if case fail.
        if (!newArr) 
        {
            //printf("cast failed\n");
            return 0;  // return false if dynamic cast fails
        }
    
        // return false if size is not the same
    	if (this->size_ != newArr->size_) 
        {
            //printf("size not same: %lu != %lu\n", this->size_, newArr->size_);
            return 0;  // return 0 if size is not the same
        }
      
        // return true if both array are empty
		if ((this->size_==0) && (newArr->size_==0)) 
        {
            //printf("all size = 0\n");
            return 1;
        }
      
        //printf("equals(): size=%lu  = %lu\n", this->size_, newArr->size_);
		// compare each element in this Array and newArr 
		for (int i=0; i<this->size_; i++)
		{
            //printf("===%d", i);
            // return false if sees one element is different
			if ( !this->data_[i]->equals(newArr->data_[i]) ) return 0;
		}
        // otherwise , return true
		return 1;        
    }

    // Returns number of elements in this
    size_t count() { return this->size_; }

    // Return the object at the index or null
    String* get(size_t idx) 
    { 
        //printf("array.get(): size=%lu   idx=%lu\n", this->size_, idx);
        if (idx < this->size_) return this->data_[idx];
        return nullptr;
    }

    // Append a copy of the object to the end of this and return this
    StringArray* append(String* obj)
    {
        // if data[] is full, 1. double the space. 2. copy content to new array
		if (this->max_ == this->size_)                      // current arry is full
		{ 	
			String** opp = new String *[2*this->max_];      // double space
			this->max_ = 2*this->max_;                      //the array capacity is doubled

			//copy content of current array to the newArr
			for (int i=0; i<this->size_; i++)
			{
				opp[i] = this->data_[i];
			}
			// destory the current array, and use the new bigger array
			delete [] this->data_;
			this->data_ = opp;
		} 
		this->data_[this->size_] = obj;         // now, append o into array
		this->size_++;   			            // increase array size by 1
        return this;
    }

    // Return index of object in this or -1 if the object is not contained
    size_t index(const String* obj)
    {
        for (size_t i=0; i<this->size_; i++)
        {
            if ( this->data_[i]->equals(const_cast<String*>(obj))) 
                return i;
        }
        return -1;
    }

    // Returns true if this contains object else false
    bool contains(const String* obj)
    {
        if (index(obj) == -1) return 0;
        return 1; 
    }

    // Appends a copy of all items of the other array to this and returns this
    StringArray* extend(const StringArray* arr)
    {
        // append each arr's element to THIS array
        for (int i=0; i<arr->size_; i++)
        {
            this->append(arr->data_[i]);
        }
        return this;
    }

    // Insert a copy of the object at the index if index from [0, this->count()] and return this
    StringArray* insert(size_t idx, String* obj)
    {
        // if idx is not meaningful, do nothing and just return this
        if ( ((int)idx<0) || ((int)idx>this->count()) ) return this;

        // if the current data[] if full, double its capacity
		if (this->max_ == this->size_) 
		{ 	
			String** newDataArr = new String *[2*this->max_];   //double space
			this->max_ = 2*this->max_;                          //double the array capacity
  
            //printf("--insert():max=%d, idx=%d", max_, idx);
			//copy content of current data[0..idx-1] to the new data array
			for (int i=0; i<idx; i++)
			{
				newDataArr[i] = this->data_[i];
			}

            // insert obj to new data[idx]
            newDataArr[idx] = obj;             

            // copy content of current data[idx..size] to the newDataArr[]
            for (int i=idx; i<this->count(); i++)
            {
                newDataArr[i+1] = this->data_[i];
            }
			//destory the current data array, and use the new bigger data array
			delete [] this->data_;
			this->data_ = newDataArr;
		} 
        else 
        {
            // if current data[] has enough space, just insert
            // first, move data[idx..size] right 1 position
            for (int i=this->count(); i>idx; i--)
            {
                this->data_[i] = this->data_[i-1];
            }
            // now, insert obj
            this->data_[idx] = obj;
        }
		this->size_++;   			// increase array size by 1
        return this;
    }

    // Return and remove object at the index or return null
    String* pop(size_t idx)
    {
        // only when idx is within this->count(), do pop
        if ( ((int)idx>=0) && ((int)idx<this->count()) )
        {
            // rembers the going to be removed object
            //String* ret = new String("");
            //ret = this->data_[idx];
            String* ret = new String(this->data_[idx]->c_str());

            // shift data[idx..size] left one position. i.e. remove data[idx]
            for (int i=idx; i<this->count()-1; i++)
            {
                this->data_[i] = this->data_[i+1];
            }
            this->size_--;
            return ret;
        }
        // otherwise, return null
        return nullptr;
    }

    // Removes first occurance of object from this if this contains it and
    // return this
    StringArray* remove(const String* obj)
    {
        //printf("remove() start--------\n");
        size_t idx = this->index(obj);     // get the index of obj (1st occurrence)
        if (idx != -1)  this->pop(idx);     // pop the element with index
        return this;
    }

    // Inplace reverse of this and return this
    StringArray* reverse()
    {
        int i = 0;
        int j = this->count()-1;
        String* tmp = new String(this->data_[0]->c_str());

        // i start from beginning of array, and j start from the end of array
        while (i != j)
        {
            // swap data[i] and data[j]
            tmp = this->data_[i];
            this->data_[i] = this->data_[j];
            this->data_[j] = tmp; 
            i++;        // increase i
            j--;        // decrease j
        }
        return this;
    }

    // Delete contents of this
    void clear()
    {
        //printf("clear():---\n");
        if (this->data_) delete [] this->data_;
        this->data_ = nullptr;
        this->size_ = 0;
        this->max_ = 0;
    }

    // add this method to print out elements in data[]
   void printArr() 
   { 
        for (int i = 0; i < this->size_; i++) 
             printf("%s  ", this->data_[i]->c_str());

            //this->data_[i]->print();
        pln();
    }  
};

