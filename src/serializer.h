// lang::CwC
#pragma once
#include <cstddef>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

class Serializer {
public:
    char* sdata_ = new char[1024];  //data serialized so far
    size_t length_ = 0; //number of char serialized

    // constructor: empty
    Serializer() {}

    // destructor
    ~Serializer() { if (this->sdata_) delete [] this->sdata_; }

    size_t get_size() {return length_;}
    char* get_serialized_data() { return sdata_;}
    
    // serialize for size_t type data
    void serialize_size_t(size_t& data) {
        memcpy(this->sdata_+this->length_, &data, sizeof(size_t));
        this->length_ += sizeof(size_t);    // advance the buffer index
        //printf("data=%zd, this->data=%zd, length=%d\n", (size_t)data, (size_t)this->sdata_[0], this->length_);
    }

    // serialize for double type data
    void serialize_double(double& data) {
        memcpy(this->sdata_+this->length_, &data, sizeof(double));
        this->length_ += sizeof(double);    // advance the buffer index
    }
    // serialize for int type data
    void serialize_int(int& data) {
        memcpy(this->sdata_+this->length_, &data, sizeof(int));
        this->length_ += sizeof(int);    // advance the buffer index
    }
    // serialize for bool type data
    void serialize_bool(bool& data) {
        memcpy(this->sdata_+this->length_, &data, sizeof(bool));
        this->length_ += sizeof(bool);    // advance the buffer index
    }

   // serialize for char* type data
    void serialize_chars(char* data, size_t len)
    {
        memcpy(this->sdata_+this->length_, data, len*sizeof(char));
        //printf("serialize() length=%d, ", length_);
        this->length_ += len;   // advance the buffer index
    }

    void print_size_t(int startFrom)
    {
        printf("print_size_t()----\n");
        for (int i=startFrom; i<startFrom+this->length_; i+=8) printf("%zd ", this->sdata_[i]);
        printf("\n");
    }
    void print_char(int startFrom) {
        printf("print_char-------------\n");
        for (int i=startFrom; i<startFrom+this->length_; i++) 
            printf("%c", this->sdata_[i]);
        printf("\n");
    }
};

////////////////////////////////////////////////////
// deserialize data
class Deserializer {
public:
    char* ddata_;       // data need to be serialized
    size_t dlen_ = 0; // number of char that is deserialized

    // constructor: take in tobe deserialized data and length
    Deserializer(char* data, size_t len) {
        //printf("\n-----Deserializer:: constructor -----\n");
        //for (int i=0; i<len; i++) printf("%zd ", data[i]);
        this->ddata_ = new char[len+1];
        memcpy(this->ddata_, data, len*sizeof(char));    // get data to be deserialized
        this->dlen_ = 0;                  // set number of char which is deserialized
    }
    
    // destructor
    ~Deserializer() { if (this->ddata_) delete [] this->ddata_; }

    // return a deserialized size_t data after deserialize size_t type data 
    size_t deserialize_size_t() {
        size_t v;
        memcpy(&v, this->ddata_ + this->dlen_, sizeof(size_t));
        this->dlen_ += sizeof(size_t);
        return v;
    }

    // return a deserialized double data 
    double deserialize_double() {
        double v;
        memcpy(&v, this->ddata_ + this->dlen_, sizeof(double));
        this->dlen_ += sizeof(double);
        return v;
    }

    // return a deserialized int data 
    int deserialize_int() {
        int v;
        memcpy(&v, this->ddata_ + this->dlen_, sizeof(int));
        this->dlen_ += sizeof(int);
        return v;
    }
    // return a deserialized bool data
    bool deserialize_bool() {
        bool v;
        memcpy(&v, this->ddata_ + this->dlen_, sizeof(bool));
        this->dlen_ += sizeof(bool);
        return v;
    }

    // return a deserialize string after deserialize char* type
    char* deserialize_chars(size_t strLen) {
        char* ret = new char[strLen+1];
        memcpy(ret, this->ddata_ + this->dlen_, strLen*sizeof(char));
        ret[strLen] = '\0';
        this->dlen_ += strLen*sizeof(char);
        //printf("deserialize_char():: ret=%s dlen=%d, strlen=%d\n", ret, this->dlen_, strLen);
        return ret;
    }

    void deserialize_chars(char* data, size_t strLen) {
        memcpy(data, this->ddata_ + this->dlen_, strLen*sizeof(char));
        this->dlen_ += strLen*sizeof(char);
    }

    void print_size_t(int startFrom, int len)
    {
        printf("Deserializer::print_size_t()----\n");
        for (int i=startFrom; i<startFrom+len; i++) printf("%zd ", this->ddata_[i]);
        printf("\n");
    }
    void print_char(int startFrom, int len)
    {
        printf("Deserializer::print_char()----\n");
        //for (int i=startFrom; i<len; i++) printf("data[%d]=%c\n", i, this->ddata_[i]);
        for (int i=startFrom; i<startFrom+len; i++) printf("%c", this->ddata_[i]);
        printf("\n");
    }
};

