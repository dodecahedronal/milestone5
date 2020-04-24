// lang::CwC
#pragma once
#include <cstddef>
#include <stdlib.h>
#include <stdio.h> 
#include <string.h>

#include <sys/socket.h>
#include <unistd.h> 
#include <sys/socket.h> 
#include <netinet/in.h> 

#include "object.h"
#include "serializer.h"
#include "string.h"
#include "dataframe.h"

enum class MsgKind { Ack, Nack, Put,
                    Reply,  Get, WaitAndGet, Status,
                    Kill,   Register,  Directory };

class Message : public Object {
public:
    MsgKind kind_;  // the message kind
    size_t sender_; // the index of the sender node
    size_t target_; // the index of the receiver node
    size_t id_;     // an id t unique within the node

    // constructor with Deserializer d
    Message(Deserializer& d) {
        //printf("Message::Message(d)\n");
        //this->kind_ = (MsgKind) d.deserialize_size_t();
        this->sender_ = d.deserialize_size_t();
        this->target_ = d.deserialize_size_t();
        this->id_ = d.deserialize_size_t();
    }

    Message() {}

    virtual bool equals(Message* other) {
        if (this->kind_ != other->kind_) return false;
        if (this->sender_ != other->sender_) return false;
        if (this->target_ != other->target_) return false;
        if (this->id_ != other->id_) return false;

        // otherwise return true
        return true;
    }

    // serialize a message
    virtual void serialize(Serializer& s) {
        //printf("kind=%zd, sender=%zd, target=%zd, id=%zd\n", (size_t)this->kind_, this->sender_, this->target_, this->id_);
        s.serialize_size_t( (size_t&) this->kind_);
        s.serialize_size_t(this->sender_);
        s.serialize_size_t(this->target_);
        s.serialize_size_t(this->id_);
        //s.print_size_t(0);
    }
    
    // deserialize: return a message
    static Message* deserialize(Deserializer& d);

    virtual void printMessage() {
        printf("    Message::printMsg(): kind=%d, sender=%d, target=%d, id=%d\n", (int)kind_, (int)sender_, (int)target_, (int)id_);
    }
};

// acknowledge message class
class Ack : public Message {
public:
    // constructor
    Ack(Deserializer& d) : Message(d) {}

    // destructor
    ~Ack() {}
};

///////////////////////////////////
// status message class
class Status : public Message {
public:
    String* msg_;

    // constructor
    Status(Deserializer &d) : Message(d)
    {
        //printf("Status::Status(d)\n");
        msg_ = new String(d);
    }

    Status(String *s) : msg_(s) {}

    // Serialize
    void serialize(Serializer &s)
    {
        Message::serialize(s);
        this->msg_->serialize(s);
    }

    bool equals(Status* other) {
        //bool ret = Message::equals(other);
        return Message::equals(other) && this->msg_->equals(other->msg_);
    }
};

////////////////////////////////////////
// register message
class Register : public Message {
public:
    sockaddr_in client_;  // client socket address structure
    size_t port_;           // client port

    // constructor: initialize client_ and port_ with given deserializer
    Register(Deserializer& d) : Message(d) {
        this->kind_ = MsgKind::Register;
        char *dst = (char *)&client_;
        d.deserialize_chars(dst, sizeof(sockaddr_in));
        this->port_ = d.deserialize_size_t();
    }

    Register(sockaddr_in& addr, size_t port) {
        memset(&client_, 0, sizeof(client_));
        client_.sin_family = addr.sin_family;
        client_.sin_port = addr.sin_port;
        client_.sin_addr = addr.sin_addr;

        port_ = port;
    }

    Register(size_t idx, size_t port) 
    {
        this->target_ = 0;      // register is alway to the server(node 0)
        this->sender_ = idx;    // sender is itself;
        this->kind_ = MsgKind::Register;
        this->port_ = port;
        client_.sin_family = AF_INET;
        client_.sin_port = htons(port);
        client_.sin_addr.s_addr = INADDR_ANY;
    }

    // serialize 
    void serialize(Serializer& s) {
        //serialize parent
        Message::serialize(s);      

        // serialize this->client as a string
        char* p = (char*)&this->client_;
        s.serialize_chars(p, sizeof(sockaddr_in));

        // serialize port_
        s.serialize_size_t(this->port_);
    }

    // return sender from base class Message
    size_t sender() { return this->sender_; }

    bool equals(Register* other) {
        if (client_.sin_family != other->client_.sin_family) return false;
        if (client_.sin_addr.s_addr != other->client_.sin_addr.s_addr) return false;
        if (client_.sin_port != other->client_.sin_port) return false;
        if (port_ != other->port_) return false;

        return Message::equals(other);
    }

};

/////////////////////////////////////////////// 
// have information for the registered clients
class Directory : public Message {
public:
   size_t clients_;   // number of clients ?
   size_t * ports_;  // port numbers
   size_t * addr_;  // ip addresses

    Directory(size_t* p, size_t* addr) {
        this->sender_ = 0;    
        this->kind_ = MsgKind::Directory;
        clients_ = args.numNodes_;
        ports_ = p;
        addr_ = addr;
    }

   // constructor
   Directory(Deserializer& d) : Message(d) {
       this->kind_ = MsgKind::Directory;
       // get client_
       this->clients_ = d.deserialize_size_t();

        // get ports_[]
       this->ports_ = new size_t[this->clients_];
       this->addr_ = new size_t[this->clients_];
       for (int i=0; i<this->clients_; i++) {
           this->ports_[i] = d.deserialize_size_t();
           this->addr_[i] = d.deserialize_size_t();
       }
   }

   // serialize a directory message
   void serialize(Serializer &s)
   {
       Message::serialize(s);
       s.serialize_size_t(this->clients_);

       for (int i=0; i<this->clients_; i++) {
            s.serialize_size_t(this->ports_[i]);
            s.serialize_size_t(this->addr_[i]);
       }
   }

    // print directory content
   void printDir()
   {
       printf("     printDir(): -------\n");
       printf("     kind=%d, sender=%d, target=%d, id=%d, nclient=%d\n", (int)kind_, (int)sender_, (int)target_, (int)id_, (int)clients_);
       for (int i=1; i<args.numNodes_; i++) 
           printf("     ports[%d]=%d, addr[%d]=%d\n", i, (int)ports_[i], i, (int)addr_[i]);
   }
};

///////////////////////////////////////////
// DataFrame message class
class Put : public Message {
public:
    DataFrame* data_;
    Key* key_;

    Put(size_t from, size_t to, DataFrame* df, Key* k) {
        this->id_ = 0;
        this->target_ = to;
        this->sender_ = from;
        this->kind_ = MsgKind::Put;
        this->data_ = df;
        this->key_ = k;
    }

    Put(Deserializer& d) : Message(d) {
        this->kind_ = MsgKind::Put;
        //printMessage();
        data_ = new DataFrame(d);
        key_ = new Key(d);
    }

    void serialize(Serializer& s) {
        Message::serialize(s);
        //data_->print_dataframe();
        data_->serialize(s);
        key_->serialize(s);
    }
};

// now implement Message::deserialize()
Message* Message::deserialize(Deserializer& d) {
    MsgKind kind = (MsgKind) d.deserialize_size_t();
    //printf("    Message::deserialize(): kind=%d\n", (int)kind);
    switch (kind) {
        case MsgKind::Ack: return (new Ack(d));
        case MsgKind::Status: return (new Status(d));
        case MsgKind::Register: return (new Register(d));
        case MsgKind::Directory: return (new Directory(d));
        case MsgKind::Put: return (new Put(d));
        case MsgKind::Nack:
        case MsgKind::Get:
        case MsgKind::Kill:
        case MsgKind::Reply:
        case MsgKind::WaitAndGet: return (new Message());
    }
    return (new Message());
};

