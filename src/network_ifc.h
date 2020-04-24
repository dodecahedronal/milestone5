// LANGUAGE : CwC
#pragma once
#include "object.h"
#include "message.h"

// a abstract class for network interface
class NetworkIfc : public Object {
public:
    // register node
    virtual void register_node(size_t idx) {}

    // return node index
    virtual size_t index() {return 0;}

    // send message msg
    virtual void send_msg(Message* msg) = 0;

    // wait for a message to arrive
    virtual Message* recv_msg() = 0;
};
