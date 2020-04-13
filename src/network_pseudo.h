// LANGUAGE: CwC
#include "object.h"
#include "array.h"
#include "thread.h"
#include "message.h"
#include "network_ifc.h"
#include "map.h"
#include "procArgs.h"

/************
 * MessageQueue : an array with lock for atomic push/pop
 * **********/
class MessageQueue : public Object {
public:
    Array mq_;      //message queue
    Lock lock_;     // lock for atomic pop/push

    // constructor: empty q
    MessageQueue() : mq_() {}

    // push message msg to q atomically
    void push(Message* msg) {
        this->lock_.lock();
        this->mq_.append(msg);
        this->lock_.notify_all();
        this->lock_.unlock();
    }

    // pop amessage from q
    Message* pop() {
        lock_.lock();                           // lock
        while (mq_.count() == 0) lock_.wait();  // wait if q is empty
        Message* ret = dynamic_cast<Message*> (mq_.pop(0));  // pop the 1st element
        lock_.unlock();
        return ret;
    }
}; // MessageQueue

//extern ProcArgs arg;

/**********
 MessageQueueArray: an array of message queue
**********/
class MessageQueueArray : public Array {
public:
//    Array qArray;
    // constructor
    MessageQueueArray() : Array() {}

    // get the ith element in qArray
    MessageQueue *get(size_t i) {
        //printf("==== array i=%d size=%d\n", i, size_);
        MessageQueue* ret = dynamic_cast<MessageQueue*>(Array::get(i));
        return ret;
    } 
}; // MessageQueueArray

/**********
 communication layer for each node being represented by a thread
**********/
class NetworkPseudo : public NetworkIfc {
public:
    STMap threads_;         // map thread string -> size_t
    Lock threads_lock_;     // lock for threads
    MessageQueueArray qs_;  // array of message queues, one per thread

    // constructor: create an empty entry in q for each node
    NetworkPseudo() : qs_() {
        for (int i=0; i<args.numNodes_; i++)
            qs_.append(new MessageQueue());
    }

    //map index to thread ID
    void register_node(size_t index) override {
        threads_lock_.lock();
        String* threadId = Thread::thread_id();
        this->threads_.set(threadId, index);
        threads_lock_.unlock();
        //printf("register node %d\n", index);
        //delete threadId;
    }

    // send message msg <=> push to q
    void send_msg(Message* msg) override {
        MessageQueue* mq = this->qs_.get(msg->target_); // get the mq for node target_
        //printf("send msg to q=%d\n",msg->target_);
        if (mq)
            mq->push(msg);    // push the msg to the corresponding q
        else
            printf("Queue %d not found\n", msg->target_);
    }

    // receive message and return <=> pop from q
    Message* recv_msg() override {
        threads_lock_.lock();
        String* threadId = Thread::thread_id(); // get thread id
        size_t i = threads_.get(threadId);      // get index of the thread id
        threads_lock_.unlock();
 
 
        MessageQueue* mq = this->qs_.get(i);    // get mq for this node
        //printf("recv msg from q=%d\n", i);
        //delete threadId;
        return mq->pop();   // pop the 1st msg from the q
    }


}; //NetworkPseudo
