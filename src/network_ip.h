#pragma once
//LANGUAGE: CwC
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "object.h"
#include "network_ifc.h"
#include "procArgs.h"

int net_debug = 0;
/*********
 * NodeInfo: each node is identified by node id(or index) and socket address
 * *******/
class NodeInfo : public Object {
public:
    size_t id_;
    sockaddr_in address_;
}; // NodeInfo

/***************
 * NetworkIp: Ip based network communication layer.
 * **************/
class NetworkIp : public NetworkIfc {
public:
    NodeInfo* nodes_;   // all nodes
    size_t this_node_;  // node index
    int socket_;        // socket number
    sockaddr_in ip_;    // ip address

    NetworkIp(sockaddr_in addr) : ip_(addr) { }
    ~NetworkIp() { close(socket_); }

    // return node index
    size_t index() override {return this_node_;}

    // create socket
    void init_socket_(size_t port) {
        int opt = 1;

        // create socket
        assert( (this->socket_ = socket(AF_INET, SOCK_STREAM, 0)) >=0 );
        assert( setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == 0 );

        // set sockaddr structure
        ip_.sin_family = AF_INET;
        ip_.sin_addr.s_addr = INADDR_ANY;
        ip_.sin_port = htons(port);

        // bind socket_ with sockaddr structure data
        assert( bind(socket_, (struct sockaddr *)&ip_, sizeof(ip_)) >= 0);

        //maximum of 100 pending connections for the master socket to listen
        assert(listen(socket_, 100) >= 0);
        if (net_debug) printf("========== init_socket() done for port=%d=====\n", (int)port);
    }

    // write a message to other nodes in the directory
    void send_msg(Message* msg) override {
        NodeInfo& tgt = nodes_[msg->target_];

        // get a new socket
        int newSocket = socket(AF_INET, SOCK_STREAM, 0);
        assert(newSocket>=0 && "    send_msg(): Unable to create client socket.");

        // connect new socket to target
        if (connect(newSocket, (sockaddr*)&tgt.address_, sizeof(tgt.address_))<0) 
            printf("    send_msg(): Fatal Error:Unable to connect to target %d\n", (int)msg->target_);
        
        // serialize msg
        Serializer s;
        msg->serialize(s);

        // send the serialized msg to target
        char* buf = s.get_serialized_data();
        size_t size = s.get_size();
        if (net_debug)  printf("    send_msg(): type=%zd size=%d\n", buf[0], (int)size);
        send(newSocket, &size, sizeof(size_t), 0); // send size first
        send(newSocket, buf, size, 0);             // send serialized data now
        close(newSocket);
    }

    //  reads a message from the server socket, deserialize it and return the object
    Message* recv_msg() override {
        sockaddr_in sender;
        socklen_t addrlen = sizeof(sender);
        int req = accept(socket_, (sockaddr*)&sender, &addrlen);

        size_t size = 0;
        int n = read(req, &size, sizeof(size_t));
        // wait to recieve message size
        while (n<=0)
            read(req, &size, sizeof(size_t));
        if (net_debug) printf("    recv_msg(): recieving size... size=%d\n", (int)size);

        // wait to recieve buf
        char* buf = new char[size];
        int rd = 0;
        while (rd != size) rd += read(req, buf+rd, size-rd);;
        buf[rd] = 0;
        if (net_debug) printf("    recv_msg(): recieving buf....size=%d, buf=%s\n", (int)size, buf);

        Deserializer d(buf, size);
        Message* msg = Message::deserialize(d);
        return msg;
    }
    ///////////////////////////////////////////////////
    // initialize server (node 0)
    void server_init(size_t idx, size_t port) {
        this->this_node_ = idx;
        init_socket_(port); // create socket and get ready to listen

        // create numNodes_ nodes, and initilize them
        this->nodes_ = new NodeInfo[args.numNodes_]; // allocate numNodes_ nodes
        for (size_t i=0; i<args.numNodes_; i++ ) nodes_[i].id_ = 0;
        this->nodes_[0].address_ = this->ip_;

        // recieve register messaage from each node, and setup
        if (net_debug) printf("=====server_init(): recving node registration messages\n");
        for (int i=1; i<=args.numNodes_-1; i++) {
            Register* msg = dynamic_cast<Register*>(recv_msg());
            nodes_[msg->sender_].id_ = msg->sender_;
            nodes_[msg->sender_].address_.sin_family = AF_INET;
            nodes_[msg->sender_].address_.sin_addr = msg->client_.sin_addr;
            nodes_[msg->sender_].address_.sin_port = htons(msg->port_);
            if (net_debug) printf("    node%d: sender=%d, port=%d\n", i, (int)msg->sender_, (int)msg->port_);
        }

        // set up directory and send it to all nodes
        if (net_debug) printf("=====server_init(): setup node directory messages\n");
        size_t* ports = new size_t[args.numNodes_-1]; // exclude server itself(node 0)
        size_t* addrs = new size_t[args.numNodes_-1];
        for (size_t i=1; i<args.numNodes_; i++) {
            ports[i] = ntohs(nodes_[i].address_.sin_port);
            addrs[i] = nodes_[i].address_.sin_addr.s_addr;
            if (net_debug) printf("    addr=%d, port=%d\n",(int)addrs[i], (int)ports[i]);
        }

        Directory dir(ports, addrs);
        for (int i=1; i<args.numNodes_; i++) {
            dir.target_ = i;
            if (net_debug) printf("    sedning directory to node %d...\n", i);
            send_msg(&dir);
        }
        if (net_debug) printf("==========server_init() ends ==========\n");
    } // server_init()

    //////////////////////////////////////////////////////////
    // initialize client node (none 0 node)
    void client_init(size_t idx, size_t port, char* server_addr, size_t server_port) {
        // set up this node id, and create socket
        this->this_node_ = idx;
        init_socket_(port);

        // setup server node info for send_msg() connect to server
        this->nodes_ = new NodeInfo[1];
        nodes_[0].id_ = 0;
        nodes_[0].address_.sin_family = AF_INET;
        nodes_[0].address_.sin_port = htons(server_port);
        if (inet_pton(AF_INET, server_addr, &nodes_[0].address_.sin_addr) <= 0) 
            assert(false && "Invalid server IP address format.");
        
        // send register message to server(node 0)
        Register msg(idx, port);
        if (net_debug) printf("===========sending register msg for idx=%d, port=%d...\n",(int)msg.sender_, (int)msg.port_);
        send_msg(&msg);

        //receive Directory message from server
        Directory* dir = dynamic_cast<Directory*>(recv_msg());
        if (net_debug) dir->printDir();

        // load dir to nodes_[]
        NodeInfo* nodes = new NodeInfo[args.numNodes_]; // create a temperary nodes array
        nodes[0] = nodes_[0];
        for (size_t i=0; i<dir->clients_; i++) {
            nodes[i+1].id_ = i + 1;
            nodes[i+1].address_.sin_family = AF_INET;
            nodes[i+1].address_.sin_port = htons(dir->ports_[i]);
            nodes[i+1].address_.sin_addr.s_addr = dir->addr_[i];
            //if (inet_pton(AF_INET, dir->addresses_[i]->c_str(), &nodes[i+1].address_.sin_addr)<=0)
            //    printf("Invalide IP directory-address for node %d\n", i+1);
        }

        delete [] nodes_;
        nodes_ = nodes;     // now let this->nodes_ points to nodes.
        delete dir; 
        if (net_debug) printf("==========client_init() end ===========\n");
    }

};
