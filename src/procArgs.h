//lang::CwC
#pragma once
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// process command line arguments
class ProcArgs : public Object {
public:
    char* file_ = nullptr;          // input file name
    size_t numNodes_ = 999;           // number of nodes
    size_t rowsPerChunk_ = 0;       //number of rows in a chunk
    size_t nodeId_ = 0;              // ID of this node
    size_t  myPort_ = 0;             // my port number
    char* masterIp_ = nullptr;       // master server's ip address
    size_t masterPort_ = 0;          // master server's port number

    // constructor
    ProcArgs() {} 

    // destructor : clean up memory
    ~ProcArgs() {
        if (this->file_) delete [] this->file_;
        if (this->masterIp_) delete [] this->masterIp_;
    }

    void printUsage(char* procName)
    {
        printf("Usage: %s -file <file name>\n", procName);
        printf("            -node <number of nodes>\n");
        printf("            -index <node ID>\n");
        printf("            -myport <myport>\n");
        printf("            -masterip <master ip>\n");
        printf("            -masterport <master port>\n");
        printf("            -rowsperchunk <rows per chunk>\n");
        exit(1);
    }

    void checkClientArgs() {
        if (masterIp_ == nullptr)
        {
            printf("[-masterip] must be given.\n");
            exit(1);
        }
        if (masterPort_ == 0)
        {
            printf("[-masterport] must be given.\n");
            exit(1);
        }
    }

    void checkArgs()
    {
      /*  if (!file_)
        {
            printf("[-file] must be given.\n");
            exit(1);
        }
        */
        if (numNodes_ == 0)
        {
            printf("[-node] must be given.\n");
            exit(1);
        }

        if (myPort_ == 0)
        {
            printf("[-myport] must be given.\n");
            exit(1);
        }
        if (nodeId_ == -1)
        {
            printf("[-index] must be given.\n");
            exit(1);
        }
    }
    // parsing the command line
    void parser(int argc, char** argv)
    {
        if (argc == 1) printUsage(argv[0]);

        for (int i=1; i<argc; i++) {
            if (strcmp(argv[i], "-file") == 0) {
                i++;
                this->file_ = new char[strlen(argv[i])];
                strcpy(this->file_, argv[i]);
            } else if (strcmp(argv[i], "-node") == 0) {
                i++;
                this->numNodes_ = atoi(argv[i]);
            } else if ( strcmp(argv[i], "-index") == 0) {
                i++;
                this->nodeId_ = atoi(argv[i]);
            } else if ( strcmp(argv[i], "-myport") == 0 ) {
                i++;
                this->myPort_ = atoi(argv[i]);
            } else if ( strcmp(argv[i], "-masterip") == 0 ) {
                i++;
                this->masterIp_ = new char[strlen(argv[i])];
                strcpy(this->masterIp_, argv[i]);
            }   else if ( strcmp(argv[i], "-masterport") == 0 ) {
                i++;
                this->masterPort_ = atoi(argv[i]);
            }  else if ( strcmp(argv[i], "-rowsperchunk") == 0 ) {
                i++;
                this->rowsPerChunk_ = atoi(argv[i]);
            } else 
                printUsage(argv[0]);
        }
    } 
};

extern ProcArgs args;
//extern int debug;
