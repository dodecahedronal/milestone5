
CC=g++
CFLAGS=-I./src
HEADERFILES = application.h array.h dataframe.h helper.h keyvalue.h map.h message.h network_ip.h network_ifc.h object.h procArgs.h serializer.h sorer.h string.h 
CPPFILES = linus.cpp
OBJ = 

node = 3
masterip = 192.168.0.109
masterport = 8080

build: $(CPPFILE)
	$(CC) -pthread -o eau2 src/linus.cpp -std=c++11

how:
	@echo "server: ./eau2 -node $(node) -myport $(masterport) -index 0"
	@echo "client: ./eau2 -node $(node) -myport 11111 -masterip $(masterip) -masterport $(masterport) -index 1"

test:
	./eau2 -node $(node) -myport $(masterport) -index 0&
	./eau2 -node $(node) -myport 11111 -masterip $(masterip) -masterport $(masterport) -index 1&
	./eau2 -node $(node) -myport 21111 -masterip $(masterip) -masterport $(masterport) -index 2&


valgrind:
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes ./eau2 -node $(node) -myport $(masterport) -index 0

clean:
	rm -f a.out
	rm -f *.o
	rm -f *.exe
	rm -f *~
	rm eau2
	#rm genTest
	

