
CC=g++
CFLAGS=-I./src
HEADERFILES = application.h array.h dataframe.h helper.h keyvalue.h map.h message.h network_pseudo.h network_ifc.h object.h procArgs.h serializer.h sorer.h string.h 
CPPFILES = wordCount.cpp
OBJ = 

#build: $(CPPFILE) $(HEADERFILES)
build: $(CPPFILE)
	$(CC) -o eau2 src/wordCount.cpp -std=c++11

test: 
	@./eau2 -file input.txt -node 3

valgrind:
	valgrind --tool=memcheck --leak-check=yes --show-reachable=yes --num-callers=20 --track-fds=yes ./eau2 -node 3

clean:
	rm -f a.out
	rm -f *.o
	rm -f *.exe
	rm -f *~
	rm eau2
	#rm genTest
	

