TOPDIR= ..

include ${TOPDIR}/Makefile.conf


INCLUDE_DIRS = -I/usr/include/libxml2/ ${INCLUDES}
OBJECTS= sdsqlite.o
SOURCES= sdsqlite.cc
CFLAGS += -fexceptions
CLIBS:=  -lsqlite3 -lgtest_main

target: main 

./%.o: ./%.cc 
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o "$@" "$<"

main: ${OBJECTS}
	 
all: main

clean:
	rm -f *.o 
