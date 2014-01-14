TOPDIR= ..

include ${TOPDIR}/Makefile.conf


INCLUDE_DIRS = -I/usr/include/libxml2/ -I../reader ${INCLUDES}
CFLAGS2 = ${CFLAGS} -shared

OBJECTS=blockingfilter.o ../anthillutil.o ../sqlite/sdsqlite.o ../blocking.o  ../cache.o  ../classifier.o  ../comparator.o  ../datasource.o  ../project.o  ../stringutil.o ../encode.o

./%.o: ./%.cc
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o "$@" "$<"

target: ${OBJECTS} blockingfilter.so labelblocking.so #bigram.so

blockingfilter.so: blockingfilter.cc blockingfilter.h ../sqlite/sdsqlite.cc
	${CXX} ${CFLAGS2} ${OBJECTS} ${INCLUDE_DIRS} -o blockingfilter.so -lsqlite3

#bigram.so: bigram.cc bigram.h
#	${CXX} ${CFLAGS2} ${OBJECTS} ${INCLUDE_DIRS} -o bigram.so bigram.cc

labelblocking.so: labelblocking.cc
	${CXX} ${CFLAGS2} ${INCLUDE_DIRS} -o labelblocking.so labelblocking.cc

clean:
	rm -f *.o *.so
