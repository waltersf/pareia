TOPDIR=..
include ${TOPDIR}/Makefile.conf

all: target
OBJECTS= ../anthillutil.o ../blocking.o  ../cache.o  ../classifier.o  ../comparator.o  ../datasource.o  ../project.o  ../stringutil.o ../encode.o ../buscabr.o
target: readerfilter.so labelreader.so labelfinishedjob.so labelcomparepair.so

%.o: %.cc
	${CXX} ${INCLUDES} -c -o "$@" $< $(CFLAGS)

readerfilter.so: readerfilter.cc readerfilter.h ${OBJECTS}
	${CXX} -o readerfilter.so -shared ${CFLAGS} ${INCLUDES} ${OBJECTS} readerfilter.cc -lz ${CLIBS}
#../general/dmetaphone.o ../general/stringutil.o ../general/encode.o ../general/aprutil.o ../general/bitvector.o
labelreader.so: labelreader.cc
	${CXX} ${CFLAGS} ${INCLUDES} labelreader.cc -o labelreader.so -shared

#labelreader1.so: labelreader1.cc
#	${CXX} ${CFLAGS} ${INCLUDES} labelreader1.cc -o labelreader1.so -shared

labelcomparepair.so: labelcomparepair.cc
	${CXX} ${CFLAGS} ${INCLUDES} labelcomparepair.cc -o labelcomparepair.so -shared

labelfinishedjob.so: labelfinishedjob.cc
	${CXX} ${CFLAGS} ${INCLUDES} labelfinishedjob.cc -o labelfinishedjob.so -shared

labelreader2.so: labelreader2.cc
	${CXX} ${CFLAGS} ${INCLUDES} labelreader2.cc -o labelreader2.so -shared
clean:
	rm -f *.o *.so
