TOPDIR= ..

include ${TOPDIR}/Makefile.conf


INCLUDE_DIRS = -I/usr/include/libxml2/ -I../reader ${INCLUDES}
CFLAGS2 = ${CFLAGS} -shared -lz
OBJECTS=

target: ${OBJECTS} ../anthillutil.o resultfilter.so labelresult.so

resultfilter.so: resultfilter.cc resultfilter.h
	${CXX} ${CLIBS} ${OBJECTS} ${INCLUDE_DIRS} -o resultfilter.so resultfilter.cc ../project.o ../datasource.o ../blocking.o ../stringutil.o ../encode.o ../comparator.o ../classifier.o ../basefilter.o ../anthillutil.o ${CFLAGS2}

labelresult.so: labelresult.cc
	${CXX} ${CLIBS} ${OBJECTS} ${INCLUDE_DIRS} -o labelresult.so labelresult.cc ${CFLAGS2}

clean:
	rm -f *.o *.so
