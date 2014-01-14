TOPDIR= ..

include ${TOPDIR}/Makefile.conf

INCLUDE_DIRS = -I/usr/include/libxml2/ ${INCLUDES}
CFLAGS2 = ${CFLAGS} -lz -shared
OBJECTS=mergerfilter.o ../anthillutil.o ../blocking.o  ../cache.o  ../classifier.o  ../comparator.o  ../datasource.o  ../project.o  ../stringutil.o ../encode.o

target: ${OBJECTS} mergerfilter.so labelmerger.so

./%.o: ./%.cc
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o "$@" "$<"

mergerfilter.so: mergerfilter.cc mergerfilter.h
	${CXX} ${OBJECTS} ${INCLUDE_DIRS} -o mergerfilter.so ${CFLAGS2}

labelmerger.so: labelmerger.cc
	${CXX} ${INCLUDE_DIRS} labelmerger.cc -o labelmerger.so ${CFLAGS2}

clean:
	rm -f *.o *.so
