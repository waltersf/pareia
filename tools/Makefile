TOPDIR= ..
include ${TOPDIR}/Makefile.conf


INCLUDE_DIRS = -I/usr/include/libxml2/ -I../reader ${INCLUDES}
OBJECTS= transitivity.o
CLIBS =
CFLAGS = -g -Wno-deprecated -Wall -O2
EXECS = extract2eval transitivity catfiledatasource statistics setoperation getsample projectinfo
# phony rules
.PHONY: ${SUBDIRS} clean

./%.o: ./%.cc
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o "$@" "$<"

target: main

extract2eval: extract2eval.cc
	$(CXX) $(CFLAGS) $(INCLUDES) extract2eval.cc ../project.o ../datasource.o \
		../classifier.o ../blocking.o ../stringutil.o ../comparator.o ../encode.o ../buscabr.o -lz -lexpat -o "extract2eval"

transitivity: transitivity.cc
	$(CXX) $(CFLAGS) $(INCLUDES) transitivity.cc -lz -o "transitivity"

catfiledatasource: catfiledatasource.cc ../datasource.cc
	$(CXX) $(CFLAGS) $(INCLUDES) catfiledatasource.cc ../project.o ../datasource.o \
		../classifier.o ../blocking.o ../stringutil.o ../comparator.o ../encode.o ../buscabr.o -lexpat -o "catfiledatasource"

getsample: getsample.cc
	$(CXX) $(CFLAGS) $(INCLUDES) getsample.cc ../project.o ../datasource.o \
		../classifier.o ../blocking.o ../stringutil.o ../comparator.o ../encode.o ../buscabr.o -lexpat -o "getsample"

statistics: statistics.cc
	$(CXX) $(CFLAGS) $(INCLUDES) statistics.cc ../project.o ../datasource.o \
		../classifier.o ../blocking.o ../stringutil.o ../comparator.o ../encode.o ../buscabr.o -lexpat -o "statistics"

setoperation: setoperation.cc
	$(CXX) $(CFLAGS) $(INCLUDES) setoperation.cc -lz -o "setoperation"

projectinfo: projectinfo.cc
	$(CXX) $(CFLAGS) $(INCLUDES) projectinfo.cc ../project.o ../datasource.o \
		../classifier.o ../blocking.o ../stringutil.o ../comparator.o ../encode.o ../buscabr.o -lexpat -o "projectinfo"
main: $(EXECS)

clean:
	rm -f $(EXECS)

