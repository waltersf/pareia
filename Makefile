TOPDIR= .

include ${TOPDIR}/Makefile.conf


SUBDIRS = blocking merger reader result tools sqlite
all: main ${SUBDIRS}



# phony rules
.PHONY: ${SUBDIRS} clean

${SUBDIRS}:
	make -C $@

C_SRCS += anthillutil.cc basefilter.cc main.cc project.cc datasource.cc classifier.cc comparator.cc stringutil.cc cache.cc blocking.cc encode.cc buscabr.cc
OBJS += anthillutil.o basefilter.o main.o project.o datasource.o classifier.o comparator.o stringutil.o cache.o blocking.o encode.o buscabr.o
LIBS_GEN += acklabel.so

./%.o: ./%.cc
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -c -o "$@" "$<"

./%.so: ./%.cc
	@echo 'Building file: $<, using flags $(CFLAGS)'
	$(CXX) $(CFLAGS) $(INCLUDES) -shared -o "$@" "$<"

main: $(OBJS) $(LIBS_GEN)
	@echo 'Building target: $@'
	@echo 'Invoking: $(CC) Linker'
	$(CXX) $(CFLAGS) $(INCLUDES) -o "pareia" $(OBJS) $(CLIBS)
	@echo 'Finished building target: $@'
	@echo ' '
clean-filters:
	make -C reader clean
	make -C merger clean
	make -C blocking clean
	make -C result clean
	make -C tools clean
clean:
	\rm -f *.o *.so pareia
	make -C reader clean
	make -C merger clean
	make -C blocking clean
	make -C result clean
	make -C tools clean
	make -C sqlite clean

r:
	make -C reader clean all

cleanComp: clean all
