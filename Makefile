.PHONEY: all

all: thin_repair thin_dump thin_restore

SOURCE=\
	checksum.cc \
	endian_utils.cc \
	error_set.cc \
	hex_dump.cc \
	human_readable_format.cc \
	metadata.cc \
	metadata_checker.cc \
	metadata_dumper.cc \
	metadata_disk_structures.cc \
	restore_emitter.cc \
	space_map_disk.cc \
	thin_pool.cc \
	transaction_manager.cc \
	xml_format.cc

PROGRAM_SOURCE=\
	thin_dump.cc \
	thin_repair.cc \
	thin_restore.cc

OBJECTS=$(subst .cc,.o,$(SOURCE))
TOP_DIR:=$(PWD)
CPPFLAGS=-Wall -g -I$(TOP_DIR)
#CPPFLAGS=-Wall -std=c++0x -g -I$(TOP_DIR)
LIBS=-lstdc++ -lboost_program_options -lexpat

.PHONEY: test-programs

test-programs: $(TEST_PROGRAMS)

.SUFFIXES: .cc .o .d

%.d: %.cc
	g++ -MM -MT $(subst .cc,.o,$<) $(CPPFLAGS) $< > $@.$$$$;                  \
	sed 's,\([^ :]*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

thin_dump: $(OBJECTS) thin_dump.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

thin_restore: $(OBJECTS) thin_restore.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

thin_repair: $(OBJECTS) thin_repair.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

include unit-tests/Makefile.in
include $(subst .cc,.d,$(SOURCE))
include $(subst .cc,.d,$(TEST_SOURCE))
include $(subst .cc,.d,$(PROGRAM_SOURCE))
