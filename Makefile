SOURCE=\
	endian_utils.cc \
	metadata_disk_structures.cc

#	metadata.cc \

TEST_SOURCE=\
	unit-tests/block_t.cc \
	unit-tests/btree_t.cc \
	unit-tests/endian_t.cc \
	unit-tests/space_map_t.cc \
	unit-tests/space_map_disk_t.cc \
	unit-tests/transaction_manager_t.cc \

#	unit-tests/metadata_t.cc \

OBJECTS=$(subst .cc,.o,$(SOURCE))
TEST_PROGRAMS=$(subst .cc,,$(TEST_SOURCE))
TOP_DIR:=$(PWD)
CPPFLAGS=-Wall -std=c++0x -g -I$(TOP_DIR)
LIBS=-lstdc++

.PHONEY: unit-tests test-programs

test-programs: $(TEST_PROGRAMS)

unit-test: $(TEST_PROGRAMS)
	for p in $(TEST_PROGRAMS); do echo Running $$p; ./$$p; done

.SUFFIXES: .cc .o .d

%.d: %.cc
	g++ -MM $(CPPFLAGS) $< > $@.$$$$;                  \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

multisnap_display: $(OBJECTS) main.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/block_t: unit-tests/block_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/btree_t: unit-tests/btree_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/space_map_t: unit-tests/space_map_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/space_map_disk_t: unit-tests/space_map_disk_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/transaction_manager_t: unit-tests/transaction_manager_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/metadata_t: unit-tests/metadata_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

unit-tests/endian_t: unit-tests/endian_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

include $(subst .cc,.d,$(SOURCE))
include $(subst .cc,.d,$(TEST_SOURCE))