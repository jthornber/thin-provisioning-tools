SOURCE=\
	endian.cc \
	metadata.cc \
	metadata_disk_structures.cc \
	space_map_disk.cc \

TEST_SOURCE=\
	block_t.cc \
	btree_t.cc \
	endian_t.cc \
	metadata_t.cc \
	space_map_t.cc \
	space_map_disk_t.cc \
	transaction_manager_t.cc \

OBJECTS=$(subst .cc,.o,$(SOURCE))
TEST_PROGRAMS=$(subst .cc,,$(TEST_SOURCE))
CPPFLAGS=-Wall -std=c++0x -g
INCLUDES=
LIBS=-lstdc++

.PHONEY: unit-tests test-programs

test-programs: $(TEST_PROGRAMS)

unit-tests: $(TEST_PROGRAMS)
	for p in $(TEST_PROGRAMS); do echo Running $$p; ./$$p; done

.SUFFIXES: .cc .o .d

%.d: %.cc
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$;                  \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

multisnap_display: $(OBJECTS) main.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

block_t: block_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

btree_t: btree_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

space_map_t: space_map_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

space_map_disk_t: space_map_disk_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

transaction_manager_t: transaction_manager_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

metadata_t: metadata_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

endian_t: endian_t.o $(OBJECTS)
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

include $(subst .cc,.d,$(SOURCE))
include $(subst .cc,.d,$(TEST_SOURCE))