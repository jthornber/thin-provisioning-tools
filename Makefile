SOURCE=\
	metadata.cc

PROGRAM_SOURCE=\
	block_t.cc \
	btree_t.cc \
	space_map_t.cc \
	transaction_manager_t.cc

OBJECTS=$(subst .cc,.o,$(SOURCE))
CPPFLAGS=-Wall -std=c++0x -g
INCLUDES=
LIBS=-lstdc++

.PHONEY: unit-tests

unit-tests: block_t btree_t space_map_t transaction_manager_t
	./block_t
	./btree_t
	./space_map_t
	./transaction_manager_t

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

transaction_manager_t: transaction_manager_t.o
	g++ $(CPPFLAGS) -o $@ $+ $(LIBS)

include $(subst .cc,.d,$(SOURCE))
include $(subst .cc,.d,$(PROGRAM_SOURCE))