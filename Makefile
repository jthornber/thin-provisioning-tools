SOURCE=\
	metadata.cc

PROGRAM_SOURCE=\
	block_t.cc

OBJECTS=$(subst .cc,.o,$(SOURCE))
CPPFLAGS=-Wall -std=c++0x
INCLUDES=
LIBS=-lstdc++

.SUFFIXES: .cc .o .d

%.d: %.cc
	$(CC) -MM $(CPPFLAGS) $< > $@.$$$$;                  \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

multisnap_display: $(OBJECTS) main.o
	g++ -o $@ $+ $(LIBS)

block_t: block_t.o
	g++ -o $@ $+ $(LIBS)

include $(subst .cc,.d,$(SOURCE))
include $(subst .cc,.d,$(PROGRAM_SOURCE))