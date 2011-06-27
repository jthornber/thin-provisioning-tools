SOURCE=\
	main.cc \
	metadata.cc

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

multisnap_display: $(OBJECTS)
	g++ -o $@ $+ $(LIBS)

include $(subst .cc,.d,$(SOURCE))