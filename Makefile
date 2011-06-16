SOURCE=\
	main.cc

OBJECTS=$(subst .cc,.o,$(SOURCE))
CPPFLAGS=-Wall -Weffc++ -std=c++0x
INCLUDES=
LIBS=-lstdc++


.SUFFIXES: .cc .o

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

multisnap_display: $(OBJECTS)
	g++ -o $@ $+ $(LIBS)