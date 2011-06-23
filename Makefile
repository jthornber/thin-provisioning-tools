SOURCE=\
	block.cc \
	main.cc \
	metadata.cc \
	transaction_manager.cc

OBJECTS=$(subst .cc,.o,$(SOURCE))
CPPFLAGS=-Wall -std=c++0x
INCLUDES=
LIBS=-lstdc++


.SUFFIXES: .cc .o

.cc.o:
	g++ -c $(CPPFLAGS) $(INCLUDES) -o $@ $<

multisnap_display: $(OBJECTS)
	g++ -o $@ $+ $(LIBS)

main.o: block.h
block.o: block.h
transaction_manager.o: transaction_manager.h block.h
metadata.o: block.h transaction_manager.h btree.h metadata.h