bcache.so: bcache.o
	gcc -shared -o $@ $< -laio

bcache.o: bcache.c
	gcc -std=gnu11 -fpic -I. -Wall -c -o $@ $<

.PHONEY: clean
clean:
	rm -f bcache.so bcache.o


