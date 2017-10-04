crc32c.so: crc32c.o
	gcc -shared -o $@ $< -laio

crc32c.o: crc32c.c
	gcc -std=gnu11 -fpic -I. -Wall -c -o $@ $<

.PHONEY: clean
clean:
	rm -f crc32c.so crc32c.o


