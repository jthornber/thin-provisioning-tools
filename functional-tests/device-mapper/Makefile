dm-ioctl.so: dm-ioctl.o
	gcc -shared -o $@ $< -laio

dm-ioctl.o: dm-ioctl.c
	gcc -std=gnu11 -fpic -I. -Wall -c -o $@ $<

.PHONEY: clean
clean:
	rm -f dm-ioctl.so dm-ioctl.o


