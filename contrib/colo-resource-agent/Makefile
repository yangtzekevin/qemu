CC=gcc

CFLAGS=-g -O2 -Wall -Wextra -fsanitize=address `pkg-config --cflags glib-2.0 json-glib-1.0`
LDFLAGS=-lcorosync_common -lcpg `pkg-config --libs glib-2.0 json-glib-1.0`

%.o: %.c *.h
	$(CC) -c -o $@ $< $(CFLAGS)

colod: util.o qemu_util.o json_util.o coutil.c qmp.o client.o daemon.o
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

.PHONY: clean

clean:
	rm -f *.o colod
