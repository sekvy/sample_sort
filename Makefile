CC        = gcc
CFLAGS    = -std=c99 -pipe -Wall -W -pedantic-errors -march=native
CFLAGS   += -Wmissing-braces -Wparentheses
OBJS      = sort


all: sort

clean:
	$(RM) $(OBJS)

sort:
	$(CC) $(CFLAGS) -O2 sort.c -lpthread -o sort

dsort:
	$(CC) $(CFLAGS) -g -DDEBUG sort.c -lpthread -o sort

