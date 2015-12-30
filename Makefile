all:
	gcc -Wall main.c -D_FILE_OFFSET_BITS=64 -o main -lfuse
