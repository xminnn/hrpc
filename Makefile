test: *.c hrpc/*.c hrpc/*.h
	gcc -O3 *.c hrpc/*.c -I hrpc -o test -lm -lpthread