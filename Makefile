CC=gcc
SRC=main.c
OUTPUT=lazydb

all:$(SRC)
	$(CC) $(SRC) -o $(OUTPUT)

clean: 
	rm $(OUTPUT)
