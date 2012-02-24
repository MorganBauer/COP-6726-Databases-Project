CC = g++ -O0 -Wno-deprecated -Wall -Wextra -Wshadow -Wno-write-strings -Weffc++ -pedantic-errors -ggdb3 -fopenmp
#  -march=native
#-D_GLIBCXX_PARALLEL
# wtf is this below? tag is set to '-i', but if on linux it is '-n'? INVESTIGATE TODO
tag = -i

ifdef linux
tag = -n
endif

again: clean all

all: a1test a2-1test

a2-2test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o
	$(CC) -o a2-2test Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-2test.o -lfl -lpthread

a2-2test.o: a2-2test.cc
	$(CC)  -c a2-2test.cc

a2-1test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-1test.o
	$(CC) -o a2-1test Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o a2-1test.o -lfl -lpthread

a2-11test.o: a2-1test.cc
	$(CC)  -c a2-1test.cc

a1test: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o a1test.o
	$(CC) -o a1test Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o a1test.o -lfl

a1test.o: a1test.cc
	$(CC)  -c a1test.cc

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl

main.o: main.cc
	$(CC)  -c main.cc

Comparison.o: Comparison.cc
	$(CC)  -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC)  -c ComparisonEngine.cc

Pipe.o: Pipe.cc Pipe.h
	$(CC)  -c Pipe.cc

BigQ.o: BigQ.cc BigQ.h
	$(CC)  -c BigQ.cc

DBFile.o: DBFile.cc DBFile.h
	$(CC)  -c DBFile.cc

File.o: File.cc File.h
	$(CC)  -c File.cc

Record.o: Record.cc
	$(CC)  -c Record.cc

Schema.o: Schema.cc
	$(CC)  -c Schema.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	rm -f a1test
	rm -f a2-1test
	rm -f a2-2test
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
