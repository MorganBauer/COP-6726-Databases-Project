
#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

int main () {



	yyparse();

// these data structures hold the result of the parsing
struct FuncOperator *finalFunction;
struct TableList *tables;
struct AndList *boolean;
struct NameList *groupingAtts;
struct NameList *attsToSelect;
int distinctAtts;
int distinctFunc;
}


