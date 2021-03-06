
%{
#include "ParseTree.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <algorithm>
  using namespace std;

  extern "C" int yylex();
  extern "C" int yyparse();
  extern "C" void yyerror(char *s);

  // these data structures hold the result of the parsing
  struct FuncOperator *finalFunction = 0; // the aggregate function (NULL if no agg)
  struct TableList *tables = 0; // the list of tables and aliases in the query
  struct AndList *boolean = 0; // the predicate in the WHERE clause
  struct NameList *groupingAtts = 0; // grouping atts (NULL if no grouping)
  struct NameList *attsToSelect = 0; // the set of attributes in the SELECT (NULL if no such atts)
  int distinctAtts = 0; // 1 if there is a DISTINCT in a non-aggregate query
  int distinctFunc = 0; // 1 if there is a DISTINCT in an aggregate query
  int query = 0;
  // maintenance commands
  // CREATE
  int createTable = 0; // 1 if the SQL is create table
  int tableType = 0; // 1 for heap, 2 for sorted.
  char * attrName;
  // int attrType;
  vector<myAttribute> attributes;
  // INSERT
  int insertTable = 0; // 1 if the command is Insert into table
  int dropTable = 0; // 1 is the command is Drop table
  // SET command
  int outputChange = 0;
  int planOnly = 0; // 1 if we are changing settings to planning only. Do not execute.
  int setStdOut = 0;

  bool keepGoing = true;
  // shared variables, variables shared between more than one parsing.
  string tableName;
  string fileName; // this is used for both input and output file names. A copy is made inside the database.
  %}

// this stores all of the types returned by production rules
%union {
  struct FuncOperand *myOperand;
  struct FuncOperator *myOperator;
  struct TableList *myTables;
  struct ComparisonOp *myComparison;
  struct Operand *myBoolOperand;
  struct OrList *myOrList;
  struct AndList *myAndList;
  struct NameList *myNames;
  struct AttrList *myAttrs;
  char *actualChars;
  char whichOne;
  int attrType;
}

%token <actualChars> FilePath
%token <actualChars> Name
%token <actualChars> Attribute
%token <actualChars> Float
%token <actualChars> Int
%token <actualChars> String
%token CREATE
%token DROP
%token TABLE
%token SELECT
%token GROUP
%token DISTINCT
%token BY
%token FROM
%token WHERE
%token SUM
%token AS
%token AND
%token OR
%token <attrType> INTEGER_ATTR FLOAT_ATTR STRING_ATTR
%token HEAP SORTED
%token ON
%token SET
%token INSERT INTO
%token OUTPUT NONE STDOUT
%token UPDATE STATISTICS
%token SHUTDOWN

%type <myOrList> OrList
%type <myAndList> AndList
%type <myOperand> SimpleExp
%type <myOperator> CompoundExp
%type <whichOne> Op
%type <myComparison> BoolComp
%type <myComparison> Condition
%type <myTables> Tables
%type <myBoolOperand> Literal
%type <myNames> Atts
%type <myAttrList> AttrList
%type <attrType> AttrType

%start SQL


 //******************************************************************************
 // SECTION 3
 //******************************************************************************
 /* This is the PRODUCTION RULES section which defines how to "understand" the
  * input language and what action to take for each "statment"
  */

%%

SQL: SELECT WhatIWant FROM Tables WHERE AndList
{
  query = 1;
  tables = $4;
  boolean = $6;
  groupingAtts = NULL;
  }

| SELECT WhatIWant FROM Tables WHERE AndList GROUP BY Atts
{
  query = 1;
  tables = $4;
  boolean = $6;
  groupingAtts = $9;
  }
| CREATE TABLE Name '(' AttrList ')' AS DBFileType
{
  tableName = $3;
  createTable = 1;
  reverse(attributes.begin(), attributes.end());
}
| DROP TABLE Name
{
  dropTable = 1;
}
| SET OUTPUT OutSetting
{
  outputChange = 1;
}
| INSERT FilePath INTO Name
{
  insertTable = 1;
  fileName = $2;
}
| SHUTDOWN
{
  query = -1;
  keepGoing = false;
};

DBFileType : HEAP
{
  tableType = 1;
}
| SORTED ON Atts
{
  tableType = 2;
};

OutSetting: STDOUT
{
  setStdOut = 1;
  fileName = "";
}
| NONE
{
  planOnly = 1;
  fileName = "";
}
| FilePath
{
  string fn($1);
  fileName = $1;
  // printf("found ### %s ###\n", $1);
  // printf("%p\n", $1);
};

AttrList: Name AttrType ',' AttrList // non terminal type, more than one attribute
{
  myAttribute attr;
  attr.name = $1;
  attr.myType = $2;
  attributes.push_back(attr);
}
| Name AttrType // final attribute in list
{
  // const static int IntType = 0, DoubleType = 1, StringType = 2;
  myAttribute attr;
  attr.name = $1;
  attr.myType = $2;
  attributes.push_back(attr);
};

AttrType : INTEGER_ATTR
{
  $$ = 0;
}
| FLOAT_ATTR
{
  $$ = 1;
}
| STRING_ATTR
{
  $$ = 2;
};

WhatIWant: Function ',' Atts
{
  attsToSelect = $3;
  distinctAtts = 0;
}

| Function
{
  attsToSelect = NULL;
}

| Atts
{
  distinctAtts = 0;
  finalFunction = NULL;
  attsToSelect = $1;
}

| DISTINCT Atts
{
  distinctAtts = 1;
  finalFunction = NULL;
  attsToSelect = $2;
  finalFunction = NULL;
};

Function: SUM '(' CompoundExp ')'
{
  distinctFunc = 0;
  finalFunction = $3;
}

| SUM DISTINCT '(' CompoundExp ')'
{
  distinctFunc = 1;
  finalFunction = $4;
};

Atts: Name
{
  $$ = (struct NameList *) malloc (sizeof (struct NameList));
  $$->name = $1;
  $$->next = NULL;
}

| Atts ',' Name
{
  $$ = (struct NameList *) malloc (sizeof (struct NameList));
  $$->name = $3;
  $$->next = $1;
}

Tables: Name AS Name
{
  $$ = (struct TableList *) malloc (sizeof (struct TableList));
  $$->tableName = $1;
  $$->aliasAs = $3;
  $$->next = NULL;
}

| Tables ',' Name AS Name
{
  $$ = (struct TableList *) malloc (sizeof (struct TableList));
  $$->tableName = $3;
  $$->aliasAs = $5;
  $$->next = $1;
}

CompoundExp: SimpleExp Op CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator->leftOperator = NULL;
  $$->leftOperator->leftOperand = $1;
  $$->leftOperator->right = NULL;
  $$->leftOperand = NULL;
  $$->right = $3;
  $$->code = $2;

}

| '(' CompoundExp ')' Op CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator = $2;
  $$->leftOperand = NULL;
  $$->right = $5;
  $$->code = $4;

}

| '(' CompoundExp ')'
{
  $$ = $2;

}

| SimpleExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator = NULL;
  $$->leftOperand = $1;
  $$->right = NULL;

}

| '-' CompoundExp
{
  $$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
  $$->leftOperator = $2;
  $$->leftOperand = NULL;
  $$->right = NULL;
  $$->code = '-';

}
;

Op: '-'
{
  $$ = '-';
}

| '+'
{
  $$ = '+';
}

| '*'
{
  $$ = '*';
}

| '/'
{
  $$ = '/';
}
;

AndList: '(' OrList ')' AND AndList
{
  // here we need to pre-pend the OrList to the AndList
  // first we allocate space for this node
  $$ = (struct AndList *) malloc (sizeof (struct AndList));

  // hang the OrList off of the left
  $$->left = $2;

  // hang the AndList off of the right
  $$->rightAnd = $5;

}

| '(' OrList ')'
{
  // just return the OrList!
  $$ = (struct AndList *) malloc (sizeof (struct AndList));
  $$->left = $2;
  $$->rightAnd = NULL;
}
;

OrList: Condition OR OrList
{
  // here we have to hang the condition off the left of the OrList
  $$ = (struct OrList *) malloc (sizeof (struct OrList));
  $$->left = $1;
  $$->rightOr = $3;
}

| Condition
{
  // nothing to hang off of the right
  $$ = (struct OrList *) malloc (sizeof (struct OrList));
  $$->left = $1;
  $$->rightOr = NULL;
}
;

Condition: Literal BoolComp Literal
{
  // in this case we have a simple literal/variable comparison
  $$ = $2;
  $$->left = $1;
  $$->right = $3;
}
;

BoolComp: '<'
{
  // construct and send up the comparison
  $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
  $$->code = LESS_THAN;
}

| '>'
{
  // construct and send up the comparison
  $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
  $$->code = GREATER_THAN;
}

| '='
{
  // construct and send up the comparison
  $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
  $$->code = EQUALS;
}
;

Literal : String
{
  // construct and send up the operand containing the string
  $$ = (struct Operand *) malloc (sizeof (struct Operand));
  $$->code = STRING;
  $$->value = $1;
}

| Float
{
  // construct and send up the operand containing the FP number
  $$ = (struct Operand *) malloc (sizeof (struct Operand));
  $$->code = DOUBLE;
  $$->value = $1;
}

| Int
{
  // construct and send up the operand containing the integer
  $$ = (struct Operand *) malloc (sizeof (struct Operand));
  $$->code = INT;
  $$->value = $1;
}

| Name
{
  // construct and send up the operand containing the name
  $$ = (struct Operand *) malloc (sizeof (struct Operand));
  $$->code = NAME;
  $$->value = $1;
}
;


SimpleExp:

Float
{
  // construct and send up the operand containing the FP number
  $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
  $$->code = DOUBLE;
  $$->value = $1;
}

| Int
{
  // construct and send up the operand containing the integer
  $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
  $$->code = INT;
  $$->value = $1;
}

| Name
{
  // construct and send up the operand containing the name
  $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
  $$->code = NAME;
  $$->value = $1;
}
;

%%

