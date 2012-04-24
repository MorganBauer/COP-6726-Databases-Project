
#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

void PrintParseTree(struct AndList *pAnd)
{
  while (pAnd)
    {
      struct OrList *pOr = pAnd->left;
      while (pOr)
        {
          struct ComparisonOp *pCom = pOr->left;
          if (pCom!=NULL)
            {
              {
                struct Operand *pOperand = pCom->left;
                if(pOperand!=NULL)
                  {
                    cout<<pOperand->value<<" ";
                  }
              }
              switch(pCom->code)
                {
                case 1:
                  cout<<" < "; break;
                case 2:
                  cout<<" > "; break;
                case 3:
                  cout<<" = ";
                }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL)
                  {
                    cout<<pOperand->value<<" ";
                  }
              }
            }
          if(pOr->rightOr)
            {
              cout<<" OR ";
            }
          pOr = pOr->rightOr;
        }
      if(pAnd->rightAnd)
        {
          cout<<" AND ";
        }
      pAnd = pAnd->rightAnd;
    }
  cout << endl;
}

int main () {



  yyparse();

  // these data structures hold the result of the parsing
  extern struct FuncOperator *finalFunction;
  extern struct TableList *tables;
  extern struct AndList *boolean;
  extern struct NameList *groupingAtts;
  extern struct NameList *attsToSelect;
  extern int distinctAtts;
  extern int distinctFunc;


  PrintParseTree(boolean);
  // SELECT SUM DISTINCT (a.b + b), d.g
  // FROM a AS b
  // WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5)
  // GROUP BY a.f, c.d, g.f

  // q6 first ish., pure select.
  // q3 looks pretty easy
  // q10
  // q4 as well, superfluous join,
  // selects before joins.

  clog << "distinct atts " << distinctAtts << endl;
  clog << "function for distinct was :" << distinctFunc << ". 0 means SUM, 1 means SUM DISTINCT" << endl;


}


