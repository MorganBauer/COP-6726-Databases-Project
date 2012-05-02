
#include <iostream>
#include <algorithm>
#include "ParseTree.h"
#include "Statistics.h"
#include <cassert>
#include "Schema.h"
#include "main.h"
using namespace std;

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

typedef vector<AndList> Clauses;

// these data structures hold the result of the parsing
extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
// AndList const * const cnf = boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect; // final bits out,

extern int distinctAtts;
extern int distinctFunc;

extern int query;
extern int outputChange;
extern int planOnly;
extern int setStdOut;
extern char * outName;


char * const supplier = "supplier";
char * const partsupp = "partsupp";
char * const part = "part";
char * const nation = "nation";
char * const customer = "customer";
char * const orders = "orders";
char * const region = "region";
char * const lineitem = "lineitem";

map<std::string, Schema> initSchemas()
{
  map<std::string, Schema> schemas;
  schemas[region] = Schema("catalog",region);
  schemas[part] = Schema("catalog",part);
  schemas[partsupp] = Schema("catalog",partsupp);
  schemas[nation] = Schema("catalog",nation);
  schemas[customer] = Schema("catalog",customer);
  schemas[supplier] = Schema("catalog",supplier);
  schemas[lineitem] = Schema("catalog",lineitem);
  schemas[orders] = Schema("catalog",orders);
  return schemas;
}

Statistics initStatistics(void)
{
  // char region[] = "region";
  // char nation[] = "nation";
  // char part[] = "part";
  // char supplier[] = "supplier";
  // char partsupp[] = "partsupp";
  // char customer[] = "customer";
  // char orders[] = "orders";
  // char lineitem[] = "lineitem";
  // region=5
  //   nation=25
  //   part=200000
  //   supplier=10000
  //   partsupp=800000
  //   customer=150000
  //   orders=1500000
  //   lineitem=6001215
  Statistics s;
  s.AddRel(region,5);
  s.AddRel(nation,25);
  s.AddRel(part,200000);
  s.AddRel(supplier,10000);
  s.AddRel(partsupp,800000);
  s.AddRel(customer,150000);
  s.AddRel(orders,1500000);
  s.AddRel(lineitem,6001215);

  // region
  s.AddAtt(region, "r_regionkey",5); // r_regionkey=5
  s.AddAtt(region, "r_name",5); // r_name=5
  s.AddAtt(region, "r_comment",5); // r_comment=5
  // nation
  s.AddAtt(nation, "n_nationkey",25); // n_nationkey=25
  s.AddAtt(nation, "n_name",25);  // n_name=25
  s.AddAtt(nation, "n_regionkey",5);  // n_regionkey=5
  s.AddAtt(nation, "n_comment",25);  // n_comment=25
  // part
  s.AddAtt(part, "p_partkey",200000); // p_partkey=200000
  s.AddAtt(part, "p_name",200000); // p_name=199996
  s.AddAtt(part, "p_mfgr",200000); // p_mfgr=5
  s.AddAtt(part, "p_brand",200000); // p_brand=25
  s.AddAtt(part, "p_type",200000); // p_type=150
  s.AddAtt(part, "p_size",200000); // p_size=50
  s.AddAtt(part, "p_container",200000); // p_container=40
  s.AddAtt(part, "p_retailprice",200000); // p_retailprice=20899
  s.AddAtt(part, "p_comment",200000); // p_comment=127459
  // supplier
  s.AddAtt(supplier,"s_suppkey",10000);
  s.AddAtt(supplier,"s_name",10000);
  s.AddAtt(supplier,"s_address",10000);
  s.AddAtt(supplier,"s_nationkey",25);
  s.AddAtt(supplier,"s_phone",10000);
  s.AddAtt(supplier,"s_acctbal",9955);
  s.AddAtt(supplier,"s_comment",10000);
  // partsupp
  s.AddAtt(partsupp,"ps_partkey",200000);
  s.AddAtt(partsupp,"ps_suppkey",10000);
  s.AddAtt(partsupp,"ps_availqty",9999);
  s.AddAtt(partsupp,"ps_supplycost",99865);
  s.AddAtt(partsupp,"ps_comment",799123);
  // customer
  s.AddAtt(customer,"c_custkey",150000);
  s.AddAtt(customer,"c_name",150000);
  s.AddAtt(customer,"c_address",150000);
  s.AddAtt(customer,"c_nationkey",25);
  s.AddAtt(customer,"c_phone",150000);
  s.AddAtt(customer,"c_acctbal",140187);
  s.AddAtt(customer,"c_mktsegment",5);
  s.AddAtt(customer,"c_comment",149965);
  // orders
  s.AddAtt(orders,"o_orderkey",1500000);
  s.AddAtt(orders,"o_custkey",99996);
  s.AddAtt(orders,"o_orderstatus",3);
  s.AddAtt(orders,"o_totalprice",1464556);
  s.AddAtt(orders,"o_orderdate",2406);
  s.AddAtt(orders,"o_orderpriority",5);
  s.AddAtt(orders,"o_clerk",1000);
  s.AddAtt(orders,"o_shippriority",1);
  s.AddAtt(orders,"o_comment",1478684);
  // lineitem
  s.AddAtt(lineitem,"l_orderkey",1500000);
  s.AddAtt(lineitem,"l_partkey",200000);
  s.AddAtt(lineitem,"l_suppkey",10000);
  s.AddAtt(lineitem,"l_linenumber",7);
  s.AddAtt(lineitem,"l_quantity",50);
  s.AddAtt(lineitem,"l_extendedprice",933900);
  s.AddAtt(lineitem,"l_discount",11);
  s.AddAtt(lineitem,"l_tax",9);
  s.AddAtt(lineitem,"l_returnflag",3);
  s.AddAtt(lineitem,"l_linestatus",2);
  s.AddAtt(lineitem,"l_shipdate",2526);
  s.AddAtt(lineitem,"l_commitdate",2466);
  s.AddAtt(lineitem,"l_receiptdate",2554);
  s.AddAtt(lineitem,"l_shipinstruct",4);
  s.AddAtt(lineitem,"l_shipmode",7);
  s.AddAtt(lineitem,"l_comment",4501941);

  return s;
}

void PrintParseTree(struct AndList *pAnd)
{
  cout << "(";
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
                    cout<<pOperand->value<<"";
                  }
              }
              switch(pCom->code)
                {
                case LESS_THAN:
                  cout<<" < "; break;
                case GREATER_THAN:
                  cout<<" > "; break;
                case EQUALS:
                  cout<<" = "; break;
                default:
                  cout << " unknown code " << pCom->code;
                }
              {
                struct Operand *pOperand = pCom->right;
                if(pOperand!=NULL)
                  {
                    cout<<pOperand->value<<"";
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
          cout<<") AND (";
        }
      pAnd = pAnd->rightAnd;
    }
  cout << ")" << endl;
}

void PrintTablesAliases (TableList * tblst)
{
  while (0 != tblst)
    {
      cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
      tblst = tblst->next;
    }
}

/* for each alias in the above table, do a CopyRel for it from the
   tableName to aliasAs */
void CreateTablesAliases (TableList * tblst, Statistics & s, map<std::string, Schema> & schs)
{
  while (0 != tblst)
    {
      cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
      s.CopyRel(tblst->tableName,tblst->aliasAs);
      Schema oldSchema = schs[tblst->tableName];
      // might need to reseat iterior before assignment.
      oldSchema.Reseat(tblst->aliasAs);
      schs[tblst->aliasAs] = oldSchema;
      tblst = tblst->next;
    }
}

unsigned NumTables (TableList * tblst)
{
  unsigned tableCount = 0;
  while (0 != tblst)
    {
      tableCount++;
      cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
      tblst = tblst->next;
    }
  return tableCount;
}

// fill a vector with pointers to the aliases of the various tables
void FillTablePtrVector(TableList * tblst, vector < char *> & v)
{
  while (0 != tblst)
    {
      cout << tblst->aliasAs << endl;;
      v.push_back(tblst->aliasAs);
      tblst = tblst->next;
    }
  return;
}

size_t NameListLen(NameList * nmlst)
{
  size_t len= 0;
  while (0 != nmlst)
    {
      len++;
      nmlst = nmlst->next;
    }
  return len;
}

void PrintNameList(NameList * nmlst)
{
  while (0 != nmlst)
    {
      cout << nmlst->name << "&";
      nmlst = nmlst->next;
    }
  cout << endl;
}

void SeparateJoinsAndSelects(struct AndList *pAnd, vector<AndList>& joins, vector<AndList>& selects)
{
  while (0 != pAnd)
    {
      struct OrList *pOr = pAnd->left;
      struct ComparisonOp *pCom = pOr->left;
      if (pCom!=NULL)
        {
          Operand *lOperand = pCom->left;
          Operand *rOperand = pCom->right;
          switch(pCom->code)
            {
            case LESS_THAN:
            case GREATER_THAN:
              {
                AndList toPush = *pAnd;
                toPush.rightAnd = 0;
                selects.push_back(toPush); break;
              }
            case EQUALS:
              if ((0 != lOperand and (NAME == lOperand->code)) and
                  (0 != rOperand and (NAME == rOperand->code)))
                {
                  AndList toPush = *pAnd;
                  toPush.rightAnd = 0;
                  joins.push_back(toPush);
                }
              else
                {
                  AndList toPush = *pAnd;
                  toPush.rightAnd = 0;
                  selects.push_back(toPush);
                }
              break;
            default:
              cout << " unknown code " << pCom->code;
            }
        }
      pAnd = pAnd->rightAnd;
    }
  cout << endl;
}

void FindVirtualSelects(Clauses & selects, Statistics & s, Clauses & vs)
{
  // everything that has come in is a select,
  // thus can be equality, or inequality,
  // but will not have a NAME on both sides.
  Clauses::iterator it;
  for(it = selects.begin(); it != selects.end(); it++)
    {
      PrintParseTree(&(*it));
      struct OrList *pOr = (*it).left;

      if (0 != pOr->rightOr) // fail early, if there is no next node, don't bother examining anything.
        {
          set<string> tablesNeeded;
          // take each of the arguments,
          while (0 != pOr)
            {
              struct ComparisonOp *pCom = pOr->left;
              Operand *lOperand = pCom->left;
              Operand *rOperand = pCom->right;

              Operand *opnd = 0;
              Operand *constant = 0;
              if (NAME == lOperand->code)
                {opnd = lOperand; constant = rOperand; }
              else if (NAME == rOperand->code)
                {opnd = rOperand; constant = lOperand;}
              assert(0 != opnd); // something was assigned
              assert(0 != constant); // something was assigned
              clog << "attribute " <<  opnd->value << " needs table ";
              string const tableNeeded = s.getAttrHomeTable(opnd->value);
              clog << tableNeeded << ".";
              tablesNeeded.insert(tableNeeded); // look up operands home table, put into set
              pOr = pOr->rightOr;
            }
          if(tablesNeeded.size() > 1)
            {
              vs.push_back(*it);
              it = selects.erase(it);
              it--; // go back one, because the iterator returned above is one after where the old iterator pointed (an implicit ++ from the removed guy). Our for loop is doing the advancing, so I need to go back (or change the for loop).
            }
        }
      else
        {
          clog << "single clause OR, moving to next " << endl;
        }
    }
  clog << endl;
}

string getSelectAttr(struct AndList *pAnd)
{
  struct OrList *pOr = pAnd->left;
  struct ComparisonOp *pCom = pOr->left;
  {
    Operand *lOperand = pCom->left;
    Operand *rOperand = pCom->right;

    Operand *opnd = 0;
    Operand *constant = 0;
    if (NAME == lOperand->code)
      {opnd = lOperand; constant = rOperand; }
    else if (NAME == rOperand->code)
      {opnd = rOperand; constant = lOperand;}
    assert(0 != opnd); // something was assigned
    assert(0 != constant); // something was assigned
    return string(opnd->value);
  }
}

enum QueryNodeType {Generic, SelectFile, SelectPipe, Project, Join, DuplicateRemoval, Sum, GroupBy, WriteOut };

struct TreeNode
{
  QueryNodeType code;
  TreeNode () {}
  TreeNode (QueryNodeType c) : code(c) {}
  Schema sch;
  unsigned PipeID;
  virtual ~TreeNode () {}
};

struct SelectPipeNode : TreeNode
{
  TreeNode * up;
  AndList * cnf; // the selection predicate.
  string attr; // the attribute to be selected upon
  SelectPipeNode() : TreeNode(SelectPipe) {}
};

struct SelectFileNode : TreeNode
{
  TreeNode * up;
  SelectFileNode() : TreeNode(SelectFile) {}
  AndList * cnf; // the selection predicate.
  string attr; // the attribute to be selected upon
  void Print(void)
  {
    clog << "********" << endl
         << "This is a Selection Node " << this << endl
         << " on at least the attribute " << attr << endl;
    clog << "the selection condition is " << endl;
    PrintParseTree(cnf);
    clog << " we came from " << up << " and are connected to it through Pipe " << PipeID << endl;
    clog << "********" << endl;
  }
};

struct ProjectNode : TreeNode
{
  ProjectNode() : TreeNode(Project) {}
  vector<int> attsToKeep;
  int numIn;
  int numOut;
  TreeNode * down;
  ~ProjectNode() { }
  void Print()
  {
    clog << "********" << endl
         << "This is a Projection Node " << this << endl
         << numIn << " attributes are coming in" << endl
         << numOut << " attributes are going out" << endl
         << "it is connected to the node " << down << endl
         << "by a pipe, with PipeID: " << PipeID << endl
         << " the out schema is ";
    sch.Print();
    clog << "********" << endl;
  }
};

struct JoinNode : TreeNode
{
  JoinNode() : TreeNode(Join) {}
  TreeNode * left;
  TreeNode * right;
};

void GetAttsOut(NameList * nmlst, vector<string> & attrs)
{
  while (0 != nmlst)
    {
      string attr(nmlst->name);
      attrs.push_back(attr);
      nmlst = nmlst->next;
    }
  return;
}

void PrintScheduleTree(TreeNode * tn)
{
  if(0 != tn)
    {
      switch(tn->code)
        {
        case Project :
          {
            // print the project node.
            ProjectNode * pn = ((ProjectNode *)tn);
            pn->Print();
            PrintScheduleTree(pn->down);
          }
          break;
        case SelectFile:
          {
            SelectFileNode * sfn = ((SelectFileNode *)tn);
            sfn->Print();
            return; // this is a dead end, there can be nothing below it.
          }
          break;
        default:
          {

          }
        }

    }
}

static unsigned pipeID = 0;
int GetPipeID()
{
  return ++pipeID;
}

void FreeNameList (NameList * nmlst)
{
  while (0 != nmlst)
    {
      NameList * oldNmLst = nmlst;
      nmlst = oldNmLst->next;
      oldNmLst->next = 0;
      free(oldNmLst);
    }
}

void cleanup(void)
{
  // extern struct FuncOperator *finalFunction;
  // extern struct TableList *tables;
  // extern struct AndList *boolean;
  //  extern struct NameList *groupingAtts;
  // extern struct NameList *attsToSelect; // final bits out,
  // FreeNameList(groupingAtts);
  // FreeNameList(attsToSelect);
}

int main ()
{
  {
    const string message("Welcome to MnemosyneDB");
    const string spaces(message.size(),' ');
    const string padding ("* " + spaces + " *");
    const string tbBorder(padding.size(),'*');
    cout << tbBorder << endl
         << padding << endl
         << "* " << message << " *" << endl
         << padding << endl
         << tbBorder << endl;
  }
  bool Execute = false;

  // while (true) // uncomment when we get the final stuff up.
  {
    cout << "mdb> ";
    yyparse();

    HiResTimer rtt; // time taken before control returned to the user.

    if (1 == query)
      {
        // create project node
        ProjectNode pn;
        TreeNode * top;
        // need to malloc/new the space for the pointer for the atts to keep, need to iterate over the atts to select. call the find in schema function for each att to select. save into calloced array, this must be done at almost the last point because I will not know where it is in the entire schema till i have the end schema at the end.

        // likewise for the number of atts in. num atts out is easy to calculate from what atts to select are given.
        if (0 != finalFunction)
          {
            cout << "we have a function to compute" << endl;
          }
        cout << "grouping atts are" << endl;
        PrintNameList(groupingAtts);
        cout << "atts to select are" << endl;
        PrintNameList(attsToSelect);


        // need number of tables, and their aliases, in an array. First, the
        // number of tables
        unsigned const tableCount = NumTables(tables);
        vector < char * > tblPtrs ;
        cout << "There are " << tableCount << " tables" << endl;

        FillTablePtrVector(tables, tblPtrs);
        clog << "size is " << tblPtrs.size();
        assert (tblPtrs.size() == tableCount);
        for (unsigned i = 0 ; i < tableCount ; i++)
          {
            cerr << tblPtrs[i] << " " << endl;
          }

        PrintParseTree(boolean); // cnf

        map<std::string, Schema> schemas = initSchemas();

        Statistics s;
        s = initStatistics();
        // s.Write("beforeAlias");
        // s.print();

        CreateTablesAliases(tables,s,schemas); // from clause
        // ReadTablesSchemas(tables, schemaHolder);

        cerr << "estimating" << endl;
        s.Estimate(boolean, &tblPtrs[0], tableCount);
        cerr << "end estimating" << endl;

        // s.Write("afterAlias");
        // s.print();

        /* break the cnf up into selections and joins, then push selections
           down as far as possible. do the joins from smallest to
           largest. */
        Clauses joins;
        Clauses selects;
        {
          SeparateJoinsAndSelects(boolean,joins, selects);
        }
        // find virtual select ( a condition like (l_orderkey < 100 OR o_orderkey < 100)
        // which requires a joined tables, and thus a select pipe (which I don't think I implemented, heh)
        Clauses virtualSelects;
        FindVirtualSelects(selects, s, virtualSelects);
        clog << "num joins is " << joins.size() << endl
             << "num selects is " << selects.size() << endl
             << "num virtual selects needing joined tables is " << virtualSelects.size() << endl;

        // make nodes for each join and select
        clog << "select file node generation" << endl;
        vector<SelectFileNode> sfNodes;
        for (Clauses::iterator it = selects.begin(); it != selects.end(); it++)
          {
            AndList * cnf = &(*it);
            PrintParseTree(cnf);
            double result = s.Estimate(cnf, &tblPtrs[0], tableCount);
            clog << "select estimate is" << result << endl;
            SelectFileNode sfn;
            sfn.cnf = cnf;
            string attr = getSelectAttr(cnf); // get the attribute that is in the select statement.
            sfn.attr = attr;
            clog << " attr in select is " << attr << endl;
            // I want the schema, so I need the
            string attrHome = s.getAttrHomeTable(attr);
            clog << "table that attr is in is " << attrHome << endl;
            sfn.sch = schemas[attrHome];
            sfn.sch.Print();
            sfNodes.push_back(sfn);
          } // select file nodes now have a schema set, and cnf set.
        assert(sfNodes.size() == selects.size());
        clog << "join node generation" << endl;
        vector<JoinNode> jNodes;
        for (Clauses::iterator it = joins.begin(); it != joins.end(); it++)
          {
            AndList * cnf = &(*it);
            PrintParseTree(cnf);
            double result = s.Estimate(cnf, &tblPtrs[0], tableCount);
            clog << "select estimate is" << result << endl;
            JoinNode jn;
            jNodes.push_back(jn);
          }
        assert(jNodes.size() == joins.size());
        clog << "select pipe node generation" << endl;
        vector<SelectPipeNode> spNodes;
        for (Clauses::iterator it = virtualSelects.begin(); it != virtualSelects.end(); it++)
          {
            AndList * cnf = &(*it);
            PrintParseTree(cnf);
            double result = s.Estimate(cnf, &tblPtrs[0], tableCount); // shouldn't work.
            clog << "select estimate is" << result << endl;
            SelectPipeNode spn;
            spNodes.push_back(spn);
          }
        assert(spNodes.size() == virtualSelects.size());
        clog << "built all the interior nodes from where clause (select, join, virtualSelect)" << endl;
        // connect them all up.

        /* I need to go through each select clause, and
           if there is more than one OR clause inside,
           check that each of it's arguments is belonging to a relation, that is all the same relation, if not, a joined table is required.
        */

        // new Schema (catalog_path, part); // how to get a schema for a relation.....................

        // q6 first ish., pure select.
        // q3 looks pretty easy
        // q10
        // q4 as well, superfluous join,
        // selects before joins.

        clog << "distinct atts " << distinctAtts << endl;
        clog << "function was: " << distinctFunc << ". 0 means SUM, 1 means SUM DISTINCT" << endl;

        // use
        // Function.GrowFromParseTree (finalfunc, *left);
        // constructs arithmetic inside function to

        if(0 == finalFunction)
          {
            clog << "we have no function given, so, there is no SUM or SUM DISTINCT." << endl;;
            if(0 == distinctAtts)
              {
                clog << "there is no distinct at all." << endl;
                /*
                  SELECT l.l_orderkey
                  FROM lineitem AS l
                  WHERE (l.l_quantity > 30)

                  should be
                  project to output
                  select to project
                  file to select

                  select can be selectfile, not select pipe
                */
                if (sfNodes.size() > 0 and 0 == jNodes.size() and 0 == spNodes.size()) // the third is necessarily true from the second. can't have a join selectpipe if there was no join, can only ever have one select if there is no join.
                  {
                    clog << "only select, and nothing else, combined with no function and no distinct, makes hopefully an easy case." << endl;
                    // how do I know to connect anything with anything else.
                    // here we have no joins, and no interior selects. so only select and project.

                    vector<string> attrs;
                    GetAttsOut(attsToSelect, attrs);

                    // guys in NameList come out in reverse order, so reverse it.
                    reverse(attrs.begin(), attrs.end());

                    // find the select statement, that has the attr that is coming out.
                    SelectFileNode sfn;
                    vector<int> attrIndexesToKeep;
                    for (vector<string>::iterator jt = attrs.begin(); jt != attrs.end(); jt++)
                      {
                        for (vector<SelectFileNode>::iterator it = sfNodes.begin(); it != sfNodes.end(); it++)
                          {
                            if (-1 != (*it).sch.Find((*jt).c_str())) // if it isn't found,
                              { // then it has been found
                                auto location = (*it).sch.Find((*jt).c_str());
                                attrIndexesToKeep.push_back(location);
                                sfn = (*it);
                              }
                          }
                      }
                    // at this point, we have the attrs of the schema that exists in the node below, and the node below.
                    // sfn, is our node that we will point our select at, and have point at our project.
                    // attrIndexesToKeep, is the 'array' that we will give

                    // NameList *attsToSelect; // final bits out,

                    unsigned freshPID = GetPipeID();
                    sfn.PipeID = pn.PipeID = freshPID;
                    int numIn(sfn.sch.GetNumAtts());
                    int numOut(NameListLen(attsToSelect));
                    Schema pOutSchema(sfn.sch, attrIndexesToKeep); // need to generate the new schema, that has numout attrs, which are those of the attrindexestokeep
                    pOutSchema.Print(); // oh my god is this right...?
                    pn.attsToKeep = attrIndexesToKeep;
                    pn.numIn = numIn;
                    pn.numOut = numOut;
                    pn.sch = pOutSchema;
                    pn.down = &sfn;
                    sfn.up = &pn;
                    top = &pn;
                  }
              }
          }

        // everything has been constructed at this point

        // print
        clog << endl << " Printing the query plan" << endl << endl;
        // assuming for now that the last thing is always a ProjectNode, which we created at the beginning. (It could be a write to file, implemented in p5)
        PrintScheduleTree(top);

        // execute.
        clog << endl << " Executing the query " << endl << endl;
        // we've got our output node top.

        // select files need to know files to select.
        // need to instantiate relation operators
        // need to establish pipes from place to place
        // for our select file nodes, need to
        clog << "was a query" << endl;
      }
    else
      {
        clog << "was a maintenance command" << endl;
        if (1 == outputChange)
          {
            clog << "OutputChanging command" << endl;
            // check variable planOnly
            clog << "FILE IS " << (void*)outName << endl;

            if(1 == planOnly)
              {
                clog << "PLANNING ONLY, NO EXECUTION." << endl;
                Execute = false;
              }
            else if (1 == setStdOut)
              {
                 clog << "RESULTS TO STANDARD OUT, EXECUTE." << endl;
                 Execute = true;
              }
            else if (0 != outName)
              {
                 clog << "RESULTS TO FILE, EXECUTE." << endl;
                 string fname(outName);
                 clog << "FILE IS " << fname << endl;
                 Execute = true;
              }
          }
      }
    clog << endl;
  }
}
