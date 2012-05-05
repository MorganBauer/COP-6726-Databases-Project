
#include <iostream>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <vector>
#include <map>
#include <set>
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

char * catalog_path = "catalog";
char * tpch_dir ="/tmp/dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
// char *tpch_dir ="/cise/homes/mhb/dbi/origData/";
char * dbfile_dir = "/tmp/mhb/";



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
extern int createTable; // 1 if the SQL is create table
extern int tableType; // 0 for heap, 1 for sorted.
extern int insertTable; // 1 if the command is Insert into table
extern int dropTable; // 1 is the command is Drop table
extern int outputChange;
extern int planOnly;
extern int setStdOut;
extern string tableName;
extern string fileName;
extern bool keepGoing;
extern bool pureSelection;
extern vector<Attribute> attributes;

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
      // cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
      s.CopyRel(tblst->tableName,tblst->aliasAs);
      Schema oldSchema = schs[tblst->tableName];
      // might need to reseat iterior before assignment.
      oldSchema.Reseat(tblst->aliasAs);
      schs[tblst->aliasAs] = oldSchema;
      tblst = tblst->next;
    }
}

vector<string> GetTableNames (TableList * tblst)
{
  vector<string> names;
  while (0 != tblst)
    {
      string name(tblst->tableName);
      names.push_back(name);
      tblst = tblst->next;
    }
  reverse(names.begin(), names.end());
  return names;
}

unsigned NumTables (TableList * tblst)
{
  unsigned tableCount = 0;
  while (0 != tblst)
    {
      tableCount++;
      // cout << "Table " << tblst->tableName << " is aliased to " << tblst->aliasAs << endl;
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
  TreeNode (QueryNodeType c) : code(c), down(0), InPipeID(0), OutPipeID(0) {}
  Schema sch;
  TreeNode * down;
  unsigned InPipeID;
  unsigned OutPipeID;
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
    clog << " we came from " << up << " and are connected to it through Pipe " << InPipeID << endl;
    clog << "********" << endl;
  }
};

struct ProjectNode : TreeNode
{
  ProjectNode() : TreeNode(Project) {}
  vector<int> attsToKeep;
  int numIn;
  int numOut;
  ~ProjectNode() { }
  void Print()
  {
    clog << "********" << endl
         << "This is a Projection Node " << this << endl
         << numIn << " attributes are coming in" << endl
         << numOut << " attributes are going out" << endl
         << "it is connected to the node " << down << endl
         << "by a pipe, with PipeID: " << OutPipeID << endl
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

struct WriteOutNode : TreeNode
{
  WriteOutNode() : TreeNode(WriteOut) {}
  string fileOutName;
  void Print()
  {
    clog << "********" << endl
         << "This is a Write Out To File Node " << this << endl
         << "writing to file " << fileOutName << endl
         << "it is connected " // to the node " << down << endl
         << "by a pipe, with PipeID: " << InPipeID << endl
         << " the out schema is ";
    sch.Print();
    clog << "********" << endl;
  }
};


void GetAttsOut(NameList * nmlst, vector<string> & attrs)
{
  while (0 != nmlst)
    {
      string attr(nmlst->name);
      attrs.push_back(attr);
      nmlst = nmlst->next;
    }
  reverse(attrs.begin(), attrs.end());
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
        case WriteOut:
            {
              WriteOutNode * won = ((WriteOutNode *)tn);
              won->Print();
              PrintScheduleTree(won->down);
            }
          break;
        default:
          {
            clog << "did not implement printing of node with code " << tn->code << endl;
          }
        }

    }
}

void ExecuteScheduleTree(TreeNode * tn)
{
  if(0 != tn)
    {
      switch(tn->code)
        {
        case Project :
          {
            // print the project node.
            ProjectNode * pn = ((ProjectNode *)tn);
            PrintScheduleTree(pn->down);
          }
          break;
        case SelectFile:
          {
            SelectFileNode * sfn = ((SelectFileNode *)tn);
            return; // this is a dead end, there can be nothing below it.
          }
          break;
        case WriteOut:
            {
              WriteOutNode * won = ((WriteOutNode *)tn);
              PrintScheduleTree(won->down);
            }
          break;
        default:
          {
            clog << "did not implement printing of node with code " << tn->code << endl;
          }
        }
    }
}

static unsigned pipeID = 0;
int GetPipeID()
{
  return ++pipeID;
}


void FreeTableList (TableList * lst)
{
  while (0 != lst)
    {
      TableList * oldLst = lst;
      lst = oldLst->next;
      oldLst->next = 0;
      free(oldLst->tableName);
      free(oldLst->aliasAs);
      free(oldLst);
    }
}

void FreeAndList (AndList * lst)
{
  while (0 != lst)
    {
      AndList * oldLst = lst;
      lst = oldLst->rightAnd;
      oldLst->rightAnd = 0;
      free(oldLst);
    }
}

void FreeNameList (NameList * lst)
{
  while (0 != lst)
    {
      NameList * oldLst = lst;
      lst = oldLst->next;
      oldLst->next = 0;
      free(oldLst);
    }
}

void cleanup(void)
{
  // extern struct FuncOperator *finalFunction;
  // extern struct TableList *tables;
  // FreeTableList(tables);
  // extern struct AndList *boolean;
  // FreeAndList(boolean); // need to recurse more deeper into the list to free everything else.
  //  extern struct NameList *groupingAtts;
  // extern struct NameList *attsToSelect; // final bits out,
  // FreeNameList(groupingAtts);
  // FreeNameList(attsToSelect);
  fileName = "";
  tableName = "";
  // empty the vector of attributes, free the namestring.
  for(vector<Attribute>::iterator it = attributes.begin(); it != attributes.end(); it++)
    {
      free((*it).name);
    }
  attributes.clear();
  distinctAtts = 0; // 1 if there is a DISTINCT in a non-aggregate query
  distinctFunc = 0; // 1 if there is a DISTINCT in an aggregate query
  query = 0;
  // maintenance commands
  // CREATE
  createTable = 0; // 1 if the SQL is create table
  tableType = 0; // 1 for heap, 2 for sorted.
  // INSERT
  insertTable = 0; // 1 if the command is Insert into table
  dropTable = 0; // 1 is the command is Drop table
  // SET command
  outputChange = 0;
  planOnly = 0; // 1 if we are changing settings to planning only. Do not execute.
  setStdOut = 0;
  pureSelection = false;
  pipeID = 0;
}

int main ()
{
  // load all the tables from a previous run



  {
    // const string message("Welcome to MnemosyneDB");
    // const string spaces(message.size(),' ');
    // const string padding ("* " + spaces + " *");
    // const string tbBorder(padding.size(),'*');
    // cout << tbBorder << endl
    //      << padding << endl
    //      << "* " << message << " *" << endl
    //      << padding << endl
    //      << tbBorder << endl << endl;
    cout << "Program defaults to planning only" << endl;
  }
  bool Execute = false;
  bool WriteToFile = false;
  string FileToWriteTo;
  string TableDir (dbfile_dir);
  string TPCHDir (tpch_dir);

  map<string, Schema> TableToSchema; // table to schema.
  map<string, string> TableToFileName;
  set<string> Tables;
  do
    {
      cout << "db> ";

      yyparse();

      HiResTimer rtt; // time taken before control returned to the user.
      rtt.start();

      if (1 == query)
        {
          // create project node
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


          auto queryTableNames = GetTableNames(tables); // get the realnames of all the tables we need.
          // check that all the realnames exist in our set of Tablenames that are available.
          bool haveTables = true;
          for(auto it = queryTableNames.begin(); it != queryTableNames.end(); it++)
            {
              if (0 == Tables.count(*it))
                {
                  clog << "could not find the tables we need to continue" << endl;
                  haveTables = false;
                  break;
                }
            }
          if(!haveTables)
            {
              cerr << "fatty fatty no tables" << endl;
            }
          else
            {
              clog << "have all the tables needed, good to go!" << endl;

              Statistics s;
              s = initStatistics();
              map<std::string, Schema> schemas = TableToSchema;// initSchemas(); // need to put schemas from TableToSchma in here
              CreateTablesAliases(tables,s,schemas); // from clause, creates the aliases in the statistics object using copyrel.

              // number of tables
              unsigned const tableCount = queryTableNames.size();
              cout << "There are " << tableCount << " tables" << endl;

              if (1 == tableCount) // SINGLETABLE
                { // no joins, not selectpipe, purely a selectfile from a single table, to a series of tubes all the way to output.
                  // take entire cnf, put into select file node
                  clog << "only one table" << endl;
                  SelectFileNode * sfnptr = new SelectFileNode();
                  SelectFileNode & sfn = *sfnptr; // this node can take the entire cnf.
                  sfn.cnf = boolean;
                  string onlyTable (tables->tableName);
                  string onlyTableAlias (tables->aliasAs);
                  // schema for select file, need alias of table
                  sfn.sch = schemas[onlyTableAlias];
                  sfn.sch.Print();
                  unsigned freshPID = GetPipeID();
                  sfn.OutPipeID = freshPID;
                  top = sfnptr; // first thing, let's not lose it

                  if(pureSelection)
                    {
                      clog << "This is a pure selection" << endl;
                      // take output of selection, pipe into project
                      ProjectNode * pnptr = new ProjectNode();
                      ProjectNode & pn = *pnptr; // this node can take the entire cnf.
                      top = pnptr;
                      pn.down = &sfn;
                      sfn.up = &pn;

                      sfn.OutPipeID = pn.InPipeID = freshPID;
                      const int numIn(sfn.sch.GetNumAtts());
                      pn.numIn = numIn;
                      const unsigned numOut(NameListLen(attsToSelect));
                      pn.numOut = numOut;
                      {
                        vector<string> attrs;
                        GetAttsOut(attsToSelect, attrs);
                        // old schema is sfn.sch
                        // attributes as vector is attrs
                        vector<int> attsToKeep;
                        for (auto it = attrs.begin(); it != attrs.end(); it++)
                          {
                            attsToKeep.push_back(sfn.sch.Find((*it).c_str()));
                          }
                        assert (numOut == attsToKeep.size());
                        Schema pOutSchema(sfn.sch, attsToKeep); // need to generate the new schema, that has numout attrs, which are those of the attrindexestokeep
                        pn.attsToKeep = attsToKeep;
                        pn.sch = pOutSchema;
                      }
                    }
                  else // something more complicated than a pure selection.
                    {
                      if (1 == distinctAtts) // distinct non-aggregate
                        {}
                      else if (1 == distinctFunc) // distinct Aggregate (there is a sum over the distinct atts)
                        {}
                    }
                }

              if(WriteToFile) // add a writeFileNode at the end
                {
                  WriteOutNode * wonptr = new WriteOutNode;
                  WriteOutNode & won = *wonptr;
                  won.sch = top->sch; // take directly previous schema
                  unsigned freshPID = GetPipeID();
                  won.InPipeID = top->OutPipeID = freshPID;
                  won.fileOutName = FileToWriteTo;
                  won.down = top;
                  top = wonptr; // make the new top the won node
                }

              // everything has been constructed at this point

              // print
              clog << endl << " Printing the query plan" << endl << endl;
              PrintScheduleTree(top);

              // execute.
              if (Execute)
                {
                  clog << endl << " Executing the query " << endl << endl;
                  // we've got our output node top.
                  ExecuteScheduleTree(top);
                  // select files need to know files to select.
                  // need to instantiate relation operators
                  // need to establish pipes from place to place
                  // for our select file nodes, need to
                }
              clog << "was a query" << endl;
            }
        }
      else if (0 == query) // not a query
        {
          clog << "was a maintenance command" << endl;
          if (1 == outputChange) // SET OUTPUT
            {
              clog << "OutputChanging command" << endl;
              // check variable planOnly
              clog << "FILE IS " << fileName << endl;

              if(1 == planOnly)
                {
                  clog << "PLANNING ONLY, NO EXECUTION." << endl;
                  WriteToFile = false;
                  Execute = false;
                  FileToWriteTo = "";
                }
              else if (1 == setStdOut)
                {
                  clog << "RESULTS TO STANDARD OUT, EXECUTE." << endl;
                  WriteToFile = false;
                  Execute = true;
                  FileToWriteTo = "";
                }
              else if (!fileName.empty()) // we have a filename. I should probably use an explicit flag here.
                {
                  clog << "RESULTS TO FILE, EXECUTE." << endl;
                  string fname(fileName);
                  WriteToFile = true;
                  Execute = true;
                  clog << "FILE IS " << fname << endl;
                  FileToWriteTo = fname;
                }
            }
          else if (1 == insertTable) // INSERT 'mytable.tbl' INTO relation
            {
              cout << "insert table" << endl;
              if(0 == Tables.count(tableName))
                {
                  cout << "there was no table by that name, doing nothing, try again." << endl;
                }
              else
                {
                  DBFile dbfile;
                  char * fname = strdup(TableToFileName[tableName].c_str());
                  dbfile.Open(fname);
                  free(fname);
                  string tableLocation (TPCHDir+fileName);
                  clog << tableLocation << endl;
                  fname = strdup(tableLocation.c_str());
                  dbfile.Load(TableToSchema[tableName],fname);
                  free(fname);
                  dbfile.Close();
                }
            }
          else if (1 == createTable)
            {
              cout << "create table" << endl;
              if(1 == Tables.count(tableName))
                {
                  cout << "table already exists, drop table first, doing nothing, try again." << endl;
                }
              else{
                // CREATE TABLE mytable (att1 INTEGER, att2 DOUBLE, att3 STRING) AS HEAP;
                // this should create a 'mytable.bin' file.
                // need to manage that alias of mytable -> mytable.bin
                // alias of mytable to Schema();

                // need bookkeeping to remember the tables name.

                // vector<myAttribute> attributes; // make schema with this, store in map

                clog << "this many attributes in the new table" << attributes.size() << endl;

                Schema newTableSchema(tableName.c_str(),attributes.size(),&attributes[0]);
                newTableSchema.Print();
                TableToSchema[tableName] = newTableSchema;
                static const int HeapTableType(1);
                static const int SortedTableType(2);

                DBFile dbfile;
                string tableLocation (TableDir+tableName+".bin");
                TableToFileName[tableName] = tableLocation;
                Tables.insert(tableName);
                clog << "table will be located at " << tableLocation << endl;
                if (HeapTableType == tableType)
                  {
                    clog << "dbfile created in heap mode " << endl;
                    char * blah = strdup(tableLocation.c_str());
                    dbfile.Create(blah,heap,0);
                    dbfile.Close();
                    free(blah);
                  }
                else if (SortedTableType == tableType)
                  {
                    // struct {OrderMaker *om; int l;} startup = {&om, runlen};
                    // int rv = dbfile.Create (tableLocation, sorted, &startup); // create
                  }
              }
            }
          else if (1 == dropTable)
            {
              cout << "drop table" << endl;
              if(1 == Tables.count(tableName))
                {
                  remove(TableToFileName[tableName].c_str()); // remove bin file
                  remove((TableToFileName[tableName]+".meta").c_str()); // remove meta file
                  TableToFileName.erase(tableName); // forget about bin file
                  TableToSchema.erase(tableName); // forget about schema
                  Tables.erase(tableName); // forget about table
                }
              else
                {
                  cout << "table doesn't exist, create table first, doing nothing, try again." << endl;
                }
            }
        }
      else // something else, like quit maybe?
        {
          clog << endl;
        }
      rtt.stop();
      rtt.duration();
      // do cleanup of all variables from parser, or maybe at the top.
      cleanup();
    }
  while (keepGoing); // uncomment when we get the final stuff up.
  cleanup();
  // shutdown here
  // write catalog to disk.
}
