#include <iostream>
#include <algorithm>
#include <vector>
#include "DBFile.h"
#include "a1test.h"

/* Morgan Bauer */

// make sure that the file path/dir information below is correct
char *dbfile_dir = "/tmp/mhb/"; // dir where binary heap files should be stored
// char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/1G/"; // dir where dbgen tpch files (extension *.tbl) can be found
//char *tpch_dir ="/cise/homes/mhb/dbi/origData/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *tpch_dir ="/Users/morganbauer/Downloads/tpch_2_14_3/dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;
Relation *rel;

// load from a tpch file
void test1 () {

  DBFile dbfile;
  cout << " DBFile will be created at " << rel->path () << endl;
  dbfile.Create (rel->path(), heap, NULL); // IMPLEMENT THIS

  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " tpch file will be loaded from " << tbl_path << endl;

  dbfile.Load (*(rel->schema ()), tbl_path); // IMPLEMENT THIS
  dbfile.Close (); // IMPLEMENT THIS
}

// sequential scan of a DBfile
void test2 () {

  DBFile dbfile;
  dbfile.Open (rel->path()); // IMPLEMENT
  dbfile.MoveFirst (); // IMPLEMENT

  Record temp;

  int counter = 0;
  while (dbfile.GetNext (temp) == 1) { // IMPLEMENT THIS
    counter += 1;
    //temp.Print (rel->schema());
    if (counter % 10000 == 0) {
      cout << counter << "\n";
    }
  }
  cout << " scanned " << counter << " recs \n";
  dbfile.Close (); // IMPLEMENTED FOR TEST-ONE
}

// scan of a DBfile and apply a filter predicate
void test3 () {

  cout << " Filter with CNF for : " << rel->name() << "\n";

  CNF cnf;
  Record literal;
  rel->get_cnf (cnf, literal);

  cnf.Print ();

  DBFile dbfile;
  dbfile.Open (rel->path()); // IMPLEMENTED FOR TEST-TWO
  dbfile.MoveFirst (); // IMPLEMENTED FOR TEST-TWO

  Record temp;

  int counter = 0;
  while (dbfile.GetNext (temp, cnf, literal) == 1) { // IMPLEMENT THIS
    counter += 1;
    temp.Print (rel->schema());
    if (counter % 10000 == 0) {
      cout << counter << "\n";
    }
  }
  cout << " selected " << counter << " recs \n";
  dbfile.Close (); // IMPLEMENTED FOR TEST ONE
}

void test4 ()
{
  test2 ();

  DBFile dbfile;
  Record temp; // hold a record

  dbfile.Open (rel->path()); // IMPLEMENTED FOR TEST-TWO
  dbfile.MoveFirst();
  dbfile.GetNext(temp); //Get the first record
  cout << "ADDING" << endl;
  dbfile.Add(temp);
  cout << "ADDED" << endl;
  dbfile.Close(); // test 2 opens and closes the file itself.

  test2 ();
}

void generateAll ()
{
  DBFile dbfile;
  cout << " DBFile will be created at " << rel->path () << endl;
  dbfile.Create (rel->path(), heap, NULL); // IMPLEMENT THIS

  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " tpch file will be loaded from " << tbl_path << endl;

  dbfile.Load (*(rel->schema ()), tbl_path); // IMPLEMENT THIS
  dbfile.Close (); // IMPLEMENT THIS
}

struct sorter : public std::binary_function<Record *, Record *, bool>
{
  OrderMaker * _so;
public:
  sorter(OrderMaker so) {this->_so = &so;}
  bool operator()(Record & _x, Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(&_x, &_y, _so) < 0) ? true : false; }
  bool operator()(Record * _x, Record * _y) { ComparisonEngine comp;
    return  (comp.Compare((_x), (_y), _so) < 0) ? true : false; }
};

// void ptfn (Record * rr)
// {
//   rr->Print (rel->schema());
// }

void ptfn (Record rr)
{
  rr.Print (rel->schema());
}

void testSort()
{
  cout << endl << endl << endl
       << " TESTING SORTING " 
       << endl << endl << endl;
  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " tpch file will be loaded from " << tbl_path << endl;
  FILE *tableFile = fopen (tbl_path, "r");

  vector < Record> records;
  for (int i = 0; i < 5; i++)
    {
      Record rr;
      rr.SuckNextRecord(&*(rel->schema ()), tableFile);
      records.push_back(rr);
    }

  for_each(records.begin(), records.end(), ptfn);

}

void testCompare()
{
  cout << endl << endl << endl 
       << " TESTING COMPARISON FUNCTIONS AND FUNCTORS "
       << endl << endl << endl;
  Record x;
  Record y;

  char tbl_path[100]; // construct path of the tpch flat text file
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " tpch file will be loaded from " << tbl_path << endl;
  FILE *tableFile = fopen (tbl_path, "r");

  x.SuckNextRecord(&*(rel->schema ()), tableFile);
  y.SuckNextRecord(&*(rel->schema ()), tableFile);
  x.Print (rel->schema());
  y.Print (rel->schema());

  OrderMaker sortorder;
  rel->get_sort_order (sortorder);

  ComparisonEngine ceng;
  int compout = ceng.Compare (&x, &y, &sortorder);
  cout << "comparison says " << compout << endl;



  bool lessthanp; // = sorter(sortorder)(x,y);
  // cout << "lessthanp says " << lessthanp << endl;
  // lessthanp = sorter(sortorder)(&x,&y);
  // cout << "lessthanp says " << lessthanp << endl;
  {
    cout << "using the functor" << endl;
    sorter st = sorter(sortorder);
    lessthanp = st(x,y);
    cout << "lessthanp says " << lessthanp << endl;
    lessthanp = st(&x,&y);
    cout << "lessthanp says " << lessthanp << endl;
  }

  {
    cout << "vector containing pointers to Record, copy" << endl;
    vector<Record *> v;
    v.push_back(&x);
    v.push_back(&y);
    // x.Print (rel->schema()); // works
    // y.Print (rel->schema()); // works
    // Record rt;
    Page pt;
    // rt.Copy(&x);
    // pt.Append(&rt);
    // rt.Copy(&y);
    // pt.Append(&rt);
    // all references are broken after this
    // cout << "first last ref" << endl;
    // (v.front())->Print (rel->schema());
    // v.back()->Print (rel->schema());
    cout << "direct index ref" << endl;
    v[0]->Print (rel->schema());
    v[1]->Print (rel->schema());


    if(false){
      cout << "before sort" << endl << endl;
      sorter st = sorter(sortorder);
      // sort(v.begin(),v.end(),sorter(sortorder));
    }
    // (l_linenumber)
    // cout << "after sort" << endl << "printing records" << endl;
    v[0]->Print (rel->schema());
    v[1]->Print (rel->schema());

    {
      cout << endl << "functor again" << endl;
      Record * r1;
      Record * r2;
      sorter st = sorter(sortorder);
      r1 = v[0];
      r2 = v[1];
      lessthanp = st(r1,r2);
      cout << "lessthanp says " << lessthanp << endl;

      std::sort(v.begin(), v.end(), sorter(sortorder));

      lessthanp = st(r1,r2);
      cout << "lessthanp says " << lessthanp << endl;
    }
    v[0]->Print (rel->schema());
    v[1]->Print (rel->schema());


    pt.EmptyItOut();
  } // vector is destroyed and deallocated after this.

  /*
    {
    cout << "vector containing pointers to Record, nocopy" << endl;
    vector<Record *> v;
    v.push_back(&x);
    v.push_back(&y);
    x.Print (rel->schema()); // works
    y.Print (rel->schema()); // works

    Page pt;
    pt.Append(&x);
    pt.Append(&y);
    // all references are broken after this
    //    (v.front())->Print (rel->schema());
    // v[0]->Print (rel->schema());
    // v[1]->Print (rel->schema());
    // v.back()->Print (rel->schema());
    pt.EmptyItOut();
    } // vector is destroyed and deallocated after this.
  */


}

int main () {

  setup (catalog_path, dbfile_dir, tpch_dir);

  void (*test) ();
  Relation *rel_ptr[] = {n, r, c, p, ps, o, li};
  void (*test_ptr[]) () = {&test1, &test2, &test3, &test4, &testCompare, &testSort, &generateAll};

  int tindx = 0;
  while (tindx < 1 || tindx > 6) {
    cout << " select test: \n";
    cout << " \t 1. load file \n";
    cout << " \t 2. scan \n";
    cout << " \t 3. scan & filter \n";
    cout << " \t 4. add \n";
    cout << " \t 5. testCompare \n";
    cout << " \t 6. testSort \n";
    cout << " \t ";
    cin >> tindx;
  }

  int findx = 0;
  while (findx < 1 || findx > 7) {
    cout << "\n select table: \n";
    cout << "\t 1. nation \n";
    cout << "\t 2. region \n";
    cout << "\t 3. customer \n";
    cout << "\t 4. part \n";
    cout << "\t 5. partsupp \n";
    cout << "\t 6. orders \n";
    cout << "\t 7. lineitem \n \t ";
    cin >> findx;
  }

  rel = rel_ptr [findx - 1];
  test = test_ptr [tindx - 1];

  test ();

  cleanup ();
}
