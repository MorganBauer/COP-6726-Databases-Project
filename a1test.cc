#include <iostream>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include "DBFile.h"
#include "a1test.h"
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <ctime>
#include <omp.h>
/* Morgan Bauer */

// make sure that the file path/dir information below is correct
char *dbfile_dir = "/tmp/mhb/"; // dir where binary heap files should be stored
// char *tpch_dir ="/cise/tmp/dbi_sp11/DATA/1G/"; // dir where dbgen tpch files (extension *.tbl) can be found
#ifdef linux
char *tpch_dir ="/tmp/dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
#elif __MACH__
char *tpch_dir ="/Users/morganbauer/Downloads/tpch_2_14_3/dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
#endif
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

struct sorter //: public std::binary_function<Record *, Record *, bool>
{
  OrderMaker * _so;
public:
  //sorter(OrderMaker so) {this->_so = &so;}
  sorter(OrderMaker so) : _so(&so){}
  bool operator()(Record & _x, Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(&_x, &_y, _so) < 0) ? true : false; }
  bool operator()(const Record & _x, const Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(const_cast<Record *>(&_x), const_cast<Record *>(&_y), _so) < 0) ? true : false; }
  bool operator()(Record * _x, Record * _y) { ComparisonEngine comp;
    return  (comp.Compare((_x), (_y), _so) < 0) ? true : false; }
};

extern FILE *yyin;

// void ptfn (Record * rr)
// {
//   rr->Print (rel->schema());
// }
 
void ptfn (Record rr)
{
  rr.Print (rel->schema());
}

static char buffer[] = "(l_orderkey) AND (l_returnflag) AND (l_tax) AND (l_discount)";

void hijack_parser()
{
  // hijack yyin here, before yyparse needs it.

  cout << buffer << endl;
#ifdef linux
  yyin = fmemopen(buffer, strlen(buffer),"r");
  if (yyin == NULL)
    {
      cout << "well crap, out of memory" << endl;
      exit (-1);
    }
#elif __MACH__
  //yyin = funopen(); // bsd version that is a pain in the ass
#endif

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
  omp_set_num_threads(4);
  static const int records_to_read = 500;

  // Starting the time measurement
  double start = omp_get_wtime();
  // Computations to be measured

  // Measuring the elapsed time
  double end = omp_get_wtime();
  // Time calculation (in seconds)
  cout << "elpased time is: " << (end-start)
       << " with resolution " << omp_get_wtick() << endl;

//     cout << "sequential time" << endl;
//   {
//     vector < Record *> records;
//     for (int i = 0; i < records_to_read; i++)
//       {
//         records.push_back(new Record);
//         records.back()->SuckNextRecord(&*(rel->schema ()), tableFile);
//       }
//     //    cout << endl << "printing records" << endl;
//     //  for_each(records.begin(), records.end(), ptfn);

//     hijack_parser();

//     OrderMaker sortorder;
//     rel->get_sort_order (sortorder);
//     cout << endl << "sorting records" << endl;
//     start = omp_get_wtime();
//     //#ifdef __gnu_parallel
//     // __gnu_parallel::sort(records.begin(), records.end(), sorter(sortorder));
//     // #else
//     sort(records.begin(), records.end(), sorter(sortorder));
//     // #endif
//     end = omp_get_wtime();
//     cout << "elpased time is: " << (end-start)
//          << " with resolution " << omp_get_wtick() << endl;


//     cout << endl << "sorted records" << endl;
//     cout << "sorted " << records.size() << " records." << endl;
//     //cout << endl << "printing records" << endl;
//     //for_each(records.begin(), records.end(), de);
//   }

//     cout << "parallel time" << endl;
// {
//     vector < Record *> records;
//     for (int i = 0; i < records_to_read; i++)
//       {
//         records.push_back(new Record);
//         records.back()->SuckNextRecord(&*(rel->schema ()), tableFile);
//       }
//     //cout << endl << "printing records" << endl;
//     //  for_each(records.begin(), records.end(), ptfn);

//     hijack_parser();

//     OrderMaker sortorder;
//     rel->get_sort_order (sortorder);
//     cout << endl << "sorting records" << endl;
//     start = omp_get_wtime();
//     //#ifdef __gnu_parallel
//     __gnu_parallel::sort(records.begin(), records.end(), sorter(sortorder));
//     // #else
//     //sort(records.begin(), records.end(), sorter(sortorder));
//     // #endif
//     end = omp_get_wtime();
//     cout << "elpased time is: " << (end-start)
//          << " with resolution " << omp_get_wtick() << endl;


//     cout << endl << "sorted records" << endl;
//     cout << "sorted " << records.size() << " records." << endl;
//     //cout << endl << "printing records" << endl;
//     // for_each(records.begin(), records.end(), ptfn);
//   }

//     cout << "auto chosen time" << endl;
// {
//     vector < Record> records;
//     for (int i = 0; i < records_to_read; i++)
//       {
//         cout << records.capacity() << endl;
//         cout << "about to create record" << endl;
//         records.push_back(*(new Record()));
//         cout << "created record, sucking data" << endl;
//         records.back().SuckNextRecord(rel->schema (), tableFile);
//         cout << "data sucked" << endl;
//       }
//     cout << records.size() << "records read" << endl;
//     // cout << endl << "printing records" << endl;
//     //   for_each(records.begin(), records.end(), ptfn);

//     hijack_parser();

//     OrderMaker sortorder;
//     rel->get_sort_order (sortorder);
//     cout << endl << "sorting records" << endl;
//     start = omp_get_wtime();
//     //    #ifdef __gnu_parallel
//     // cout << "compiler chose parallel version" << endl;
//     // __gnu_parallel::sort(records.begin(), records.end(), sorter(sortorder));
//     // #else
//     // cout << "compiler chose sequential version" << endl;
//     //sort(records.begin(), records.end(), sorter(sortorder));
//     // #endif
//     end = omp_get_wtime();
//     cout << "elpased time is: " << (end-start)
//          << " with resolution " << omp_get_wtick() << endl;


//     cout << endl << "sorted records" << endl;
//     cout << "sorted " << records.size() << " records." << endl;
//  // cout << endl << "printing records" << endl;
//  //     for_each(records.begin(), records.end(), ptfn);
//   }


    cout << "alternate allocation" << endl;
{
    vector < Record> records;
    for (int i = 0; i < records_to_read; i++)
      {
        cout << records.capacity() << endl;
        cout << "about to create record" << endl;
        Record rr;
        cout << "created record, sucking data" << endl;
        rr.SuckNextRecord(rel->schema (), tableFile);
        cout << "pushing record" << endl;
        records.push_back(rr);
        cout << "record pushed" << endl;
      }
    cout << records.size() << "records read" << endl;
     cout << endl << "printing records" << endl;
       for_each(records.begin(), records.end(), ptfn);

    hijack_parser();

    OrderMaker sortorder;
    rel->get_sort_order (sortorder);
    cout << endl << "sorting records" << endl;
    start = omp_get_wtime();
    //    #ifdef __gnu_parallel
    // cout << "compiler chose parallel version" << endl;
    // __gnu_parallel::sort(records.begin(), records.end(), sorter(sortorder));
    // #else
    // cout << "compiler chose sequential version" << endl;
    sort(records.begin(), records.end(), sorter(sortorder));
    // #endif
    end = omp_get_wtime();
    cout << "elpased time is: " << (end-start)
         << " with resolution " << omp_get_wtick() << endl;


    cout << endl << "sorted records" << endl;
    cout << "sorted " << records.size() << " records." << endl;
  cout << endl << "printing records" << endl;
      for_each(records.begin(), records.end(), ptfn);
  }

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

  hijack_parser();

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
