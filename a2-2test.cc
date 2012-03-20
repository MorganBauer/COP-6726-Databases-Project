#include "a2-2test.h"
#include "BigQ.h"
#include <pthread.h>
#include <cassert>
void test1 ();
void test2 ();
void test3 ();
void test4 ();
void test6 ();

int add_data (FILE *src, int numrecs, int &res) {
  DBFile dbfile;
  dbfile.Open (rel->path ());
  Record temp;

  int proc = 0;
  int xx = 20000;
  while (proc < numrecs && (res = temp.SuckNextRecord (rel->schema (), src))) {
    dbfile.Add (temp);
    if (proc == xx) cerr << "\t ";
    if (proc % xx == 0) cerr << ".";
    ++proc;
  }

  dbfile.Close ();
  return proc;
}


// create a dbfile interactively
void test1 () {

  OrderMaker om;
  rel->get_sort_order (om);

  int runlen = 0;
  while (runlen < 1) {
    cout << "\t\n specify runlength:\n\t ";
    cin >> runlen;
  }
  struct {OrderMaker *om; int l;} startup = {&om, runlen};

  DBFile dbfile;
  cout << "\n output to dbfile : " << rel->path () << endl;
  {
    int rv = dbfile.Create (rel->path(), sorted, &startup);
    cout << "rv = " << rv << endl;
    assert(1 == rv);
    rv = dbfile.Close ();
    cout << "rv = " << rv << endl;
    assert(1 == rv);
  }


  char tbl_path[100];
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " input from file : " << tbl_path << endl;

  FILE *tblfile = fopen (tbl_path, "r");
  if (NULL == tblfile)
    {
      cout << "tblfile is null" << endl;
      assert(NULL != tblfile);
    }
  srand48 (time (NULL));

  int proc = 1, res = 1, tot = 0;
  while (proc && res) {
    int x = 0;
    while (x < 1 || x > 4) {
      cout << "\n select option for : " << rel->path () << endl;
      cout << " \t 1. add a few (1 to 1k recs)\n";
      cout << " \t 2. add a lot (1k to 1e+06 recs) \n";
      cout << " \t 3. run some query \n \t ";
      cout << " \t 4. add all the records \n \t";
      cin >> x;
      cout << x;
    }
    if (x < 3) {
      proc = add_data (tblfile,lrand48()%(int)pow(1e3,x)+(x-1)*1000, res);
      tot += proc;
      if (proc)
        cout << "\n\t added " << proc << " recs..so far " << tot << endl;
    }
    else if (x == 4)
      {}
    else {
      test3 ();
    }
  }
  cout << "\n create finished.. " << tot << " recs inserted\n";
  fclose (tblfile);
}

// sequential scan of a DBfile
void test2 () {

  cout << " scan : " << rel->path() << "\n";
  DBFile dbfile;
  dbfile.Open (rel->path());
  dbfile.MoveFirst ();

  Record temp;

  int cnt = 0;
  cerr << "\t";
  while (dbfile.GetNext (temp) && ++cnt) {
    temp.Print (rel->schema());
    if (cnt % 10000) {
      cerr << ".";
    }
  }
  cout << "\n scanned " << cnt << " recs \n";
  dbfile.Close ();
}

void test3 () {

  CNF cnf;
  Record literal;
  rel->get_cnf (cnf, literal);
  cnf.Print();
  // literal.Print (rel->schema());

  DBFile dbfile;
  dbfile.Open (rel->path());
  dbfile.MoveFirst ();

  Record temp;

  int cnt = 0;
  cerr << "\t";
  while (dbfile.GetNext (temp, cnf, literal) && ++cnt) {
    temp.Print (rel->schema());
    if (cnt % 10000 == 0) {
      cerr << ".";
    }
  }
  cout << "\n query over " << rel->path () << " returned " << cnt << " recs\n";
  dbfile.Close ();
}

void test4 ()
{
  OrderMaker om;
  rel->get_sort_order (om);

  int runlen = 0;
  while (runlen < 1) {
    cout << "\t\n specify runlength:\n\t ";
    cin >> runlen;
  }
  struct {OrderMaker *om; int l;} startup = {&om, runlen};

  DBFile dbfile;
  cout << "\n output to dbfile : " << rel->path () << endl;
  {
    int rv = dbfile.Create (rel->path(), sorted, &startup); // create
    cout << "rv = " << rv << endl;
    assert(SUCCESS == rv);
    rv = dbfile.Close (); // close
    cout << "rv = " << rv << endl;
    assert(1 == rv);
  }

  char tbl_path[100];
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " input from file : " << tbl_path << endl;

  FILE *tblfile = fopen (tbl_path, "r");
  if (NULL == tblfile)
    {
      cout << "tblfile is null" << endl;
      assert(NULL != tblfile);
    }

  dbfile.Open (rel->path ());
  Record temp;
  dbfile.Close();
  int processed = 0;
  for(int i = 0; i < 4; ++i)
    {
      cout << "\t\tadding 10k records" << endl;
      dbfile.Open (rel->path ());
      while ( ++processed <= 10000 && (temp.SuckNextRecord (rel->schema (), tblfile)))
        {
          dbfile.Add (temp);
        }
      dbfile.Close();
      processed = 0;
    }
  cout << "\t\tadding the rest" << endl;
  dbfile.Open (rel->path ());
  while ((temp.SuckNextRecord (rel->schema (), tblfile)))
    {
      dbfile.Add (temp);
    }
  dbfile.Close();
  cout << "done" << endl;
}

void test5 ()
{
  OrderMaker o;
  rel->get_sort_order (o);

  int runlen = 1;
  while (runlen < 1) {
    cout << "\t\n specify runlength:\n\t ";
    cin >> runlen;
  }
  struct {OrderMaker *o; int l;} startup = {&o, runlen};


  DBFile dbfile;
  cout << "\n output to dbfile : " << rel->path () << endl;
  dbfile.Create (rel->path(), sorted, &startup);


  char tbl_path[100];
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " input from file : " << tbl_path << endl;

  int success = dbfile.Open(rel->path());
  dbfile.Load(*(rel->schema()), tbl_path);

  Record rec;

  int recordsRead = 0;
  while (dbfile.GetNext(rec)){
    // rec.Print(rel->schema());
    recordsRead++;
  }
  cout << recordsRead << " records read" << endl;

  dbfile.Close ();
}

void test6 ()
{
  OrderMaker om;
  rel->get_sort_order (om);

  int runlen = 0;
  while (runlen < 1) {
    cout << "\t\n specify runlength:\n\t ";
    cin >> runlen;
  }
  struct {OrderMaker *om; int l;} startup = {&om, runlen};

  DBFile dbfile;
  cout << "\n output to dbfile : " << rel->path () << endl;
  {
    int rv = dbfile.Create (rel->path(), sorted, &startup); // create
    cout << "rv = " << rv << endl;
    assert(SUCCESS == rv);
    rv = dbfile.Close (); // close
    cout << "rv = " << rv << endl;
    assert(1 == rv);
  }

  char tbl_path[100];
  sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
  cout << " input from file : " << tbl_path << endl;

  FILE *tblfile = fopen (tbl_path, "r");
  if (NULL == tblfile)
    {
      cout << "tblfile is null" << endl;
      assert(NULL != tblfile);
    }

  Record temp;
  dbfile.Open (rel->path ());
  int counter = 0;
  while ((temp.SuckNextRecord (rel->schema (), tblfile)))
    {
      counter++;
      dbfile.Add (temp);
    }
  dbfile.Close();
  cout << "added " << counter << " records" << endl;
  cout << "done" << endl;
}

int main (int argc, char *argv[]) {

  setup ();

  relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
  void (*test_ptr[]) () = {&test1, &test2, &test3, &test4, &test5, &test6};
  void (*test) ();

  int tindx = 0;
  while (tindx < 1 || tindx > 6) {
    cout << " select test option: \n";
    cout << " \t 1. create sorted dbfile\n";
    cout << " \t 2. scan a dbfile\n";
    cout << " \t 3. run some query \n";
    cout << " \t 4. add/merge test \n \t ";
    cin >> tindx;
  }

  int findx = 0;
  while (findx < 1 || findx > 8) {
    cout << "\n select table: \n";
    cout << "\t 1. nation \n";
    cout << "\t 2. region \n";
    cout << "\t 3. customer \n";
    cout << "\t 4. part \n";
    cout << "\t 5. partsupp \n";
    cout << "\t 6. supplier \n";
    cout << "\t 7. orders \n";
    cout << "\t 8. lineitem \n \t ";
    cin >> findx;
  }
  rel = rel_ptr [findx - 1];

  test = test_ptr [tindx-1];
  test ();

  cleanup ();
  cout << "\n\n";
}
