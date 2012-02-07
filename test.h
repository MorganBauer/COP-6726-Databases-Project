#ifndef TEST_H
#define TEST_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include "DBFile.h"
#include "Record.h"
using namespace std;

extern "C" {
  int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

class Relation {

 private:
  char *rname; // Relation name
  Schema *rschema;
  char *prefix; //
  char rpath[100]; // path to relation
 public:
  Relation (char *_name, Schema *_schema, char *_prefix) :
  rname (_name), rschema (_schema), prefix (_prefix) {
    sprintf (rpath, "%s%s.bin", prefix, rname);
  }
  ~Relation()
    {
      delete rschema;
    }
  char* name () { return rname; }
  char* path () { return rpath; }
  Schema* schema () { return rschema;}
  void info () {
    cout << " Relation info\n";
    cout << "\t name: " << name () << endl;
    cout << "\t path: " << path () << endl;
  }

  void get_cnf (CNF &cnf_pred, Record &literal) {
    cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
    if (yyparse() != 0) {
      std::cout << "Can't parse your CNF.\n";
      exit (1);
    }
    cnf_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
  }
};

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";

Relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup (char *catalog_path, char *dbfile_dir, char *tpch_dir) {
  cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
  cout << " catalog location: \t" << catalog_path << endl;
  cout << " tpch files dir: \t" << tpch_dir << endl;
  cout << " heap files dir: \t" << dbfile_dir << endl;
  cout << " \n\n";

  s = new Relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
  ps = new Relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
  p = new Relation (part, new Schema (catalog_path, part), dbfile_dir);
  n = new Relation (nation, new Schema (catalog_path, nation), dbfile_dir);
  li = new Relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
  r = new Relation (region, new Schema (catalog_path, region), dbfile_dir);
  o = new Relation (orders, new Schema (catalog_path, orders), dbfile_dir);
  c = new Relation (customer, new Schema (catalog_path, customer), dbfile_dir);
}

void cleanup () {
  delete s, p, ps, n, li, r, o, c;
}

#endif
