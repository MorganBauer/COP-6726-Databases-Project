#ifndef HEAP_DBFILE_H
#define HEAP_DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFileDefs.h"
#include "GenericDBFile.h"

/* sub class for dbfile impl, supporting linear operations*/

class HeapDBFile : public GenericDBFile
{
  File f;
  Page curPage;
  off_t curPageIndex;

 public:
  HeapDBFile ();

  int Open (char *fpath);
  int Create (char *fpath, fType file_type, void *startup);
  int Close ();

  void Load (Schema &myschema, char *loadpath);

  void MoveFirst ();
  void Add (Record &addme);
  int GetNext (Record &fetchme);
  int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif




