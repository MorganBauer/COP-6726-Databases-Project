#ifndef SORTED_DBFILE_H
#define SORTED_DBFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFileDefs.h"
#include "GenericDBFile.h"
#include "BigQ.h"
#include <string>
/* sub class for dbfile impl, supporting sorted operations*/

class SortedDBFile : public GenericDBFile
{
 private:
  enum rw_mode {reading, writing};
  rw_mode currentRWMode;
  string filepath;
  File f;
  Page curPage;
  off_t curPageIndex;

  int runLength;
  OrderMaker so;
  static const int pipeBufferSize = 100;
  Pipe * toBigQ;
  Pipe * fromBigQ;
  BigQ * bq;

  void MergeDifferential (void);
  void StupidMergeDifferential (void);
 public:
  SortedDBFile ();

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
