#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <string>

class RelationalOp {
  int runLength;
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	void Use_n_Pages (int n) {runLength = n; return;}
};

class SelectFile : public RelationalOp { 
 private:
  DBFile * inF;
  Pipe * outP;
  CNF * cnf;
  Record * lit;

  pthread_t SelectFileThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
};

class SelectPipe : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
};

class Project : public RelationalOp { 
  int runLength;
  
  pthread_t ProjectThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
          // loop for numattsOut over Keepme
        }
	void WaitUntilDone () { }
};

class Join : public RelationalOp { 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
};

class DuplicateRemoval : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { }
	void WaitUntilDone () { }
};

class Sum : public RelationalOp {
  int runLength;
  int integerResult;
  double FPResult;

  Pipe * in;
  Pipe * out;
  Function * fn;
  pthread_t SumThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
 Sum() :runLength(0), integerResult(0),FPResult(0),in(0),out(0),fn(0) {}
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 
          in = &inPipe;
          out = &outPipe;
          fn = &computeMe;
          clog << "SUM pthread create" << endl;
          pthread_create (&SumThread, NULL, &Sum::thread_starter, this);
        }
	void WaitUntilDone () {
          clog << "Sum waiting" << endl;
          pthread_join (SumThread, NULL);
          clog << "Sum complete, joined" << endl;
        }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
};
#endif
