#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <string>

class RelationalOp {
 protected:
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
  Pipe * in;
  Pipe * out;
  int * atts;
  int numAttsIn;
  int numAttsOut;

  pthread_t ProjectThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
  Project () : in(0), out(0), atts(0), numAttsIn(0), numAttsOut(0) {}
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
          in = &inPipe;
          out = &outPipe;
          atts = keepMe;
          numAttsIn = numAttsInput;
          numAttsOut = numAttsOutput;
          clog << "P pthread create" << endl;
          pthread_create (&ProjectThread, NULL, &Project::thread_starter, this);
        }
	void WaitUntilDone () {  
          clog << "P waiting til done" << endl;
          pthread_join (ProjectThread, NULL);
          clog << "P complete, joined" << endl;
        }
};

class Join : public RelationalOp {
  Pipe * inL;
  Pipe * inR;
  Pipe * out;
  CNF * cnf;


  pthread_t JoinThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
          inL = &inPipeL;
          inR = &inPipeR;
          out = &outPipe;
          cnf = & selOp;
          clog << "Join pthread create" << endl;
          pthread_create (&JoinThread, NULL, &Join::thread_starter, this);
        }
	void WaitUntilDone () {
          clog << "J waiting til done" << endl;
          pthread_join (JoinThread, NULL);
          clog << "J complete, joined" << endl;
        }
};

class DuplicateRemoval : public RelationalOp {
  Pipe * in;
  Pipe * out;
  OrderMaker compare;

  pthread_t DuplicateRemovalThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
          in = &inPipe;
          out = &outPipe;
          compare = OrderMaker(&mySchema);
          pthread_create (&DuplicateRemovalThread, NULL, &DuplicateRemoval::thread_starter, this);
        }
	void WaitUntilDone () { 
          clog << "DR waiting til done" << endl;
          pthread_join (DuplicateRemovalThread, NULL);
          clog << "DR complete, joined" << endl;
        }
};

class Sum : public RelationalOp {
  int integerResult;
  double FPResult;

  Pipe * in;
  Pipe * out;
  Function * fn;
  pthread_t SumThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
 Sum() : integerResult(0),FPResult(0),in(0),out(0),fn(0) {}
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
  pthread_t GroupByThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
};

class WriteOut : public RelationalOp {
  pthread_t WriteOutThread;
  static void *thread_starter(void *context);
  void * WorkerThread(void);
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
};
#endif
