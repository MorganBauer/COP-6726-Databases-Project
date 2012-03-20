#include "RelOp.h"
#include <iostream>
#include <sstream>
#include <string>

void SelectFile::Run (DBFile &inFile_, Pipe &outPipe_, CNF &selOp_, Record &literal_) {
  clog << "select file starting" << endl;
  inF = &inFile_;
  outP = &outPipe_;
  cnf = &selOp_;
  lit = &literal_;
  pthread_create (&SelectFileThread, NULL, &SelectFile::thread_starter, this);
}

void * SelectFile :: thread_starter(void *context)
{
  return reinterpret_cast<SelectFile*>(context)->WorkerThread();
}

void * SelectFile :: WorkerThread(void) {
  clog << "SF worker thread started" << endl;
  DBFile & inFile = *inF;
  Pipe & outPipe = *outP;
  CNF & selOp = *cnf;
  Record & literal = *lit;
  int counter = 0;
  Record temp;
  while (SUCCESS == inFile.GetNext (temp, selOp, literal)) {
    counter += 1;
    if (counter % 10000 == 0) {
      cout << counter << "\n";
    }
    outPipe.Insert(&temp);
  }
  cout << " selected " << counter << " recs \n";

  outPipe.ShutDown();
  clog << "select file ending, after selecting " << counter << " records" << endl;
  pthread_exit(NULL); // make our worker thread go away
}

void SelectFile::WaitUntilDone () {
  clog << "SF waiting til done" << endl;
  pthread_join (SelectFileThread, NULL);
  clog << "SF complete, joined" << endl;
}

void * Sum :: thread_starter(void *context)
{
  clog << "starting sum thread" << endl;
  return reinterpret_cast<Sum*>(context)->WorkerThread();
}

void * Sum :: WorkerThread(void) {
  cout << "Sum thread started" << endl;
  Pipe &inPipe = *in;
  Pipe &outPipe = *out;
  Function &computeMe = *fn;
  Record temp;
  clog << "begin summing" << endl;
  Type retType;
  unsigned int counter = 0;
  while(SUCCESS == inPipe.Remove(&temp))
    {
      counter++;
      retType = computeMe.Apply(temp,integerResult,FPResult);
    }
  clog << "summing complete" << endl;
  // sum complete, take value from function and put into outpipe.
  {
    Record ret;
    stringstream ss;
    Attribute attr;
    attr.name = "SUM";
    if (Int == retType)
      {
        clog << "int result" << endl;
        attr.myType = Int;
        ss << integerResult;
        ss << "|";
      }
    else if (Double == retType) // floating point result
      {
        clog << "double result" << endl;
        attr.myType = Double;
        ss << FPResult;
        ss << "|";
      }
    Schema retSchema ("out_schema",1,&attr);
    ret.ComposeRecord(&retSchema, ss.str().c_str());
    outPipe.Insert(&ret);
  }
  outPipe.ShutDown();
  clog << "Sum ending, after seeing " << counter << " records." << endl;
  pthread_exit(NULL); // make our worker thread go away
}
