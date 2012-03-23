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
  inFile.MoveFirst ();
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
  int intresult = 0;
  double doubleresult = 0.0;
  while(SUCCESS == inPipe.Remove(&temp))
    {
      counter++;
      int tr = 0;
      double td = 0.0;
      retType = computeMe.Apply(temp,tr,td);
      intresult += tr;
      doubleresult += td;
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
        ss << intresult;
        ss << "|";
      }
    else if (Double == retType) // floating point result
      {
        clog << "double result" << endl;
        attr.myType = Double;
        ss << doubleresult;
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


void * Project :: thread_starter(void *context)
{
  clog << "starting project thread" << endl;
  return reinterpret_cast<Project*>(context)->WorkerThread();
}

void * Project :: WorkerThread(void) {
  Pipe &inPipe = *in;
  Pipe &outPipe = *out;
  Record temp;
  unsigned int counter = 0;
  while(SUCCESS == inPipe.Remove(&temp))
    {
      counter++;
      temp.Project(atts, numAttsOut,numAttsIn);
      outPipe.Insert(&temp);
    }
  outPipe.ShutDown();
  clog << "projected " << counter << " records." << endl;

}

void * Join :: thread_starter(void *context)
{
  clog << "starting join thread" << endl;
  return reinterpret_cast<Join*>(context)->WorkerThread();
}

void * Join :: WorkerThread(void) {
  Pipe& inPipeL = *inL;
  Pipe& inPipeR = *inR;
  Pipe& outPipe = *out;
  CNF& selOp = *cnf;
  OrderMaker sortOrderL;
  OrderMaker sortOrderR;
  selOp.GetSortOrders(sortOrderL, sortOrderL);
  runLength = 100;
  Pipe outPipeL(runLength);
  Pipe outPipeR(runLength);
  BigQ left(inPipeL,outPipeL,sortOrderL,runLength);
  BigQ Right(inPipeR,outPipeR,sortOrderR,runLength);
  clog << "BigQs initialized" << endl;

  outPipe.ShutDown();
  pthread_exit(NULL); // make our worker thread go away
}

void * DuplicateRemoval :: thread_starter(void *context)
{
  clog << "starting DuplicateRemoval thread" << endl;
  return reinterpret_cast<DuplicateRemoval*>(context)->WorkerThread();
}

void * DuplicateRemoval :: WorkerThread(void) {
  Pipe& inPipe = *in;
  Pipe& outPipe = *out;

  runLength = 100;
  Pipe sortedOutput(runLength);

  BigQ sorter(inPipe, sortedOutput, compare, runLength);
  clog << "BigQ initialized" << endl;
  Record recs[2];
  unsigned counter = 0;

  if(SUCCESS == sortedOutput.Remove(&recs[1]))
    {
      Record copy;
      copy.Copy(&recs[1]);
      outPipe.Insert(&copy);
      counter++;
    }

  unsigned int i = 0;
  ComparisonEngine ceng;
  while(SUCCESS == sortedOutput.Remove(&recs[i%2]))
    {
      if (0 == ceng.Compare(&recs[i%2],&recs[(i+1)%2],&compare)) // elements are the same
        { // do nothing, put it in the pipe earlier.
        }
      else // elements are different, thus there is a new element.
        {
          counter++;
          Record copy;
          copy.Copy(&recs[i%2]); // make a copy
          outPipe.Insert(&copy); // put it in the pipe.
          i++; // switch slots.
        }
    }

  outPipe.ShutDown();
  pthread_exit(NULL); // make our worker thread go away
}

void * WriteOut :: thread_starter(void *context)
{
  clog << "starting WriteOut thread" << endl;
  return reinterpret_cast<WriteOut*>(context)->WorkerThread();
}

void * WriteOut :: WorkerThread(void) {
  Pipe& inPipe = *in;

  Record temp;
  while(SUCCESS == inPipe.Remove(&temp))
    {
      ostringstream os;
      temp.Print(sch,os);
      fputs(os.str().c_str(),out);
    }
  pthread_exit(NULL); // make our worker thread go away
}
