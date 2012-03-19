#include "RelOp.h"

void SelectFile::Run (DBFile &inFile_, Pipe &outPipe_, CNF &selOp_, Record &literal_) {
  inF = &inFile_;
  outP = &outPipe_;
  cnf = &selOp_;
  lit = &literal_;
  pthread_create (&selectFileThread, NULL, &SelectFile::thread_starter, this);
}

void * SelectFile :: thread_starter(void *context)
{
  return reinterpret_cast<SelectFile*>(context)->WorkerThread();
}

void * SelectFile :: WorkerThread(void) {
  DBFile & inFile = *inF;
  Pipe & outPipe = *outP;
  CNF & selOp = *cnf;
  Record & literal = *lit;
  int counter = 0;
  Record temp;
  while (inFile.GetNext (temp, selOp, literal) == 1) {
    counter += 1;
    if (counter % 10000 == 0) {
      cout << counter << "\n";
    }
    outPipe.Insert(&temp);
  }
  cout << " selected " << counter << " recs \n";

  outPipe.ShutDown();
  pthread_exit(NULL); // make our worker thread go away
}

void SelectFile::WaitUntilDone () {
  pthread_join (selectFileThread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
  runLength = runlen;
  return;
}
