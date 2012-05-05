#include "RelOp.h"
#include <iostream>
#include <sstream>
#include <string>
#include <cassert>

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
      clog << counter/10000 << " ";
    }
    outPipe.Insert(&temp);
  }
  cout << endl << " selected " << counter << " recs \n";

  outPipe.ShutDown();
  clog << "select file ending, after selecting " << counter << " records" << endl;
  pthread_exit(NULL); // make our worker thread go away
}

void SelectFile::WaitUntilDone () {
  clog << "SF waiting til done" << endl;
  pthread_join (SelectFileThread, NULL);
  clog << "SF complete, joined" << endl;
}





void * SelectPipe :: thread_starter(void *context)
{
  return reinterpret_cast<SelectPipe*>(context)->WorkerThread();
}

void * SelectPipe :: WorkerThread(void) {
  clog << "SF worker thread started" << endl;
  Pipe & inPipe = *in;
  Pipe & outPipe = *out;
  CNF & selOp = *cnf;
  Record & literal = *lit;
  int counter = 0;
  Record temp;

  ComparisonEngine comp;

  while (inPipe.Remove(&temp))
    {
      if (SUCCESS == comp.Compare (&temp, &literal, &selOp)) {
        counter += 1;
        if (counter % 10000 == 0) {
          clog << counter/10000 << " ";
        }
        outPipe.Insert(&temp);
      }
    }
  cout << endl << " selected " << counter << " recs \n";

  outPipe.ShutDown();
  clog << "select pipe ending, after selecting " << counter << " records" << endl;
  pthread_exit(NULL); // make our worker thread go away
}

void SelectPipe::WaitUntilDone () {
  clog << "SP waiting til done" << endl;
  pthread_join (SelectPipeThread, NULL);
  clog << "SP complete, joined" << endl;
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
  Type retType = Int;
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
        attr.myType = Int;
        ss << intresult << "|";
      }
    else if (Double == retType) // floating point result
      {
        attr.myType = Double;
        ss << doubleresult << "|";
      }
    Schema retSchema ("out_schema",1,&attr);
    ret.ComposeRecord(&retSchema, ss.str().c_str());
    outPipe.Insert(&ret);
  }
  outPipe.ShutDown();
  clog << "Sum ending, after seeing " << counter << " records." << endl;
  pthread_exit(NULL); // make our worker thread go away
}

void * GroupBy :: thread_starter(void *context)
{
  clog << "starting GroupBy thread" << endl;
  return reinterpret_cast<GroupBy*>(context)->WorkerThread();
}

void * GroupBy :: WorkerThread(void) {
  cout << "GroupBy thread started" << endl;
  Pipe &inPipe = *in;
  Pipe &outPipe = *out;
  OrderMaker & compare = *comp;
  Function &computeMe = *fn;
  clog << runLength << endl;
  Pipe sortedOutput(runLength);
  BigQ sorter(inPipe, sortedOutput, compare, runLength);
  clog << "GB BigQ initialized" << endl;

  // sort everything, do what dupremoval does, but sum over the group rather than deleting it.
  Record recs[2];

  Type retType = Int;
  int intresult = 0;
  double doubleresult = 0.0;
  unsigned int counter = 0;

  if(SUCCESS == sortedOutput.Remove(&recs[1]))
    {
      int tr; double td;
      retType = computeMe.Apply(recs[1],tr,td);
      intresult += tr; doubleresult += td;
      counter++;
    }
  clog << "first removed" << endl;
  unsigned int i = 0;
  ComparisonEngine ceng;

  while(SUCCESS == sortedOutput.Remove(&recs[i%2])) // i%2 is 'current' record
    {
      counter++;
      if (0 == ceng.Compare(&recs[i%2],&recs[(i+1)%2],&compare)) // groups are the same
        { // add to current sum
          int tr; double td;
          computeMe.Apply(recs[i%2],tr,td);
          intresult += tr; doubleresult += td;
        }
      else // groups are different, thus there is a new group
        {
          WriteRecordOut(recs[(i+1)%2],retType, intresult, doubleresult);
          i++; // switch slots.
        }
    }
  WriteRecordOut(recs[i%2],retType, intresult, doubleresult);
  i++;

  outPipe.ShutDown();
  clog << "GroupBy ending, after seeing " << counter << " records, in " << i << "groups." << endl;
  pthread_exit(NULL); // make our worker thread go away
}

void GroupBy :: WriteRecordOut(Record & rec, Type const retType, int & intresult, double & doubleresult) {
  {
    Record ret;
    stringstream ss;
    Attribute attr;
    attr.name = "SUM";
    if (Int == retType)
      {
        attr.myType = Int;
        ss << intresult << "|";
      }
    else if (Double == retType) // floating point result
      {
        attr.myType = Double;
        ss << doubleresult << "|";
      }
    Schema retSchema ("out_schema",1,&attr);
    ret.ComposeRecord(&retSchema, ss.str().c_str());
    // sum as first attribute, group attr as rest
    int RightNumAtts = comp->GetNumAtts();
    int numAttsToKeep = 1 + RightNumAtts;

    int * attsToKeep = (int *)alloca(sizeof(int) * numAttsToKeep);
    { // setup AttsToKeep for MergeRecords
      int curEl = 0;
      attsToKeep[0] = 0;
      curEl++;

      for (int i = 0; i < RightNumAtts; i++)
        {
          attsToKeep[curEl] = (comp->GetWhichAtts())[i];
          curEl++;
        }
    }
    Record newret;
    newret.MergeRecords(&ret,&rec, 1, rec.GetNumAtts(),
                        attsToKeep, numAttsToKeep, 1);
    Pipe &outPipe = *out;
    outPipe.Insert(&newret);
  }
  // reset group total
  intresult = 0;
  doubleresult = 0.0;
}

void * Project :: thread_starter(void *context)
{
  clog << "starting project thread" << endl;
  return reinterpret_cast<Project*>(context)->WorkerThread();
}

void * Project :: WorkerThread(void) {
  clog << "project thread started" << endl;
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
  pthread_exit(NULL); // make our worker thread go away
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
  unsigned int counterL = 0;
  unsigned int counterR = 0;
  unsigned int counterOut = 0;
  OrderMaker sortOrderL;
  OrderMaker sortOrderR;
  bool validOrderMaker = (0 < selOp.GetSortOrders(sortOrderL, sortOrderR));
  if(validOrderMaker)
    {
      clog << "ordermakers are valid, sort-merge join" << endl;
      { // sort merge join
        clog << runLength << endl;
        Pipe outPipeL(runLength);
        Pipe outPipeR(runLength);
        BigQ Left(inPipeL,outPipeL,sortOrderL,runLength);
        BigQ Right(inPipeR,outPipeR,sortOrderR,runLength);
        clog << "BigQs initialized" << endl;
        Record LeftRecord;
        Record RightRecord;
        unsigned int counter = 0;
        clog << "getting first records" << endl;
        outPipeL.Remove(&LeftRecord); // TODO check return value
        clog << "left record" << endl;
        outPipeR.Remove(&RightRecord); // TODO check return value
        clog << "right record" << endl;

        const int LeftNumAtts = LeftRecord.GetNumAtts();
        const int RightNumAtts = RightRecord.GetNumAtts();
        const int NumAttsTotal = LeftNumAtts + RightNumAtts;

        int * attsToKeep = (int *)alloca(sizeof(int) * NumAttsTotal);
        clog << "setup atts" << endl;
        // consider factoring this into a MergeRecords that takes just two records.
        { // setup AttsToKeep for MergeRecords
          int curEl = 0;
          for (int i = 0; i < LeftNumAtts; i++)
            attsToKeep[curEl++] = i;
          for (int i = 0; i < RightNumAtts; i++)
            attsToKeep[curEl++] = i;
        }
        clog << "atts set up" << endl;
        ComparisonEngine ceng;

        vector<Record> LeftBuffer;
        LeftBuffer.reserve(1000);
        vector<Record> RightBuffer;
        RightBuffer.reserve(1000);
        Record MergedRecord;

        do {
          do { // burn records
            if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
              { // pos, left is greater than right.
                // left is greater, right is lesser
                // advance right until 0.
                do {
                  counter++; counterR++;
                  if(FAILURE == outPipeR.Remove(&RightRecord)) // pipe is empty
                    { RightRecord.SetNull(); break; } // set record to null
                }
                while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
              }
            if(0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR))
              {
                // right is greater, left is lesser.
                // advance left until equal
                do {
                  counter++; counterL++;
                  if(FAILURE == outPipeL.Remove(&LeftRecord))
                    { LeftRecord.SetNull(); break; }
                }
                while (0 < ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR));
              }
          } while (0 != ceng.Compare(&LeftRecord,&sortOrderL,&RightRecord,&sortOrderR)); // discard left and right until equal.
          { // do join
            // records are the same
            // fill both left/right buffers until they change.
            // consider using std::async in the future. or omp, but need to make sure to have enough records to join
            //#pragma omp sections
            { // The buffer filling could be done in parallel, as long as we have lots of things from each buffer.
              //#pragma omp section
              FillBuffer( LeftRecord,  LeftBuffer, outPipeL, sortOrderL);
              //#pragma omp section
              FillBuffer(RightRecord, RightBuffer, outPipeR, sortOrderR);
            }
            unsigned int lSize = LeftBuffer.size();
            unsigned int rSize = RightBuffer.size();
            counterL += lSize;
            counterR += rSize;
            counterOut += (lSize * rSize);
            // merge buffers of records. This could also be done in parallel, as long as we had lots of records. probably approx 50k
            // #pragma omp parallel for
            for (unsigned int i = 0; i < lSize; ++i)
              {
                for (unsigned int j = 0; j < rSize; ++j)
                  {
                    MergedRecord.MergeRecords(&LeftBuffer[i],&RightBuffer[j],LeftNumAtts,RightNumAtts,attsToKeep,NumAttsTotal,LeftNumAtts);
                    outPipe.Insert(&MergedRecord);
                  }
              }
            LeftBuffer.clear();
            RightBuffer.clear();
          }
        } while (!LeftRecord.isNull() || !RightRecord.isNull()); // there is something in either pipes
      }
    }
  else
    {clog << "ordermakers are not valid, block nested loop join" << endl;}

  clog << "Join" << endl
       << "read " << counterL << " records from the left"  << endl
       << "read " << counterR << " records from the right" << endl
       << "put  " << counterOut << " records out" << endl;
  outPipe.ShutDown();
  pthread_exit(NULL); // make our worker thread go away
}

void Join :: FillBuffer(Record & in, vector<Record> &buffer, Pipe & pipe, OrderMaker & sortOrder)
{
  ComparisonEngine ceng;
  buffer.push_back(in);
  if (FAILURE == pipe.Remove(&in))
    {
      in.SetNull();
      return;
    }
  while(0 == ceng.Compare(&buffer[0], &in, &sortOrder))
    {
      buffer.push_back(in);
      if (FAILURE == pipe.Remove(&in))
        {
          in.SetNull();
          return;
        }
    }
  return;
}

void * DuplicateRemoval :: thread_starter(void *context)
{
  clog << "starting DuplicateRemoval thread" << endl;
  return reinterpret_cast<DuplicateRemoval*>(context)->WorkerThread();
}

void * DuplicateRemoval :: WorkerThread(void) {
  Pipe& inPipe = *in;
  Pipe& outPipe = *out;

  Pipe sortedOutput(runLength);
  BigQ sorter(inPipe, sortedOutput, compare, runLength);
  clog << "BigQ initialized" << endl;
  Record recs[2];
  unsigned int counter = 0;

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
  unsigned counter = 0;
  while(SUCCESS == inPipe.Remove(&temp))
    {
      counter++;
      ostringstream os;
      temp.Print(sch,os);
      fputs(os.str().c_str(),out);
    }
  clog << "wrote " << counter << " records to file" << endl;
  pthread_exit(NULL); // make our worker thread go away
}
