#include "BigQ.h"
// #include "RecordSorter.h"
#include <vector>
#include <cstdlib>
#include <iostream>

struct sorter : public std::binary_function<Record *, Record *, bool>
{
  OrderMaker * _so;
public:
  sorter(OrderMaker so) {this->_so = &so;}
  bool operator()(Record & _x, Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(&_x, &_y, _so) < 0) ? true : false; }
  bool operator()(const Record & _x, const Record & _y) { ComparisonEngine comp;
    return  (comp.Compare(const_cast<Record *>(&_x), const_cast<Record *>(&_y), _so) < 0) ? true : false; }
  bool operator()(Record * _x, Record * _y) { ComparisonEngine comp;
    return  (comp.Compare((_x), (_y), _so) < 0) ? true : false; }
};

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
  // ComparisonEngine comp;
  // comp.Compare(&temp,&temp,&sortorder);

  // FIRST PHASE
  // read data from in pipe sort them into runlen pages
  vector<Record> runlenrecords;
  // proof of concept, simplest thing that could possibly work
  // for completely broken values of work
  // this will need (runlen+1 pages)+1 record of memory,
  //  because we hang on to stuff after shoving a copy in a vector
  Page p;
  Record tempRecord;
  int pageCounter = 0;
  while (1 == in.Remove(&tempRecord)) // while we can take records out of the pipe, do so.
    {
      Record copy; // Append consumes the record, so I need to make a copy.
      copy.Copy(&tempRecord); // make the copy
      if (1 == p.Append(&tempRecord)) // page has room
        {
          runlenrecords.push_back(copy); // put it in our runlen sized buffer.
        }
      else // page is full
        {
          pageCounter++; // increment page count, because we have a new page that we just filled.
          // remember to add the page we got, but could not place in a page
          p.EmptyItOut(); // erase the page contents.
          { // put the record in the fresh page.
            int pageFit = p.Append(&tempRecord); // put our page we couldn't fit in.
            if (0 == pageFit)
              {
                exit(-1);
              }
          }
          if ( pageCounter == runlen ) // if it's larger than runlen, we need to stop.
            {
              sorter s = sorter(sortorder);
              // sort the records we have in the runlen buffer.
              cout << "sorting run " << endl; 
              std::sort(runlenrecords.begin(),
                        runlenrecords.end(),
                        sorter(sortorder));
              cout << "run sorted " << endl;
              // write them out to disk. temp file somewhere?
              pageCounter = 0;// reset page counter
            }
          runlenrecords.clear(); // reset the temp vector buffer thing
          runlenrecords.push_back(copy);// put the new record in
        }
    }
  // we've taken all the records out of the pipe
  // do one last internal sort, on the the buffer that we have

  {

  }


  // SECOND PHASE


  // construct priority queue over sorted runs and dump sorted data
  // into the out pipe
  // out.Insert(&temp);

  // finally shut down the out pipe
  out.ShutDown ();
}

BigQ::~BigQ () {
}
