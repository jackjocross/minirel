#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstring>
#include <iostream>
#include "utility.h"
#include <cstdlib>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
  cout << "Algorithm: Indexed NL Join" << endl;

  /* Your solution goes here */
  Status outputStat;
  HeapFile output(result, outputStat);

  cout << "outputStat: " << outputStat << endl;

  if (attrDesc1.indexed) 
  {

  	Status heapStatus;
  	HeapFileScan heap(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), NULL, op, heapStatus);
  	cout << "heapfile scan status : " << heapStatus << endl;


  	Status heapStartScanStatus; 
  	heapStartScanStatus = heap.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), NULL, op);
  	cout << "hpfs scan start status : " << heapStartScanStatus << endl; 


  	RID heapId;
  	RID indexId;
  	RID outRid;

  	Record heapRec;
  	Record indxRec;
  	Record *insert;
  	int totalLen = 0;

	Status heapRecStat;
	Status indxRecStat;
	Status indxStrtScan;
	Status writeStatus;


  	while (heap.scanNext(heapId) == OK) 
  	{
  		Status indxStatus;
  		Index index(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), 1, indxStatus);
  		cout << "indexStatus: " << indxStatus << endl;


  		


  		heapRecStat = heap.getRandomRecord(heapId, heapRec);
  		cout << "heapRecStat: " << heapRecStat << endl;

  		void *attrVal;

  		memcpy(attrVal, heapRec.data + attrDesc2.attrOffset, attrDesc2.attrLen);

  		indxStrtScan = index.startScan(attrVal);
  		

  		cout << "index next scan: " << index.scanNext(indexId) << endl;

  		while (index.scanNext(indexId) == OK)
  		{
  			indxRecStat = heap.getRandomRecord(indexId, indxRec);
  			cout << "indxRecStat: " << indxRecStat << endl;
  			
  			if (attrDesc1.attrType == 0)
  			{
  				int indxCheck; 
  				memcpy(&indxCheck, indxRec.data + attrDesc1.attrOffset, attrDesc1.attrLen);
  				int heapCheck; 
  				memcpy(&heapCheck, heapRec.data + attrDesc2.attrOffset, attrDesc2.attrLen);

  				cout << "index data check: " << indxCheck << endl;
  				cout << "heap data check: " << heapCheck << endl;

  				if (indxCheck == heapCheck)
  				{
  					insert = new Record();
  					insert->data = (char *)malloc(reclen);
  					insert->length = reclen;
  					

  					for (int i = 0; i < projCnt; ++i)
  					{
  						memcpy(insert->data + totalLen, indxRec.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
  						totalLen += attrDescArray[i].attrLen;
  					}

  					writeStatus = output.insertRecord(*insert, outRid);

  					delete insert;

  				}
  			}
  		}
  	}


  }
  else 
  {

  }


  return OK;
}

