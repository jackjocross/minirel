#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstring>
#include <iostream>
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
    Status outputStatus;
    HeapFile output(result, outputStatus);

    Status hpfStat, indxHeapStat;

    //The HeapFileScan for the non-indexed relation (attrDesc1), which is determined in join.cpp
    HeapFileScan heapScan(attrDesc1.relName, hpfStat);
    heapScan.startScan(attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), NULL, op);
    
    //The HeapFileScan necessary to do getRandomRecord for the indexed relation
    HeapFileScan indxHeapScan(attrDesc2.relName, indxHeapStat);
    indxHeapScan.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), NULL, op);

    RID curHeapId, curIndxId, outId;
    Record heapRec, indexRec, *insert;

    Status newIndexStat, newIndxScan, getRandStat;

    while (heapScan.scanNext(curHeapId, heapRec) == OK)
    {
        Index indxScan(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), 1, newIndexStat);
        //ERROR CHECK INDX SCAN STATUS
  
        //Getting the value to do an index scan on from the non-indexed record
        void *attrVal = (char*)malloc(attrDesc1.attrLen);
        memcpy(attrVal, heapRec.data + attrDesc1.attrOffset, attrDesc1.attrLen);

        newIndxScan = indxScan.startScan(attrVal);

        while (indxScan.scanNext(curIndxId) == OK)
        {
            getRandStat = indxHeapScan.getRandomRecord(curIndxId, indexRec);

            if (attrDesc2.attrType == 0)
            {

                if (matchRec(heapRec, indexRec, attrDesc1, attrDesc2) == 0)
                {

                    insert = new Record();
                    insert->length = reclen;
                    insert->data = (char*)malloc(reclen);

                    int totalLen = 0;

                    for (int i = 0; i < projCnt; ++i)
                    {
                        string relNameCheck = attrDescArray[i].relName;

                        if (strcmp(attrDescArray[i].relName, attrDesc2.relName) == 0) 
                        {
                            memcpy(insert->data + totalLen, indexRec.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                            totalLen += attrDescArray[i].attrLen;
                        }
                        else
                        {
                            memcpy(insert->data + totalLen, heapRec.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                            totalLen += attrDescArray[i].attrLen;
                        }
                        
                    }

                    output.insertRecord(*insert, outId);

                    delete insert;
                }

            }
            else if (attrDesc2.attrType == 1)
            {

                if (matchRec(heapRec, indexRec, attrDesc1, attrDesc2) == 0)
                {
                    insert = new Record();
                    insert->length = reclen;
                    insert->data = (char*)malloc(reclen);

                    int totalLen = 0;
                    for (int i = 0; i < projCnt; ++i)
                    {
                        memcpy(insert->data + totalLen, indexRec.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                        totalLen += attrDescArray[i].attrLen;
                    }

                    output.insertRecord(*insert, outId);

                    delete insert;
                }

            }
        }

        indxScan.endScan();
    }

    heapScan.endScan();
    indxHeapScan.endScan();

    return OK;
}

