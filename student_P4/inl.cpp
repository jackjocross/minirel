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

    if (outputStatus != OK) 
    {
        return outputStatus;
    }

    Status hpfStat, indxHeapStat, heapStartStat, indxStartStat;

    //The HeapFileScan for the non-indexed relation (attrDesc1), which is determined in join.cpp
    HeapFileScan heapScan(attrDesc1.relName, hpfStat);
    if (hpfStat != OK) 
    {
        return hpfStat;
    }

    heapStartStat = heapScan.startScan(attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), NULL, op);
    if (heapStartStat != OK) 
    {
        return heapStartStat;
    }
    
    //The HeapFileScan necessary to do getRandomRecord for the indexed relation
    HeapFileScan indxHeapScan(attrDesc2.relName, indxHeapStat);
    if (indxHeapStat != OK) 
    {
        return indxHeapStat;
    }

    indxStartStat = indxHeapScan.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), NULL, op);
    if (indxStartStat != OK)
    {
        return indxHeapStat;
    } 

    RID curHeapId, curIndxId, outId;
    Record heapRec, indexRec, *insert;

    Status newIndexStat, newIndxScan, getRandStat, getNextHeapStat, getNextIndxStat, insertStat;

    while (1)
    {
        getNextHeapStat = heapScan.scanNext(curHeapId, heapRec);
        if (getNextHeapStat == FILEEOF) 
        {
            break;
        }
        else if (getNextHeapStat != OK) 
        {
            return getNextHeapStat;
        }

        Index indxScan(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), 1, newIndexStat);
        if (newIndexStat != OK)
        {
            return newIndexStat;
        } 
  
        //Getting the value to do an index scan on from the non-indexed record
        void *attrVal = (char*)malloc(attrDesc1.attrLen);
        memcpy(attrVal, heapRec.data + attrDesc1.attrOffset, attrDesc1.attrLen);

        newIndxScan = indxScan.startScan(attrVal);
        if (newIndxScan != OK) 
        {
            return newIndxScan;
        }

        while (1)
        {
            getNextIndxStat = indxScan.scanNext(curIndxId);
            if (getNextIndxStat == NOMORERECS)
            {
                break;
            }
            else if (getNextIndxStat != OK)
            {
                return getNextIndxStat;
            } 

            getRandStat = indxHeapScan.getRandomRecord(curIndxId, indexRec);
            if (getRandStat != OK)
            {
                return getRandStat;
            } 

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

            insertStat = output.insertRecord(*insert, outId);
            if (insertStat != OK) 
            {
                return insertStat;
            }

            free(insert->data);
            delete insert;
        }

        indxScan.endScan();
    }

    heapScan.endScan();
    indxHeapScan.endScan();

    return OK;
}

