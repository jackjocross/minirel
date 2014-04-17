#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>
#include <iostream>
#include <cstdlib>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
	cout << "Algorithm: Index Select" << endl;

	/* Your solution goes here */

	string relation = attrDesc->relName;

	Status outputStatus;
	HeapFile output(result, outputStatus);
	if (outputStatus != OK)
	{
		return outputStatus;
	}

	Status inStatus;
	HeapFileScan input(relation, inStatus);
	if (inStatus != OK)
	{
		return inStatus;
	} 

	Status inStartScan = input.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), NULL, op);
	if (inStartScan != OK)
	{
		return inStartScan;
	} 

	Status inputStatus;
	Index indxScan(relation, attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), 1, inputStatus);
	if (inputStatus != OK)
	{
		return inputStatus;
	} 

	Status indexStatus;
	indexStatus = indxScan.startScan(attrValue);
	if (indexStatus != OK)
	{
		return indexStatus;
	}

	RID curId, outId;
	Record rec, *insert;
	Status recStatus, scanNextStat, insertOutputStat, indxScanNextStat, getRandStat;

	while (1) {

		indxScanNextStat = indxScan.scanNext(curId);
		if (indxScanNextStat == NOMORERECS) 
		{
			break;
		}
		else if (indxScanNextStat != OK) 
		{
			return indxScanNextStat;
		}
		getRandStat = input.getRandomRecord(curId, rec);
		if (getRandStat != OK)
		{
			return getRandStat;
		} 

		insert = new Record();
		insert->data = (char *)malloc(reclen);
		insert->length = reclen;

		int totalLen = 0;

		for (int i = 0; i < projCnt; ++i) 
		{
			memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
			totalLen += projNames[i].attrLen;
		}

		insertOutputStat = output.insertRecord(*insert, outId);
		if (insertOutputStat != OK)
		{
			return insertOutputStat;
		} 
		
		free(insert->data);
		delete insert;


		//CHARS ARE NOT INDEXED, SO WE WE ONLY NEED TO CHECK FOR DOUBLES / INTS //
	}

	return OK;
}

