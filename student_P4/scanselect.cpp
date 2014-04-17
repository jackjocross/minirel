#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include "utility.h"

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
	cout << "Algorithm: File Scan" << endl;

	/**********************************************
	if attrDesc is NULL: basically do the same do while loop but
	don't worry about checking for op to evaluate to true. 

	Just run the record copy while projecting only the 
	attributes which are in projNames
	**********************************************/

	string relation = projNames[0].relName;

	Status outputStatus;
	HeapFile output(result, outputStatus);
	Status inputStatus;
	HeapFileScan input(relation, inputStatus);

	if (inputStatus != OK)
	{
		return inputStatus;
	}

	if (outputStatus != OK)
	{
		return outputStatus;
	}

	RID curId;
	RID outRid;
	Record rec;
	Record *insert;
	Status recStatus;
	Status writeStatus;
	Status scanStatus;
	Status curInputStatus;
	int totalLen;

	if (attrDesc == NULL)
	{
		scanStatus = input.startScan(0, 0, static_cast<Datatype>(0), NULL, op);
	}
	else
	{
		scanStatus = input.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), static_cast<const char*>(attrValue), op);
	}
	
	if (scanStatus != OK)
	{
		return scanStatus;
	}

	while (1)
	{
		curInputStatus = input.scanNext(curId);
		if (curInputStatus == FILEEOF)
		{
			break;
		}
		else if (curInputStatus != OK)
		{
			return curInputStatus;
		}

		totalLen = 0;

		recStatus = input.getRecord(curId, rec);
		if (recStatus != OK)
		{
			return recStatus;
		}
		insert = new Record();
		insert->data = (char *) malloc(reclen);
		insert->length = reclen;

		for (int i = 0;i < projCnt;++i)
		{
			memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
			totalLen += projNames[i].attrLen;
		}

		writeStatus = output.insertRecord(*insert, outRid);
		if (writeStatus != OK)
		{
			return writeStatus;
		}

		free(insert->data);
		delete insert;
	}

	return OK;
}










