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

//Returns true if data1 <op> data2 evaluates to true, false otherwise
template <typename T>
bool compareVals(T data1, T data2, Operator op)
{

	if (op == LT)
	{
		if (data1 < data2)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	else if (op == LTE)
	{
		if (data1 <= data2)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	else if (op == EQ)
	{
		if (data1 == data2)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	else if (op == GTE)
	{
		if (data1 >= data2)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	else if (op == GT)
	{
		if (data1 > data2)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	else if (op == NE)
	{
		if (data1 != data2)
		{
			return true;
		}
		else 
		{
			return false;
		}
	}
	else 
	{
		return true;
	}
	
}

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

	//ERROR CHECK
	if (inputStatus != OK)
	{
		error.print(inputStatus);
		return inputStatus;
	}

	if (outputStatus != OK)
	{
		error.print(outputStatus);
		return outputStatus;
	}

	RID curId;
	RID outRid;
	Record rec;
	Record *insert;
	Status recStatus;
	Status writeStatus;
	int totalLen;

	if (attrDesc == NULL)
	{
		input.startScan(0, 0, static_cast<Datatype>(0), NULL, op);

		while (input.scanNext(curId) == OK)
		{
			totalLen = 0;

			recStatus = input.getRecord(curId, rec);
			insert = new Record();
			insert->data = (char *) malloc(reclen);
			insert->length = reclen;

			for (int i = 0;i < projCnt;++i)
			{
				memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
				totalLen += projNames[i].attrLen;
			}

			writeStatus = output.insertRecord(*insert, outRid);

			delete insert;
		}
	}
	else 
	{
		input.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), static_cast<const char*>(attrValue), op);

		while (input.scanNext(curId) == OK)
		{
		//ERROR CHECK THISSSS
			recStatus = input.getRecord(curId, rec);

			if (attrDesc->attrType == 0)
			{
				int checkData;
				memcpy(&checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
				int litData;
				memcpy(&litData, attrValue, attrDesc->attrLen);

				if (compareVals(checkData, litData, op))
				{
					totalLen = 0;

					recStatus = input.getRecord(curId, rec);
					insert = new Record();
					insert->data = (char *) malloc(reclen);
					insert->length = reclen;

					for (int i = 0;i < projCnt;++i)
					{
 						memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
						totalLen += projNames[i].attrLen;
					}

					writeStatus = output.insertRecord(*insert, outRid);

					delete insert;
				}
			}
			else if (attrDesc->attrType == 1)
			{
				double checkData;
				memcpy(&checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
				double litData;
				memcpy(&litData, attrValue, attrDesc->attrLen);

				if (compareVals(checkData, litData, op))
				{
					totalLen = 0;

					recStatus = input.getRecord(curId, rec);
					insert = new Record();
					insert->data = (char *) malloc(reclen);
					insert->length = reclen;

					for (int i = 0;i < projCnt;++i)
					{
						memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
						totalLen += projNames[i].attrLen;
					}

					writeStatus = output.insertRecord(*insert, outRid);

					delete insert;
				}
			}
			else if (attrDesc->attrType == 2)
			{ 

				char checkD[attrDesc->attrLen];
				memcpy(&checkD, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
				char litD[attrDesc->attrLen];
				memcpy(&litD, attrValue, attrDesc->attrLen);

				string checkData = checkD;
				string litData = litD;

				if (compareVals(checkData, litData, op))
				{

					totalLen = 0;

					recStatus = input.getRecord(curId, rec);
					insert = new Record();
					insert->data = (char *) malloc(reclen);
					insert->length = reclen;

					for (int i = 0;i < projCnt;++i)
					{
						memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, projNames[i].attrLen);
						totalLen += projNames[i].attrLen;
					}

					writeStatus = output.insertRecord(*insert, outRid);

					delete insert;
				}
			}
		}
	}

	return OK;
}










