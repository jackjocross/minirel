#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>

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

	string relation = attrDesc->relName;

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

	input.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), NULL, op);

	RID curId;
	Record rec;
	Status recStatus;


	do
	{
		//ERROR CHECK THISSSS
		recStatus = input.getRecord(curId, rec);

		if (attrDesc->attrType == 0)
		{
			int checkData;
			memcpy(checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
			int litData;
			memcpy(litData, attrValue, attrDesc->attrLen);

			if (compareVals(checkData, litData, op))
			{
				//DO RECORD STUFF
			}
		}
		else if (attrDesc->attrType == 1)
		{
			double checkData;
			memcpy(checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
			double litData;
			memcpy(litData, attrValue, attrDesc->attrLen);

			if (compareVals(checkData, litData, op))
			{
				//DO RECORD STUFF
			}
		}
		else if (attrDesc->attrType == 2)
		{
			string checkData;
			memcpy(checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
			string litData;
			memcpy(litData, attrValue, attrDesc->attrLen);

			if (compareVals(checkData, litData, op))
			{
				//DO RECORD STUFF
			}
		}
		

	}
	while (input.scanNext(curId) == OK);

	return OK;
}










