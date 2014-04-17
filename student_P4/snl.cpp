#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstdlib>
#include <cstring>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
	cout << "Algorithm: Simple NL Join" << endl;

	/* Your solution goes here */

	string relation1 = attrDesc1.relName;
	string relation2 = attrDesc2.relName;

	Status outputStatus;
	HeapFile output(result, outputStatus);
	Status input1Status;
	HeapFileScan input1(relation1, input1Status);
	Status input2Status;
	HeapFileScan input2(relation2, input2Status);

	//ERROR CHECK
	if (input1Status != OK)
	{
		error.print(input2Status);
		return input2Status;
	}

	if (input2Status != OK)
	{
		error.print(input2Status);
		return input2Status;
	}

	if (outputStatus != OK)
	{
		error.print(outputStatus);
		return outputStatus;
	}

	RID curIdOne;
	RID curIdTwo;
	RID outRid;
	Record recOne;
	Record recTwo;
	Record *insert;
	Status recStatusOne;
	Status recStatusTwo;
	Status writeStatus;
	Status startOneStatus;
	Status startTwoStatus;
	Status scanOneStatus;
	Status scanTwoStatus;
	string relNameCheck;
	int totalLen;
	Operator scanOp;


	startOneStatus = input1.startScan(attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), NULL, op);
	if (startOneStatus != OK)
	{
		return startOneStatus;
	}

	while (1)
	{
		scanOneStatus = input1.scanNext(curIdOne, recOne);
		if (scanOneStatus == FILEEOF)
		{
			break;
		}
		else if (scanOneStatus != OK)
		{
			return scanOneStatus;
		}

		void* temp = (char*)malloc(attrDesc1.attrLen);
		memcpy(temp, recOne.data + attrDesc1.attrOffset, attrDesc1.attrLen);
		char* tempInsert = static_cast<char*>(temp);

		if (op == LT)
		{
			scanOp = GT;
		}
		else if (op == LTE)
		{
			scanOp = GTE;
		}
		else if (op == GT)
		{
			scanOp = LT;
		}
		else if (op == LTE)
		{
			scanOp = GTE;
		}
		else if (op == EQ)
		{
			scanOp = EQ;
		}
		else if (op == NE)
		{
			scanOp = NE;
		}

		startTwoStatus = input2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), tempInsert, scanOp);
		if (startTwoStatus != OK)
		{
			return startTwoStatus;
		}

		while (1)
		{	
			scanTwoStatus = input2.scanNext(curIdTwo, recTwo);
			if (scanTwoStatus == FILEEOF)
			{
				break;
			}
			else if (scanTwoStatus != OK)
			{
				return scanTwoStatus;
			}

			totalLen = 0;
			insert = new Record();
			insert->data = (char *) malloc(reclen);
			insert->length = reclen;

			for (int i = 0;i < projCnt;++i)
			{
				relNameCheck = attrDescArray[i].relName;

				if (relNameCheck == relation1)
				{
						memcpy(insert->data + totalLen, recOne.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
				}
				else
				{
					memcpy(insert->data + totalLen, recTwo.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
				}

				totalLen += attrDescArray[i].attrLen;
			}

			writeStatus = output.insertRecord(*insert, outRid);
			if (writeStatus != OK)
			{
				return writeStatus;
			}

			free(insert->data);
			delete insert;		
		}
		input2.endScan();
	}
	input1.endScan();

	return OK;
}


