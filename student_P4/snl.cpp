#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstdlib>
#include <cstring>


bool compareVals(int checkData, Operator op)
{
	if ((op == LT) && (checkData < 0))
	{
		return true;
	}
	else if ((op == LTE) && (checkData <= 0))
	{
		return true;
	}
	else if ((op == EQ) && (checkData == 0))
	{
		return true;
	}
	else if ((op == GTE) && (checkData >= 0))
	{
		return true;
	}
	else if ((op == GT) && (checkData > 0))
	{
		return true;
	}
	else if ((op == NE) && (checkData != 0))
	{
		return true;
	}
	else 
	{
		return false;
	}
}

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
	string relNameCheck;
	int totalLen;

	input1.startScan(attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), NULL, op);
	
	while (input1.scanNext(curIdOne, recOne) == OK)
	{
		input2.startScan(attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), NULL, op);

		while (input2.scanNext(curIdTwo, recTwo) == OK)
		{		
			if (attrDesc1.attrType == 0)
			{
				int checkData = matchRec(recOne, recTwo, attrDesc1, attrDesc2);
				
				if (compareVals(checkData, op))
				{
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

					delete insert;
				}
			}
			else if (attrDesc1.attrType == 1)
			{
				int checkData = matchRec(recOne, recTwo, attrDesc1, attrDesc2);
				
				if (compareVals(checkData, op))
				{
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

					delete insert;
				}
			}
			else if (attrDesc1.attrType == 2)
			{ 
				int checkData = matchRec(recOne, recTwo, attrDesc1, attrDesc2);
				
				if (compareVals(checkData, op))
				{
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

					delete insert;
				}
			}
		}

		input2.endScan();
	}

	input2.endScan();

	return OK;
}












