#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>
#include <iostream>
#include "utility.h"
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

	/*cout << "printing out the relation: \n";
	Utilities u;
	u.Print(relation);*/

	Status outputStatus;
	HeapFile output(result, outputStatus);
	Status inStatus;
	HeapFileScan input(relation, inStatus);
	input.startScan(attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), NULL, op);
	//made no difference from when I had filter as a NULL value // 


	Status inputStatus;
	Index indxScan(relation, attrDesc->attrOffset, attrDesc->attrLen, static_cast<Datatype>(attrDesc->attrType), 1, inputStatus);

	Status indexStatus;
	indexStatus = indxScan.startScan(attrValue);


	RID curId;
	RID outId;
	Record rec;
	Record *insert;
	Status recStatus;
	Status scanNextStat;
	Status insertOutputStat;

	while (indxScan.scanNext(curId) == OK) {


		input.getRandomRecord(curId, rec);

		if (attrDesc->attrType == 0) 
		{
			int checkData;
			memcpy(&checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
			int litData;
			memcpy(&litData, attrValue, attrDesc->attrLen);

			//cout << "listData: " << litData << endl;
			//cout << "checkdata: " << checkData << endl;

			if (litData == checkData) 
			{

				insert = new Record();
				insert->data = (char *)malloc(reclen);
				insert->length = reclen;

				int totalLen = 0;

				for (int i = 0; i < projCnt; ++i) 
				{
					memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, 
						projNames[i].attrLen);
					totalLen += projNames[i].attrLen;
				}

				insertOutputStat = output.insertRecord(*insert, outId);
				
				delete insert;

			}
			
		} 
		else if (attrDesc->attrType == 1) 
		{
			double checkData;
			memcpy(&checkData, rec.data + attrDesc->attrOffset, attrDesc->attrLen);
			double litData;
			memcpy(&litData, attrValue, attrDesc->attrLen);

			//cout << "listData: " << litData << endl;
			//cout << "checkdata: " << checkData << endl;

			if (litData == checkData) 
			{

				insert = new Record();
				insert->data = (char *)malloc(reclen);
				insert->length = reclen;

				int totalLen = 0;

				for (int i = 0; i < projCnt; ++i) 
				{
					memcpy(insert->data + totalLen, rec.data + projNames[i].attrOffset, 
						projNames[i].attrLen);
					totalLen += projNames[i].attrLen;
				}

				insertOutputStat = output.insertRecord(*insert, outId);
				
				delete insert;

			}
		} 

		//CHARS ARE NOT INDEXED, SO WE WE ONLY NEED TO CHECK FOR DOUBLES / INTS //
	}

	return OK;
}

