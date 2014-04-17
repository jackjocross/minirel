#include "catalog.h"
#include "query.h"
#include "index.h"
#include <iostream>
#include <string>

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
	Status selectFn; 
	Status checkStatus;
	Status convertStatus;
	AttrDesc newNames[projCnt];
	int recLen = 0;
	AttrDesc check; 

	if (op == NOTSET)
	{
		//turning infos into descs
		for (int i = 0; i < projCnt; ++i) 
		{
			convertStatus = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, newNames[i]);
			if (convertStatus != OK)
			{
				return convertStatus;
			} 
			recLen += newNames[i].attrLen;

			string attrName = projNames[i].attrName;
		}

		selectFn = ScanSelect(result, projCnt, newNames, NULL, op, NULL, recLen);
		if (selectFn != OK)
		{
			return selectFn;
		} 
	}
	else
	{	
		checkStatus = attrCat->getInfo(attr->relName, attr->attrName, check);
		if (checkStatus != OK)
		{
			return checkStatus;
		} 

		for (int i = 0; i < projCnt; ++i) 
		{
			convertStatus = attrCat->getInfo(attr->relName, projNames[i].attrName, newNames[i]);
			if (convertStatus != OK) 
			{
				return convertStatus;
			}
			recLen += newNames[i].attrLen;

			string attrName = projNames[i].attrName;

		}

		if ((check.indexed) && (op == EQ))
		{
			selectFn = IndexSelect(result, projCnt, newNames, &check, op, attrValue, recLen);
			if (selectFn != OK)
			{
				return selectFn;
			} 
		}
		else 
		{
			selectFn = ScanSelect(result, projCnt, newNames, &check, op, attrValue, recLen);
			if (selectFn != OK)
			{
				return selectFn;
			} 
		}
	}

return OK;
}

