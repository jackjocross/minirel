#include "catalog.h"
#include "query.h"
#include "index.h"
#include <iostream>
#include <cstring>


/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
    AttrDesc *insertAttr;
    Record rec;
    rec.length = 0;
    int insertCnt = attrCnt;
    Status retStatus = attrCat->getRelInfo(relation, insertCnt, insertAttr);
    if (retStatus != OK)
    {
    	error.print(retStatus);
    	return retStatus;
    }

    for (int i = 0;i < attrCnt;++i)
	{
		for (int j = 0;j < attrCnt;++j)
		{
			/*
			if (j == attrCnt)
			{
				return
				CHECK ERRORRRSSSSSSSS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			}
			*/
			string attrOne = insertAttr[i].attrName;
			string attrTwo = attrList[j].attrName;
			if (attrOne == attrTwo)
			{
				rec.length += attrList[i].attrLen;
				memcpy(&rec.data + insertAttr[i].attrOffset, attrList[j].attrValue, attrList[j].attrLen);
				break;
			}
		}
    }

    RID inRid;
    Status insertStatus = attrCat->insertRecord(rec, inRid);

    for (int k = 0;k < attrCnt;++k)
    {
    	if (insertAttr[k].indexed == true)
    	{
    		Status indexInsert = attrCat->insertEntry(VALUE, RID);
    	}
    }



    return OK;
}
