#include "catalog.h"
#include "query.h"
#include "index.h"
#include <iostream>
#include <cstring>
#include <cstdlib>
#include "utility.h"


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
    AttrDesc *insertAttr = new AttrDesc[attrCnt];
    Record *rec = new Record();
    rec->length = 0;
    int insertCnt = attrCnt;
    Status retStatus = attrCat->getRelInfo(relation, insertCnt, insertAttr);

    if (retStatus != OK)
    {
    	error.print(retStatus);
    	return retStatus;
    }

    for (int i = 0; i < attrCnt; ++i)
    {
        rec->length += insertAttr[i].attrLen;
    }

    rec->data = (char *) malloc(rec->length);

    if (rec->data == NULL) 
        cout << "data is null\n";

    for (int i = 0;i < attrCnt;++i)
	{
		for (int j = 0;j < attrCnt;++j)
		{
			//CHECK ERRORS

			string attrOne = insertAttr[i].attrName;
			string attrTwo = attrList[j].attrName;

			if (attrOne == attrTwo)
			{
               
				memcpy(rec->data + insertAttr[i].attrOffset, attrList[j].attrValue, insertAttr[i].attrLen);
				break;
			}
		}
    }

    RID inRid;
    Status insertStatus; 
    HeapFile heapInsert(relation, insertStatus);
    insertStatus = heapInsert.insertRecord(*rec, inRid);
    //Utilities u;
    //u.Print(relation);
    

    /*for (int k = 0;k < attrCnt;++k)
    {
    	if (insertAttr[k].indexed == true)
    	{
    		Status indexInsert = attrCat->insertEntry(VALUE, RID);
    	}
    }*/

    delete [] insertAttr;
    delete rec;

    return OK;
}
