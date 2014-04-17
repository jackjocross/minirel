#include "catalog.h"
#include "query.h"
#include "index.h"
#include <iostream>
#include <cstring>
#include <cstdlib>
#include "utility.h"
#include <set>


/*
 * Inserts a record into the specified relation
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
    AttrDesc *insertAttr = new AttrDesc[attrCnt];
    Record *rec = new Record();
    rec->length = 0;
    int insertCnt = 0;
    int indexComp[attrCnt];
    Status retStatus = attrCat->getRelInfo(relation, insertCnt, insertAttr);

    //I think this will take care if the relation is not correct
    if (retStatus != OK)
    {
        return retStatus;
    }  

    //we have more attributes in insert than exist in relation or we may have dups
    if (attrCnt != insertCnt)
    {
        return ATTRNOTFOUND;
    } 
    
    for (int i = 0; i < attrCnt; ++i)
    {
        rec->length += insertAttr[i].attrLen;
    }

    rec->data = (char *) malloc(rec->length);

    set<string> attrDup;

    for (int i = 0; i < attrCnt; ++i)
    {
        string attrTwo = attrList[i].attrName;
        if (attrDup.find(attrTwo) != attrDup.end())
        {
            return DUPLATTR;
        } 
        else attrDup.insert(attrTwo);
    }

    attrDup.clear();

    for (int i = 0;i < insertCnt;++i)
    {
        for (int j = 0;j < attrCnt;++j)
        {
            string attrOne = insertAttr[i].attrName;
            string attrTwo = attrList[j].attrName;
            
            if (attrOne == attrTwo)
            {
                //if the attrType is not the correct type
                if (insertAttr[i].attrType != attrList[j].attrType)
                {
                    return ATTRTYPEMISMATCH;
                } 

                //if attr length is too long
                if (attrList[j].attrLen > insertAttr[i].attrLen)
                {
                    return ATTRTOOLONG;
                } 

                memcpy(rec->data + insertAttr[i].attrOffset, attrList[j].attrValue, insertAttr[i].attrLen);
                indexComp[i] = j;
                
                attrDup.insert(attrTwo);

                break;
            }
        }
    }

    //if value doesn't exist, or if extra value than in attrList
    if (attrDup.size() != attrCnt)
    {
        return ATTRNOTFOUND;
    } 

    RID inRid;
    Status insertStatus; 
    HeapFile heapInsert(relation, insertStatus);
    if (insertStatus != OK)
    {
        return insertStatus;
    } 

    insertStatus = heapInsert.insertRecord(*rec, inRid);
    if (insertStatus != OK)
    {
        return insertStatus;
    } 


    Status entryStatus;
    Status indexStatus;

    for (int k = 0;k < attrCnt;++k)
    {
        if (insertAttr[k].indexed == true)
        {
            Index insertIndex(relation, insertAttr[k].attrOffset, insertAttr[k].attrLen, static_cast<Datatype>(insertAttr[k].attrType), 0, indexStatus);
            if (indexStatus != OK)
            {
                return indexStatus;
            } 

            entryStatus = insertIndex.insertEntry(attrList[indexComp[k]].attrValue, inRid);
            if (entryStatus != OK)
            {
                return entryStatus;
            } 
        }
    }

    free(rec->data);
    delete[] insertAttr;
    delete rec;

    return OK;
}
