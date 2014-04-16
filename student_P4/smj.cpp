#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cstring>
#include <cstdlib>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: SM Join" << endl;

    /* Your solution goes here */

    /*-----SETTING UP SORTED FILES-------*/
    unsigned unpinned = bufMgr->numUnpinnedPages();
    unsigned sortPages = unpinned * .8;

    int leftCount;
    AttrDesc* leftAttrs;
    Status leftInfo = attrCat->getRelInfo(attrDesc1.relName, leftCount, leftAttrs);

    int leftTupleSize = 0;

    for (int i = 0;i < leftCount;++i)    
    {
        leftTupleSize += leftAttrs[i].attrLen;
    }

    int rightCount;
    AttrDesc* rightAttrs;
    Status rightInfo = attrCat->getRelInfo(attrDesc2.relName,rightCount, rightAttrs);
    int rightTupleSize = 0;

    for (int i = 0; i< rightCount; ++i)
    {
        rightTupleSize += rightAttrs[i].attrLen;
    }

    int leftTuples = 1000 / leftTupleSize;
    int rightTuples = 1000 / rightTupleSize;

    int leftMax = leftTuples * sortPages;
    int rightMax = rightTuples * sortPages;

    Status leftSortStat, rightSortStat;
    SortedFile leftFile(attrDesc1.relName, attrDesc1.attrOffset, attrDesc1.attrLen, static_cast<Datatype>(attrDesc1.attrType), leftMax, leftSortStat);
    SortedFile rightFile(attrDesc2.relName, attrDesc2.attrOffset, attrDesc2.attrLen, static_cast<Datatype>(attrDesc2.attrType), rightMax, rightSortStat);

    /*-----MERGING SORTED FILES----------*/

    Status outputStat, leftRecStat, rightRecStat, rightMarkStat;
    HeapFile output(result, outputStat);

    Record curLeft, oldLeft, curRight, oldRight;

    leftRecStat = leftFile.next(curLeft);
    oldLeft.data = curLeft.data;
    oldLeft.length = curLeft.length;

    rightRecStat = rightFile.next(curRight);
    oldRight.data = curRight.data;
    oldRight.length = curRight.length;

    rightMarkStat = rightFile.setMark();
    
    int totalLen;
    Status writeStatus;
    Record *insert;
    RID outRid;

    while (1)
    {
        if (matchRec(curLeft, curRight, attrDesc1, attrDesc2) == 0)
        {
            //Add to output
            totalLen = 0;
            insert = new Record();
            insert->data = (char *) malloc(reclen);
            insert->length = reclen;

            for (int i = 0;i < projCnt;++i)
            {
                if (strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0)
                {
                    memcpy(insert->data + totalLen, curLeft.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                }
                else
                {
                    memcpy(insert->data + totalLen, curRight.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                }

                totalLen += attrDescArray[i].attrLen;
            }

            writeStatus = output.insertRecord(*insert, outRid);

            delete insert;

            //incrementing right
            oldRight.data = curRight.data;
            oldRight.length = curRight.length;
            rightRecStat = rightFile.next(curRight);
            if (rightRecStat == FILEEOF)
            {
                rightFile.gotoMark();
            }
            else if (matchRec(curRight, oldRight, attrDesc2, attrDesc2) == 0)
            {
                rightFile.setMark();
            }
        }
        else if (matchRec(curLeft, curRight, attrDesc1, attrDesc2) != 0)
        {
            //incrementing left
            oldLeft.data = curLeft.data;
            oldLeft.length = curLeft.length;
            leftRecStat = leftFile.next(curLeft);
            if (leftRecStat == FILEEOF)
            {
                break;
            }

            //checking cur and old left
            if (matchRec(curLeft, oldLeft, attrDesc1, attrDesc1) == 0)
            {
                rightFile.gotoMark();
            }

        }

    }
    
    return OK;
}

