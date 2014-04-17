#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>
#include <cstdlib>
#include <iostream>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
                       const attrInfo projNames[],     // List of projection attributes
                       const attrInfo* attr1,          // Left attr in the join predicate
                       const Operator op,              // Predicate operator
                       const attrInfo* attr2)          // Right attr in the join predicate
{
    /* Your solution goes here */

    /* Error checking */

    if (attr1->attrType != attr2->attrType)
    {
        return ATTRTYPEMISMATCH;
    }
    else  if (projCnt == 0)
    {
        return ATTRTYPEMISMATCH;
    }
    else if ((attr1 == NULL) || (attr2 == NULL))
    {
        return ATTRTYPEMISMATCH;
    }

    Status convertStatus;
    Status checkOneStatus;
    AttrDesc checkOne;
    Status checkTwoStatus;
    AttrDesc checkTwo;
    AttrDesc newNames[projCnt];
    int recLen = 0;

    //converts from infos to descs
    for (int i = 0;i < projCnt;++i)
    {
        convertStatus = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, newNames[i]);
        if (convertStatus != OK)
        {
            return convertStatus;
        }
        recLen += newNames[i].attrLen;
    }

    checkOneStatus = attrCat->getInfo(attr1->relName, attr1->attrName, checkOne);
    if (checkOneStatus != OK)
    {
        return checkOneStatus;
    }
    checkTwoStatus = attrCat->getInfo(attr2->relName, attr2->attrName, checkTwo);
    if (checkTwoStatus != OK)
    {
        return checkTwoStatus;
    }

    Status snlStatus, inlStatus, smjStatus;

    //SNL
    if (op != EQ)
    {
        snlStatus = SNL(result, projCnt, newNames, checkOne, op, checkTwo, recLen);
        if (snlStatus != OK)
        {
            return snlStatus;
        }
    }
    //INL
    else if (checkOne.indexed || checkTwo.indexed)
    {
        if (checkOne.indexed && !checkTwo.indexed)
        {
            inlStatus = INL(result, projCnt, newNames, checkTwo, op, checkOne, recLen);
            if (inlStatus != OK)
            {
                return inlStatus;
            }
        }
        else
        {
            inlStatus = INL(result, projCnt, newNames, checkOne, op, checkTwo, recLen);
            if (inlStatus != OK)
            {
                return inlStatus;
            }
        }

    }
    //SMJ
    else
    {
        smjStatus = SMJ(result, projCnt, newNames, checkOne, op, checkTwo, recLen);
        if (smjStatus != OK)
        {
            return smjStatus;
        }
    }

	return OK;

}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : (floatDiff < 0?floor(floatDiff):ceil(floatDiff));

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}