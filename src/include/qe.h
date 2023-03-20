#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <stdio.h>

#include "rm.h"
#include "rbfm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    typedef struct Unit{
        float sum;
        int count;
    } Unit;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override = default;

        RC getNextTuple(void *data) override;

        bool isSatisfy(char *data, const std::vector<Attribute> &attrs, const Condition& cond);

        bool isCompareSatisfy(void* key);
        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator* input;
        Condition condition;
        std::vector<Attribute> attributes;
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override = default;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

    private:
        Iterator* input;
        std::vector<std::string> attrNames;
        int findAttributePosition(std::string attrName);
        std::vector<Attribute> attributes;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        void fillLeftBuffer();

        int checkRecordSize(const void* record);

        int correspondingVectorSize(const void* key,AttrType attrType);

        void BNLGiveResult(const void* key, void* result);
    private:
        Iterator* leftIn;
        TableScan* rightIn;
        Condition condition;
        bool leftFinished;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        Attribute leftAttribute;
        Attribute rightAttribue;
        unsigned int maxSize = 0, remainSize = 0;
        unsigned int singlePosibileSize = 0;
        char* currentRightData = (char*) calloc(PAGE_SIZE, sizeof(char*));
        int pointer;
        std::map<int, std::vector<std::vector<char>>> intMap;
        std::map<float, std::vector<std::vector<char>>> floatMap;
        std::map<std::string, std::vector<std::vector<char>>> varcharMap;

    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator* leftIn;
        IndexScan* rightIn;
        Condition condition;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        char* currentLeftData;
        int currentLeftStatus = 0;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override = default;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        void aggreCal(const char* key, AttrType attrType,Unit & unit, const AggregateOp op);

        void giveResult(void* result,const Unit &unit, const AggregateOp op ,AttrType attrType) ;

        void giveResult(void* result, const Unit&unit,const Value value,const AggregateOp op ,AttrType attrType);

        void groupScan();
    private:
        Iterator* input;
        Attribute aggAttr;
        Attribute groupAttr;
        std::vector<Attribute> attributes;
        bool single = true;
        bool finished = false;
        int groupCount;
        Unit singleUnit;
        AggregateOp op;
        std::map<int,Unit> intUnitHash;
        std::map<float,Unit> floatUnitHash;
        std::map<std::string,Unit> stringUnitHash;
        template<typename T>
        void prepareHash(const void* key,std::map<T,Unit>&TUnitHash,std::map<std::string,Unit>&stringUnitHash,AttrType groupAttrType,AggregateOp op);
        void setSum(Unit & unit,AggregateOp op){
            unit.count = 0;
            unit.sum = 0;
            if(op==MIN){
                unit.sum=std::numeric_limits<float>::max();
            }
            else if(op==MAX){
                unit.sum=std::numeric_limits<float>::min();
            }
        }
    };

    class MyJoin {
    public:
        static bool isSatisfy(void* leftData, void* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs, const Condition& cond);
        static AttrType findKey(void* key, void* data, const std::vector<Attribute>& attrs, const std::string name);
        static bool isCompareSatisfy(void* leftData, void* rightData,CompOp op,AttrType attrType);
        static void join(char* result, char* leftData, char* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs);
    };
} // namespace PeterDB

#endif // _qe_h_
