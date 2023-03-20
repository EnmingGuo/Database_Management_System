#ifndef _cli_h_
#define _cli_h_

#include <string>
#include <cstring>
#include <cassert>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <iostream>
#include <unordered_map>

#ifdef __APPLE__
#define uint uint32_t
#define NO_HISTORY_LIST
#endif

#include "src/include/qe.h"

namespace PeterDB {
    typedef enum {
        FILTER = 0, PROJECT, BNL_JOIN, INL_JOIN, GH_JOIN, AGG, IDX_SCAN, TBL_SCAN
    } QUERY_OP;

    // Return code
    typedef int RC;

    struct Table {
        std::string tableName;
        std::vector<PeterDB::Attribute> columns;

    };

// Record Manager
    class CLI {
    public:
        static CLI *Instance();

        RC process(const std::string& input);

        RC start();

    protected:
        CLI();

        ~CLI();

    private:
        // cli parsers
        RC createTable();

        RC createIndex();

        RC createCatalog();

        RC dropTable();

        RC dropIndex(const std::string& tableName = "", const std::string& columnName = "", bool fromCommand = true);

        RC dropCatalog();

        RC addAttribute();

        RC insertTuple();

        RC dropAttribute();

        RC load();

        RC printTable(const std::string& tableName);

        RC printAttributes();

        RC printIndex();

        RC help(const std::string& input);

        RC history();

        // query parsers
        // code [0,4]: operation number
        // code -1: operation not found
        // code -2: don't call isIterator
        PeterDB::Iterator *query(PeterDB::Iterator *previous, int code = -1);

        PeterDB::Iterator *createBaseScanner(const std::string& token);

        PeterDB::Iterator *projection(PeterDB::Iterator *input);

        PeterDB::Iterator *filter(PeterDB::Iterator *input);

        PeterDB::Iterator *blockNestedLoopJoin(PeterDB::Iterator *input);

        PeterDB::Iterator *indexNestedLoopJoin(PeterDB::Iterator *input);

        PeterDB::Iterator *graceHashJoin(PeterDB::Iterator *input);

        PeterDB::Iterator *aggregate(PeterDB::Iterator *input);

        // run the query
        RC run(PeterDB::Iterator *);

        RC createProjectAttributes(const std::string& tableName, std::vector<PeterDB::Attribute> &attrs);

        RC createCondition(const std::string& tableName, PeterDB::Condition &condition, const bool join = false,
                           const std::string joinTable = "");

        RC createAttribute(PeterDB::Iterator *, PeterDB::Attribute &attr);

        RC createAggregateOp(const std::string& operation, PeterDB::AggregateOp &op);

        void addTableNameToAttrs(const std::string& tableName, std::vector<std::string> &attrs);

        bool isIterator(const std::string token, int &code);

        std::string getTableName(PeterDB::Iterator *it);

        std::string getAttribute(const std::string& input);

        std::string fullyQualify(const std::string& attribute, const std::string& tableName);

        // cli catalog functions
        RC getAttributesFromCatalog(const std::string& tableName, std::vector<PeterDB::Attribute> &columns);

        RC getAttribute(const std::string& tableName, const std::string& attrName, PeterDB::Attribute &attr);

        RC addAttributeToCatalog(const PeterDB::Attribute &attr, const std::string& tableName, const int position);

        RC addTableToCatalog(const std::string& tableName, const std::string& file_url, const std::string& type);

        RC addIndexToCatalog(const std::string& tableName, const std::string& indexName);

        // helper functions
        static char *next();

        bool expect(const std::string& token, const std::string& expected);

        bool expect(const char *token, const std::string& expected);

        std::string toLower(std::string input);

        bool checkAttribute(const std::string& tableName, const std::string& columnName, PeterDB::RID &rid,
                            bool searchColumns = true);

        RC error(const std::string& errorMessage);

        RC error(uint errorCode);

        static RC printOutputBuffer(std::vector<std::string> &buffer, uint mod);

        static RC updateOutputBuffer(std::vector<std::string> &buffer, void *data, std::vector<PeterDB::Attribute> &attrs);

        RC
        insertTupleToDB(const std::string& tableName, const std::vector<PeterDB::Attribute>& attributes, const void *data,
                        const std::unordered_map<int, void *>& indexMap);

        RC getAttribute(const std::string& name, const std::vector<PeterDB::Attribute>& pool, PeterDB::Attribute &attr);

        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();
        static CLI *_cli;
    };
}
#endif
