#ifndef _qetest_util_h_
#define _qetest_util_h_

#include <fstream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unordered_map>

#include "src/include/qe.h"
#include "general_test_utils.h"
#include "test/utils/rm_test_util.h"

namespace PeterDBTesting {

    class QE_Test : public ::testing::Test {
    protected:
        std::unordered_map<std::string, std::vector<PeterDB::Attribute >> attrsMap{
                {"left",         {
                                         {"A", PeterDB::TypeInt,     4},
                                         {"B", PeterDB::TypeInt,     4},
                                         {"C", PeterDB::TypeReal, 4},
                                 }
                },
                {"right",        {
                                         {"B", PeterDB::TypeInt,     4},
                                         {"C", PeterDB::TypeReal,    4},
                                         {"D", PeterDB::TypeInt,  4}
                                 }
                },
                {"leftvarchar",  {
                                         {"A", PeterDB::TypeInt,     4},
                                         {"B", PeterDB::TypeVarChar, 30}
                                 }
                },
                {"rightvarchar", {

                                         {"B", PeterDB::TypeVarChar, 30},
                                         {"C", PeterDB::TypeReal,    4}
                                 }
                },
                {"group",        {
                                         {"A", PeterDB::TypeInt,     4},
                                         {"B", PeterDB::TypeInt,     4},
                                         {"C", PeterDB::TypeReal, 4},
                                 }
                }
        };

        std::vector<std::string> tableNames;

        PeterDB::FileHandle fileHandle;
        size_t bufSize = 100;
        void *inBuffer = nullptr, *outBuffer = nullptr;
        unsigned char *nullsIndicator = nullptr;
        unsigned char *nullsIndicatorWithNull = nullptr;
        bool destroyFile = true;
        PeterDB::RID rid;
        std::vector<PeterDB::Attribute> attrs;

    public:

        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();
        PeterDB::IndexManager &ix = PeterDB::IndexManager::instance();

        void SetUp() override {
            // Remove any leftover index file.
            for (const std::string &indexFileName : glob(".idx")) {
                remove(indexFileName.c_str());
            }

            // Try to delete the System Catalog.
            // If this is the first time, it will generate an error. It's OK and we will ignore that.
            rm.deleteCatalog();

            // Create Catalog
            ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            if (destroyFile) {
                for (const std::string &tableName: tableNames) {
                    // Destroy the file
                    ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should succeed.";
                }

                ASSERT_EQ(glob(".idx").size(), 0) << "There should be no index file now.";

                // Delete Catalog
                ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";
            }

        }

        void createAndPopulateTable(const std::string &tableName, std::vector<std::string> indexAttrs,
                                    const unsigned &tupleCount) {
            // Create a table
            ASSERT_EQ(rm.createTable(tableName, attrsMap[tableName]), success)
                                        << "Create table " << tableName << " should succeed.";

            tableNames.emplace_back(tableName);

            size_t currentIndexCnt = glob(".idx").size();

            if (!indexAttrs.empty())
                // Create an index before inserting tuples.
                ASSERT_EQ(rm.createIndex(tableName, indexAttrs[0]), success)
                                            << "RelationManager.createIndex() should succeed.";

            populateTable(tableName, tupleCount);

            for (auto attrName = indexAttrs.begin() + 1; attrName < indexAttrs.end(); attrName++) {

                // Create index after inserting tuples - should reflect the currently existing tuples.
                ASSERT_EQ(rm.createIndex(tableName, *attrName), success)
                                            << "RelationManager.createIndex() should succeed.";
            }
            ASSERT_EQ(glob(".idx").size() - currentIndexCnt, indexAttrs.size())
                                        << "There should some new index files now.";

        }

        // Prepare the tuple to left table in the format conforming to Insert/Update/ReadTuple and readAttribute
        void prepareLeftTuple(unsigned char *nullAttributesIndicator, const unsigned &seed, void *buf) {

            // Prepare the tuple data for insertion
            unsigned a = seed % 203;
            unsigned b = (seed + 10) % 197;
            float c = (float) (seed % 167) + 50.5f;

            int offset = 0;

            // Null-indicators
            bool nullBit;
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrsMap["left"].size());

            // Null-indicator for the fields
            memcpy((char *) buf + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

            // Is the A field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

            if (!nullBit) {
                memcpy((char *) buf + offset, &a, sizeof(int));
                offset += sizeof(int);
            }

            // Is the B field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

            if (!nullBit) {
                memcpy((char *) buf + offset, &b, sizeof(int));
                offset += sizeof(int);
            }

            // Is the C field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

            if (!nullBit) {
                memcpy((char *) buf + offset, &c, sizeof(float));
            }
        }

        // Prepare the tuple to right table in the format conforming to Insert/Update/ReadTuple, readAttribute
        void prepareRightTuple(unsigned char *nullAttributesIndicator, const unsigned &seed, void *buf) {
            int offset = 0;
            unsigned b = seed % 251 + 20;
            float c = (float) (seed % 261) + 25.5f;
            unsigned d = seed % 179;
            // Null-indicators
            bool nullBit;
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrsMap["right"].size());

            // Null-indicator for the fields
            memcpy((char *) buf + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

            // Is the B field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 7u);

            if (!nullBit) {
                memcpy((char *) buf + offset, &b, sizeof(int));
                offset += sizeof(int);
            }

            // Is the C field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 6u);

            if (!nullBit) {
                memcpy((char *) buf + offset, &c, sizeof(float));
                offset += sizeof(float);
            }


            // Is the D field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 5u);

            if (!nullBit) {
                memcpy((char *) buf + offset, &d, sizeof(unsigned));
            }

        }

        void prepareLeftVarCharTuple(unsigned char *nullAttributesIndicator, const unsigned &seed, void *buf) {
            unsigned a = seed + 20;

            unsigned length = (seed % 26) + 1;
            std::string b = std::string(length, '\0');
            for (int j = 0; j < length; j++) {
                b[j] = 96 + length;
            }

            int offset = 0;

            // Null-indicators
            bool nullBit;
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrsMap["leftvarchar"].size());

            // Null-indicator for the fields
            memcpy((char *) buf + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

            // Is the A field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 7u);
            if (!nullBit) {
                memcpy((char *) buf + offset, &a, sizeof(unsigned));
                offset += sizeof(unsigned);
            }

            // Is the B field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 6u);
            if (!nullBit) {
                memcpy((char *) buf + offset, &length, sizeof(unsigned));
                offset += sizeof(unsigned);
                memcpy((char *) buf + offset, b.c_str(), length);
            }

        }

        void prepareRightVarCharTuple(unsigned char *nullAttributesIndicator, const unsigned &seed, void *buf) {
            float c = seed + 28.2f;

            unsigned length = (seed % 26) + 1;
            std::string b = std::string(length, '\0');
            for (int j = 0; j < length; j++) {
                b[j] = 96 + length;
            }

            int offset = 0;

            // Null-indicators
            bool nullBit;
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrsMap["rightvarchar"].size());

            // Null-indicator for the fields
            memcpy((char *) buf + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

            // Is the B field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 7u);
            if (!nullBit) {
                memcpy((char *) buf + offset, &length, sizeof(unsigned));
                offset += sizeof(unsigned);
                memcpy((char *) buf + offset, b.c_str(), length);
                offset += length;
            }

            // Is the C field not-NULL?
            nullBit = nullAttributesIndicator[0] & (1u << 6u);
            if (!nullBit) {
                memcpy((char *) buf + offset, &c, sizeof(float));
                offset += sizeof(float);
            }

        }

        void prepareGroupTable(unsigned char *nullAttributesIndicator, const unsigned &seed, void *buf) {
            // Prepare the tuple data for insertion
            unsigned a = seed % 5 + 1;
            unsigned b = seed % 5 + 1;
            float c = seed + 50.3;

            int offset = 0;

            // Null-indicators
            bool nullBit;
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrsMap["group"].size());

            // Null-indicator for the fields
            memcpy((char *) buf + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

            // Is the A field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

            if (!nullBit) {
                memcpy((char *) buf + offset, &a, sizeof(int));
                offset += sizeof(int);
            }

            // Is the B field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

            if (!nullBit) {
                memcpy((char *) buf + offset, &b, sizeof(int));
                offset += sizeof(int);
            }

            // Is the C field not-NULL?
            nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

            if (!nullBit) {
                memcpy((char *) buf + offset, &c, sizeof(float));
            }

        }

        void populateTable(const std::string &tableName, const unsigned &tupleCount) {

            // GetAttributes
            ASSERT_EQ(rm.getAttributes(tableName, attrs), success)
                                        << "RelationManager::getAttributes() should succeed.";

            // Initialize a NULL field indicator
            nullsIndicator = initializeNullFieldsIndicator(attrs);

            for (int i = 0; i < tupleCount; ++i) {
                memset(inBuffer, 0, bufSize);

                // Insert tuples.
                if (tableName == "left")prepareLeftTuple(nullsIndicator, i, inBuffer);
                else if (tableName == "right")
                    prepareRightTuple(nullsIndicator, i, inBuffer);
                else if (tableName == "leftvarchar")
                    prepareLeftVarCharTuple(nullsIndicator, i, inBuffer);
                else if (tableName == "rightvarchar")
                    prepareRightVarCharTuple(nullsIndicator, i, inBuffer);
                else if (tableName == "group")
                    prepareGroupTable(nullsIndicator, i, inBuffer);

                ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                            << "relationManager.insertTuple() should succeed.";
                rids.emplace_back(rid);
            }
        }
    };

}; // PeterDBTesting
#endif