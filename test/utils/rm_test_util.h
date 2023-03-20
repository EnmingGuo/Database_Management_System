#ifndef _test_util_h_
#define _test_util_h_

#include "src/include/rm.h"
#include "gtest/gtest.h"
#include "test/utils/general_test_utils.h"
#include "test/utils/rbfm_test_utils.h"

namespace PeterDBTesting {
    struct Tweet {
        unsigned tweet_id;
        std::string text;
        unsigned user_id;
        float sentiment;
        std::string hash_tags;
        std::string embedded_url;
        float lat;
        float lng;

        Tweet() {

        }

        Tweet(void *buffer) {
            char nullsIndicator = *(char *) buffer;
            unsigned offset = 1;

            if ((nullsIndicator >> 7u) & 1u) {
                tweet_id = -1;
            } else {
                tweet_id = *(unsigned *) ((char *) buffer + offset);
                offset += sizeof(unsigned);
            }

            if ((nullsIndicator >> 6u) & 1u) {
                text = "";
            } else {
                unsigned len = *(unsigned *) ((char *) buffer + offset);
                offset += sizeof(unsigned);
                char s[len + 1];
                memcpy(&s, (char *) buffer + offset, len);
                s[len] = '\0';
                text = std::string(s);
                offset += len;
            }

            if ((nullsIndicator >> 5u) & 1u) {
                user_id = -1;
            } else {
                user_id = *(unsigned *) ((char *) buffer + offset);
                offset += sizeof(unsigned);
            }
            if ((nullsIndicator >> 4u) & 1u) {
                sentiment = -1;
            } else {
                sentiment = *(float *) ((char *) buffer + offset);
                offset += sizeof(float);
            }

            if ((nullsIndicator >> 3u) & 1u) {
                hash_tags = "";
            } else {
                unsigned len = *(unsigned *) ((char *) buffer + offset);
                offset += sizeof(unsigned);
                char s[len + 1];
                memcpy(&s, (char *) buffer + offset, len);
                s[len] = '\0';
                hash_tags = std::string(s);
                offset += len;
            }
            if ((nullsIndicator >> 2u) & 1u) {
                embedded_url = "";
            } else {
                unsigned len = *(unsigned *) ((char *) buffer + offset);
                offset += sizeof(unsigned);
                char s[len + 1];
                memcpy(&s, (char *) buffer + offset, len);
                s[len] = '\0';
                embedded_url = std::string(s);
                offset += len;
            }
            if ((nullsIndicator >> 1u) & 1u) {
                lat = -1;
            } else {
                lat = *(float *) ((char *) buffer + offset);
                offset += sizeof(float);
            }
            if (nullsIndicator & 1u) {
                lng = -1;
            } else {
                lng = *(float *) ((char *) buffer + offset);
            }

        }
    };

    class RM_Catalog_Test : public ::testing::Test {

    public:
        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();

    };

    class RM_Tuple_Test : public ::testing::Test {
    protected:

        std::string tableName = "rm_test_table";
        PeterDB::FileHandle fileHandle;
        size_t bufSize;
        void *inBuffer = nullptr, *outBuffer = nullptr;
        unsigned char *nullsIndicator = nullptr;
        unsigned char *nullsIndicatorWithNull = nullptr;
        bool destroyFile = true;
        PeterDB::RID rid;
        std::vector<PeterDB::Attribute> attrs;

    public:

        PeterDB::RelationManager &rm = PeterDB::RelationManager::instance();

        void SetUp() override {
           std::cerr<<"Will run SetUp of RM_Tuple_Test"<<std::endl;
            if (!fileExists(tableName)) {
                std::cerr<<"Don't have the "+tableName<<std::endl;
                // Try to delete the System Catalog.
                // If this is the first time, it will generate an error. It's OK and we will ignore that.
                rm.deleteCatalog();

                // Create Catalog
                ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

                // Create a table
                std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                        "CREATE TABLE " + tableName + " (emp_name VARCHAR(50), age INT, height REAL, salary REAL)");
                ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                            << "Create table " << tableName << " should succeed.";
                ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";
            }

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should not fail.";
            }

            // Delete Catalog
            ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";
        }

    };

    class RM_Scan_Test : public RM_Tuple_Test {
    protected:
        PeterDB::RM_ScanIterator rmsi;

    public:

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            // Close the iterator
            ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Deleting the table should succeed.";
            }

            // Delete Catalog
            ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";
        };
    };

    class RM_Large_Table_Test : public RM_Scan_Test {
    protected:
        size_t bufSize = 4000;
        std::string tableName = "rm_test_large_table";
        bool destroyFile = false;
        std::vector<PeterDB::RID> rids;
        std::vector<size_t> sizes;
    public:

        void SetUp() override {

            if (!fileExists(tableName)) {
                // Try to delete the System Catalog.
                // If this is the first time, it will generate an error. It's OK and we will ignore that.
                rm.deleteCatalog();

                // Create Catalog
                ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";
                createLargeTable(tableName);
            }
        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);

            // Close the iterator
            ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rm.deleteTable(tableName), success) << "Deleting the table should succeed.";

                // Delete Catalog
                ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

                remove("rids_file");
                remove("sizes_file");
            }

        };

        // Create a large table for pressure test
        void createLargeTable(const std::string &largeTableName) {

            // 1. Create Table
            std::vector<PeterDB::Attribute> attrs;

            int index = 0;

            for (unsigned i = 0; i < 10; i++) {
                PeterDB::Attribute attr;
                std::string suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeVarChar;
                attr.length = (PeterDB::AttrLength) 50;
                attrs.push_back(attr);
                index++;

                suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeInt;
                attr.length = (PeterDB::AttrLength) 4;
                attrs.push_back(attr);
                index++;

                suffix = std::to_string(index);
                attr.name = "attr" + suffix;
                attr.type = PeterDB::TypeReal;
                attr.length = (PeterDB::AttrLength) 4;
                attrs.push_back(attr);
                index++;
            }

            ASSERT_EQ(rm.createTable(largeTableName, attrs), success)
                                        << "Create table " << largeTableName << " should succeed.";

        }

        static void prepareLargeTuple(int attributeCount, unsigned char *nullAttributesIndicator, const unsigned index,
                                      void *buffer, size_t &size) {
            size_t offset = 0;

            // Null-indicators
            int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // compute the count
            unsigned count = index % 50 + 1;

            // compute the letter
            char text = (char) (index % 26 + 97);

            for (unsigned i = 0; i < 10; i++) {
                // length
                memcpy((char *) buffer + offset, &count, sizeof(int));
                offset += sizeof(int);

                // varchar
                for (int j = 0; j < count; j++) {
                    memcpy((char *) buffer + offset, &text, 1);
                    offset += 1;
                }

                // integer
                memcpy((char *) buffer + offset, &index, sizeof(int));
                offset += sizeof(int);

                // real
                auto real = (float) (index + 1);
                memcpy((char *) buffer + offset, &real, sizeof(float));
                offset += sizeof(float);
            }
            size = offset;
        }

    };

    class RM_Catalog_Scan_Test : public RM_Scan_Test {

    public:
        void checkCatalog(const std::string &expectedString) {
            memset(outBuffer, 0, bufSize);
            ASSERT_NE(rmsi.getNextTuple(rid, outBuffer), RM_EOF) << "Scan should continue.";
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager::printTuple() should succeed.";
            // Here we check only the needed fields, some values are ignored (marked "x")
            checkPrintRecord(expectedString, stream.str(), true, {"table-id"}, true);
        }
    };

    class RM_Version_Test : public RM_Tuple_Test {
    protected:
        std::string tableName = "rm_extra_test_table";

    public:
        void SetUp() override {

            // Try to delete the System Catalog.
            // If this is the first time, it will generate an error. It's OK and we will ignore that.
            rm.deleteCatalog();

            remove(tableName.c_str());

            // Create Catalog
            ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

            // Create a table
            std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                    "CREATE TABLE " + tableName + " (emp_name VARCHAR(40), age INT, height REAL, salary REAL)");
            ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                        << "Create table " << tableName << " should succeed.";
            ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);
            free(nullsIndicatorWithNull);


            // Destroy the file
            ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should not fail.";

            rm.deleteCatalog();
        }
    };

    class RM_Catalog_Scan_Test_2 : public RM_Catalog_Scan_Test {
    protected:

        std::vector<PeterDB::RID> rids;
        std::vector<char> nullsIndicators;

    public:
        void SetUp() override {
            tableName = "rm_test_table_2";

            // Try to delete the System Catalog.
            // If this is the first time, it will generate an error. It's OK and we will ignore that.
            rm.deleteCatalog();

            remove(tableName.c_str());

            // Create Catalog
            ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

            // Create a table
            std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                    "CREATE TABLE " + tableName +
                    " (tweet_id INT, text VARCHAR(400), user_id INT, sentiment REAL, hash_tags VARCHAR(100), embedded_url VARCHAR(200), lat REAL, lng REAL)");
            ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                        << "Create table " << tableName << " should succeed.";
            ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";

        }

        void validateAttribute(const unsigned &attrID, const unsigned &index, const unsigned &seed,
                               const unsigned &salt) {
            std::string attributeName;
            if (attrID == 0) {
                attributeName = "tweet_id";
            } else if (attrID == 1) {
                attributeName = "text";
            } else if (attrID == 2) {
                attributeName = "user_id";
            } else if (attrID == 3) {
                attributeName = "sentiment";
            } else if (attrID == 4) {
                attributeName = "hash_tags";
            } else if (attrID == 5) {
                attributeName = "embedded_url";
            } else if (attrID == 6) {
                attributeName = "lat";
            } else if (attrID == 7) {
                attributeName = "lng";
            }


            // Read Attribute
            ASSERT_EQ(rm.readAttribute(tableName, rids[index], attributeName, outBuffer), success)
                                        << "RelationManager::readAttribute() should succeed.";

            nullsIndicator[0] = nullsIndicators[index];
            Tweet tweet;
            size_t tupleSize;
            generateTuple(nullsIndicator, inBuffer, seed, salt, tupleSize, tweet);

            if ((nullsIndicator[0] >> (7u - attrID)) & 1u) {
                EXPECT_EQ(*((unsigned char *) outBuffer), 128u)
                                    << "returned " << attributeName << " field should be null";
            } else {
                if (attributeName == "tweet_id") {

                    EXPECT_EQ(memcmp(((char *) outBuffer + 1), &tweet.tweet_id, sizeof(unsigned)), 0)
                                        << "returned tweet_id field is not correct.";

                } else if (attributeName == "text") {
                    EXPECT_EQ(*(unsigned *) ((char *) outBuffer + 1), tweet.text.length())
                                        << "returned text field length is not correct.";
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1 + sizeof(unsigned)), tweet.text.c_str(),
                                     tweet.text.length()), 0)
                                        << "returned text field is not correct.";

                } else if (attributeName == "user_id") {
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1), &tweet.user_id, sizeof(unsigned)), 0)
                                        << "returned user_id field is not correct.";

                } else if (attributeName == "sentiment") {
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1), &tweet.sentiment, sizeof(float)), 0)
                                        << "returned sentiment field is not correct.";

                } else if (attributeName == "hash_tags") {
                    EXPECT_EQ(*(unsigned *) ((char *) outBuffer + 1), tweet.hash_tags.length())
                                        << "returned hash_tags field length is not correct.";
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1 + sizeof(unsigned)), tweet.hash_tags.c_str(),
                                     tweet.hash_tags.length()), 0)
                                        << "returned hash_tags field is not correct.";

                } else if (attributeName == "embedded_url") {
                    EXPECT_EQ(*(unsigned *) ((char *) outBuffer + 1), tweet.embedded_url.length())
                                        << "returned embedded_url field length is not correct.";
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1 + sizeof(unsigned)), tweet.embedded_url.c_str(),
                                     tweet.embedded_url.length()),
                              0) << "returned embedded_url field is not correct.";

                } else if (attributeName == "lat") {
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1), &tweet.lat, sizeof(float)), 0)
                                        << "returned lat field is not correct.";

                } else if (attributeName == "lng") {
                    EXPECT_EQ(memcmp(((char *) outBuffer + 1), &tweet.lng, sizeof(float)), 0)
                                        << "returned lng field is not correct.";

                }
            }

        }

        void generateTuple(const unsigned char *nullsIndicator, void *buffer, const unsigned &seed,
                           const unsigned &salt, size_t &tupleSize, Tweet &tweet) {

            unsigned offset = 0;

            // Null-indicators

            unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(8);

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            std::default_random_engine generator(seed);
            std::uniform_int_distribution<unsigned> dist400(0, 400);
            std::uniform_int_distribution<unsigned> dist100(0, 100);
            std::uniform_int_distribution<unsigned> dist200(0, 200);


            // Is the tweet_id field not-NULL?
            if (!((nullsIndicator[0] >> 7u) & 1u)) {
                *(unsigned *) ((char *) buffer + offset) = salt + seed;
                offset += sizeof(unsigned);
            }

            // Is the text field not-NULL?
            if (!((nullsIndicator[0] >> 6u) & 1u)) {
                unsigned len = dist400(generator);
                *(unsigned *) ((char *) buffer + offset) = len;
                offset += sizeof(unsigned);
                for (unsigned i = 0; i < len; i++) {
                    *((char *) buffer + offset++) = dist400(generator) % 26 + 'A';
                }
            }

            // Is the user_id field not-NULL?
            if (!((nullsIndicator[0] >> 5u) & 1u)) {
                *(unsigned *) ((char *) buffer + offset) = 2 * seed + salt;
                offset += sizeof(unsigned);
            }

            // Is the sentiment field not-NULL?
            if (!((nullsIndicator[0] >> 4u) & 1u)) {
                *(float *) ((char *) buffer + offset) = salt / 2.3 * seed * 1000;
                offset += sizeof(float);
            }

            // Is the hash_tag field not-NULL?
            if (!((nullsIndicator[0] >> 3u) & 1u)) {
                unsigned len = dist100(generator);
                *(unsigned *) ((char *) buffer + offset) = len;
                offset += sizeof(unsigned);
                for (unsigned i = 0; i < len; i++) {
                    *((char *) buffer + offset) = dist100(generator) % 26 + 'A';
                    offset += 1;
                }
            }

            // Is the embedded_url field not-NULL?
            if (!((nullsIndicator[0] >> 2u) & 1u)) {
                unsigned len = dist200(generator);
                *(unsigned *) ((char *) buffer + offset) = len;
                offset += sizeof(unsigned);
                for (unsigned i = 0; i < len; i++) {
                    *((char *) buffer + offset) = dist200(generator) % 26 + 'A';
                    offset += 1;
                }
            }

            // Is the lat field not-NULL?
            if (!((nullsIndicator[0] >> 1u) & 1u)) {
                *(float *) ((char *) buffer + offset) = (dist100(generator) % 45) * 4.0f;
                offset += sizeof(float);
            }

            // Is the lng field not-NULL?
            if (!(nullsIndicator[0] & 1u)) {
                *(float *) ((char *) buffer + offset) = (dist100(generator) % 60) * 3.0f;
                offset += sizeof(float);
            }

            tupleSize = offset;

            tweet = Tweet(inBuffer);

        }

    };

    // Function to prepare the data in the correct form to be inserted/read/updated
    void prepareTuple(const size_t &attributeCount, unsigned char *nullAttributesIndicator, const size_t nameLength,
                      const std::string &name, const unsigned age, const float height, const float salary, void *buffer,
                      size_t &tupleSize) {
        unsigned offset = 0;

        // Null-indicators
        bool nullBit;
        unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
        offset += nullAttributesIndicatorActualSize;

        // Beginning of the actual data
        // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
        // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

        // Is the name field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &nameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, name.c_str(), nameLength);
            offset += nameLength;
        }

        // Is the age field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &age, sizeof(int));
            offset += sizeof(int);
        }


        // Is the height field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &height, sizeof(float));
            offset += sizeof(float);
        }


        // Is the salary field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &salary, sizeof(float));
            offset += sizeof(int);
        }

        tupleSize = offset;
    }

    // Function to get the data in the correct form to be inserted/read after adding the attribute ssn
    void prepareTupleAfterAdd(int attributeCount, unsigned char *nullAttributesIndicator, const int nameLength,
                              const std::string &name, const unsigned age, const float height, const float salary,
                              const int ssn, void *buffer, size_t &tupleSize) {
        unsigned offset = 0;

        // Null-indicators
        bool nullBit;
        unsigned nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributeCount);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullAttributesIndicator, nullAttributesIndicatorActualSize);
        offset += nullAttributesIndicatorActualSize;

        // Beginning of the actual data
        // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
        // e.g., if a tuple consists of four attributes and they are all nulls, then the bit representation will be: [11110000]

        // Is the name field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 7);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &nameLength, sizeof(unsigned));
            offset += sizeof(unsigned);
            memcpy((char *) buffer + offset, name.c_str(), nameLength);
            offset += nameLength;
        }

        // Is the age field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 6);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &age, sizeof(unsigned));
            offset += sizeof(unsigned);
        }

        // Is the height field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 5);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &height, sizeof(float));
            offset += sizeof(float);
        }

        // Is the salary field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 4);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &salary, sizeof(float));
            offset += sizeof(float);
        }

        // Is the ssn field not-NULL?
        nullBit = nullAttributesIndicator[0] & ((unsigned) 1 << (unsigned) 3);

        if (!nullBit) {
            memcpy((char *) buffer + offset, &ssn, sizeof(unsigned));
            offset += sizeof(unsigned);
        }

        tupleSize = offset;
    }

} // namespace PeterDBTesting
#endif // _test_util_h_