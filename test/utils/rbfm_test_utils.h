#ifndef RBFM_TEST_UTILS_H
#define RBFM_TEST_UTILS_H

#include <cmath>
#include <fstream>
#include <chrono>
#include <numeric>
#include <unordered_set>

#include "src/include/rbfm.h"
#include "gtest/gtest.h"
#include "test/utils/general_test_utils.h"

namespace {
    std::vector<PeterDB::RID> rids;
    std::vector<int> sizes;
} // anonymous namespace

namespace PeterDBTesting {

    // Calculate actual bytes for nulls-indicator based on the given field counts
    // 8 fields = 1 byte
    static int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    static unsigned char *initializeNullFieldsIndicator(const std::vector<PeterDB::Attribute> &recordDescriptor) {
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator((int) recordDescriptor.size());
        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
        memset(indicator, 0, nullFieldsIndicatorActualSize);
        return indicator;
    }

    // parse a DDL into Attributes
    std::vector<PeterDB::Attribute> parseDDL(const std::string &ddl) {

        std::string delimiters = " ,()\"";
        std::vector<PeterDB::Attribute> table_attrs;
        char *a = new char[ddl.size() + 1];
        a[ddl.size()] = 0;
        memcpy(a, ddl.c_str(), ddl.size());
        char *tokenizer = strtok(a, delimiters.c_str());
        if (tokenizer != nullptr) {
            if (std::string(tokenizer) == "CREATE") {
                tokenizer = strtok(nullptr, delimiters.c_str());
                if (tokenizer == nullptr) {
                } else {
                    std::string type = std::string(tokenizer);
                    tokenizer = strtok(nullptr, delimiters.c_str());
                    std::string name = std::string(tokenizer);

                    // parse columnNames and types

                    PeterDB::Attribute attr;
                    while (tokenizer != nullptr) {
                        // get name if there is
                        tokenizer = strtok(nullptr, delimiters.c_str());
                        if (tokenizer == nullptr) {
                            break;
                        }
                        attr.name = std::string(tokenizer);

                        // get type
                        tokenizer = strtok(nullptr, delimiters.c_str());
                        if (std::string(tokenizer) == "INT") {
                            attr.type = PeterDB::TypeInt;
                            attr.length = 4;
                        } else if (std::string(tokenizer) == "REAL") {
                            attr.type = PeterDB::TypeReal;
                            attr.length = 4;
                        } else if (std::string(tokenizer) == "VARCHAR") {
                            attr.type = PeterDB::TypeVarChar;
                            // read length
                            tokenizer = strtok(nullptr, delimiters.c_str());
                            attr.length = strtol(tokenizer, nullptr, 10);
                        }
                        table_attrs.push_back(attr);
                    }

                }
            }
            delete[] a;
            return table_attrs;
        }
        delete[] a;
        return std::vector<PeterDB::Attribute>{};
    }

    // Write RIDs to a disk - do not use this code.
    // This is not a page-based operation. For test purpose only.
    void writeRIDsToDisk(std::vector<PeterDB::RID> &rids) {
        remove("rids_file");
        std::ofstream ridsFile("rids_file", std::ios::out | std::ios::trunc | std::ios::binary);

        if (ridsFile.is_open()) {
            ridsFile.seekp(0, std::ios::beg);
            for (auto &rid : rids) {
                ridsFile.write(reinterpret_cast<const char *>(&rid.pageNum),
                               sizeof(unsigned));
                ridsFile.write(reinterpret_cast<const char *>(&rid.slotNum),
                               sizeof(unsigned));
            }
            ridsFile.close();
        }
    }

    // Write sizes to a disk - do not use this code.
    // This is not a page-based operation. For test purpose only.
    void writeSizesToDisk(const std::vector<size_t> &sizes) {
        remove("sizes_file");
        std::ofstream sizesFile("sizes_file", std::ios::out | std::ios::trunc | std::ios::binary);

        if (sizesFile.is_open()) {
            sizesFile.seekp(0, std::ios::beg);
            for (const size_t &size : sizes) {
                sizesFile.write(reinterpret_cast<const char *>(&size),
                                sizeof(size_t));
            }
            sizesFile.close();
        }
    }

    // Read rids from the disk - do not use this code.
    // This is not a page-based operation. For test purpose only.
    void readRIDsFromDisk(std::vector<PeterDB::RID> &rids, const unsigned &numRecords) {
        PeterDB::RID tempRID;
        unsigned pageNum;
        unsigned slotNum;

        std::ifstream ridsFile("rids_file", std::ios::in | std::ios::binary);
        if (ridsFile.is_open()) {
            ridsFile.seekg(0, std::ios::beg);
            for (int i = 0; i < numRecords; i++) {
                ridsFile.read(reinterpret_cast<char *>(&pageNum), sizeof(unsigned));
                ridsFile.read(reinterpret_cast<char *>(&slotNum), sizeof(unsigned));
                tempRID.pageNum = pageNum;
                tempRID.slotNum = slotNum;
                rids.push_back(tempRID);
            }
            ridsFile.close();
        }
    }

    // Read sizes from the disk - do not use this code.
    // This is not a page-based operation. For test purpose only.
    void readSizesFromDisk(std::vector<size_t> &sizes, const unsigned &numRecords) {
        size_t size;

        std::ifstream sizesFile("sizes_file", std::ios::in | std::ios::binary);
        if (sizesFile.is_open()) {

            sizesFile.seekg(0, std::ios::beg);
            for (int i = 0; i < numRecords; i++) {
                sizesFile.read(reinterpret_cast<char *>(&size), sizeof(size_t));
                sizes.emplace_back(size);
            }
            sizesFile.close();
        }
    }

    class RBFM_Test : public ::testing::Test {
    protected:

        std::string fileName = "rbfm_test_file";
        PeterDB::FileHandle fileHandle;
        void *inBuffer = nullptr, *outBuffer = nullptr;
        unsigned char *nullsIndicator = nullptr;
        bool destroyFile = true;

    public:

        PeterDB::RecordBasedFileManager &rbfm = PeterDB::RecordBasedFileManager::instance();

        void SetUp() override {

            if (!fileExists(fileName)) {
                // Create a file
                ASSERT_EQ(rbfm.createFile(fileName), success) << "Creating the file should not fail: " << fileName;
                ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
            }

            // Open the file
            ASSERT_EQ(rbfm.openFile(fileName, fileHandle), success) << "Opening the file should not fail: " << fileName;

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);


            // Close the file
            ASSERT_EQ(rbfm.closeFile(fileHandle), success) << "Closing the file should not fail.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rbfm.destroyFile(fileName), success) << "Destroying the file should not fail.";
            }
        }

        void readRecord(const std::vector<PeterDB::Attribute> &recordDescriptor, const PeterDB::RID &rid,
                        const std::string &str) {
            size_t recordSize;
            prepareRecord((int) recordDescriptor.size(), nullsIndicator, (int) str.length(), str, 25, 177.8, 6200,
                          inBuffer, recordSize);

            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                        << "Reading a record should success.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Returned Data should be the same";
        }

        void insertRecord(const std::vector<PeterDB::Attribute> &recordDescriptor, PeterDB::RID &rid,
                          const std::string &str) {
            size_t recordSize;
            prepareRecord((int) recordDescriptor.size(), nullsIndicator, (int) str.length(), str, 25, 177.8, 6200,
                          inBuffer, recordSize);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";

        }

        void updateRecord(const std::vector<PeterDB::Attribute> &recordDescriptor, PeterDB::RID &rid,
                          const std::string &str) {
            size_t recordSize;
            prepareRecord((int) recordDescriptor.size(), nullsIndicator, (int) str.length(), str, 25, 177.8, 6200,
                          inBuffer, recordSize);

            ASSERT_EQ(rbfm.updateRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Updating a record should success.";

        }

        // Function to prepare the data in the correct form to be inserted/read
        static void prepareRecord(int fieldCount,
                                  unsigned char *nullFieldsIndicator,
                                  const int nameLength,
                                  const std::string &name,
                                  const int age,
                                  const float height,
                                  const int salary,
                                  void *buffer,
                                  size_t &recordSize) {
            int offset = 0;

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

            // Is the name field not-NULL?
            bool nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 7);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &nameLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) buffer + offset, name.c_str(), nameLength);
                offset += nameLength;
            }

            // Is the age field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 6);
            if (!nullBit) {
                memcpy((char *) buffer + offset, &age, sizeof(int));
                offset += sizeof(int);
            }

            // Is the height field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 5);
            if (!nullBit) {
                memcpy((char *) buffer + offset, &height, sizeof(float));
                offset += sizeof(float);
            }

            // Is the height field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 4);
            if (!nullBit) {
                memcpy((char *) buffer + offset, &salary, sizeof(int));
                offset += sizeof(int);
            }

            recordSize = offset;
        }

        static void createLargeRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
            char *suffix = (char *) malloc(10);
            for (int i = 0; i < 10; i++) {
                PeterDB::Attribute attr;
                sprintf(suffix, "%d", i);
                attr.name = "Char";
                attr.name += suffix;
                attr.type = PeterDB::TypeVarChar;
                attr.length = (PeterDB::AttrLength) 50;
                recordDescriptor.push_back(attr);

                sprintf(suffix, "%d", i);
                attr.name = "Int";
                attr.name += suffix;
                attr.type = PeterDB::TypeInt;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);

                sprintf(suffix, "%d", i);
                attr.name = "Real";
                attr.name += suffix;
                attr.type = PeterDB::TypeReal;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);
            }
            free(suffix);
        }


        // Record Descriptor for TweetMessage
        static void createRecordDescriptorForTweetMessage(std::vector<PeterDB::Attribute> &recordDescriptor) {

            PeterDB::Attribute attr;
            attr.name = "tweetid";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "referred_topics";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

            attr.name = "message_text";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

            attr.name = "userid";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "hash_tags";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

        }

        // Function to prepare the data in the correct form to be inserted/read
        static void prepareRecordForTweetMessage(int fieldCount, unsigned char *nullFieldsIndicator, const int tweetid,
                                                 const int referred_topicsLength, const std::string &referred_topics,
                                                 const int message_textLength, const std::string &message_text,
                                                 const int userid, const int hash_tagsLength,
                                                 const std::string &hash_tags, void *buffer, size_t &recordSize) {

            size_t offset = 0;

            // Null-indicators
            bool nullBit;
            unsigned nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            // Beginning of the actual data
            // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
            // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

            // Is the tweetid field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 7);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &tweetid, sizeof(int));
                offset += sizeof(int);
            }



            // Is the referred_topics field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 6);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &referred_topicsLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) buffer + offset, referred_topics.c_str(),
                       referred_topicsLength);
                offset += referred_topicsLength;
            }

            // Is the message_text field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 5);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &message_textLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) buffer + offset, message_text.c_str(),
                       message_textLength);
                offset += message_textLength;
            }

            // Is the userid field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 4);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &userid, sizeof(int));
                offset += sizeof(int);
            }

            // Is the hash_tag field not-NULL?
            nullBit = nullFieldsIndicator[0] & ((unsigned) 1 << (unsigned) 3);

            if (!nullBit) {
                memcpy((char *) buffer + offset, &hash_tagsLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) buffer + offset, hash_tags.c_str(),
                       hash_tagsLength);
                offset += hash_tagsLength;
            }

            recordSize = offset;

        }

        static void
        prepareLargeRecordForTweetMessage(int fieldCount, unsigned char *nullFieldsIndicator, const int index,
                                          void *buffer, int *size) {
            int offset = 0;

            int count = (index + 2) % 300 + 1;
            char text = (char) ((index + 2) % 26 + 65);
            std::string suffix(count, text);

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            // tweetid = index
            int tweetid = index + 1;

            memcpy((char *) buffer + offset, &tweetid, sizeof(int));
            offset += sizeof(int);


            // referred_topics
            std::string referred_topics = "shortcut_menu" + suffix;
            int referred_topicsLength = (int) referred_topics.length();

            memcpy((char *) buffer + offset, &referred_topicsLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, referred_topics.c_str(), referred_topicsLength);
            offset += referred_topicsLength;

            // message_text
            std::string message_text = "shortcut-menu is helpful: " + suffix;
            int message_textLength = (int) message_text.length();
            memcpy((char *) buffer + offset, &message_textLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, message_text.c_str(), message_textLength);
            offset += message_textLength;

            // userid
            memcpy((char *) buffer + offset, &index, sizeof(int));
            offset += sizeof(int);

            // hash_tag

            std::string hash_tags_text = "#shortcut-menu" + suffix;
            int hash_tags_textLength = (int) hash_tags_text.length();
            memcpy((char *) buffer + offset, &hash_tags_textLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, hash_tags_text.c_str(), hash_tags_textLength);
            offset += hash_tags_textLength;

            *size = offset;
        }

        // Record Descriptor for TwitterUser
        static void createRecordDescriptorForTwitterUser(std::vector<PeterDB::Attribute> &recordDescriptor) {

            PeterDB::Attribute attr;
            attr.name = "userid";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "statuses_count";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "screen_name";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

            attr.name = "username";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

            attr.name = "satisfaction_score";
            attr.type = PeterDB::TypeReal;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "lang";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

        }

        // Record Descriptor for TwitterUser
        static void createRecordDescriptorForTwitterUser2(
                std::vector<PeterDB::Attribute> &recordDescriptor) {

            PeterDB::Attribute attr;
            attr.name = "userid";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "statuses_count";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "screen_name";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 1000;
            recordDescriptor.push_back(attr);

            attr.name = "username";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 1000;
            recordDescriptor.push_back(attr);

            attr.name = "satisfaction_score";
            attr.type = PeterDB::TypeReal;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "lang";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 500;
            recordDescriptor.push_back(attr);

        }

        static void
        prepareLargeRecordForTwitterUser(int fieldCount, unsigned char *nullFieldsIndicator, const int index,
                                         void *buffer, size_t &size) {
            size_t offset = 0;

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            int count = (index + 2) % 200 + 1;
            char text = (char) ((index + 2) % 26 + 97);
            std::string suffix(count, text);

            // userid = index
            memcpy((char *) buffer + offset, &index, sizeof(int));
            offset += sizeof(int);

            // statuses_count
            int statuses_count = count + 1;

            memcpy((char *) buffer + offset, &statuses_count, sizeof(int));
            offset += sizeof(int);

            // screen_name
            std::string screen_name = "MillironNila@" + suffix;
            int screen_nameLength = (int) screen_name.length();

            memcpy((char *) buffer + offset, &screen_nameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, screen_name.c_str(), screen_nameLength);
            offset += screen_nameLength;

            // username
            std::string username = "Milliron Nilla " + suffix;
            int usernameLength = (int) username.length();

            memcpy((char *) buffer + offset, &usernameLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, username.c_str(), usernameLength);
            offset += usernameLength;

            // satisfaction_score
            auto satisfaction_score = (float) (count + 0.1);

            memcpy((char *) buffer + offset, &satisfaction_score, sizeof(float));
            offset += sizeof(float);

            // lang
            std::string lang = "En " + suffix;
            int langLength = (int) lang.length();

            memcpy((char *) buffer + offset, &langLength, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) buffer + offset, lang.c_str(), langLength);
            offset += langLength;

            size = offset;

        }

        static void createRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {

            PeterDB::Attribute attr;
            attr.name = "EmpName";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 30;
            recordDescriptor.push_back(attr);

            attr.name = "Age";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "Height";
            attr.type = PeterDB::TypeReal;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

            attr.name = "Salary";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);

        }

        static void prepareLargeRecord(int fieldCount, unsigned char *nullFieldsIndicator,
                                       const int index, void *buffer, int *size) {
            int offset = 0;

            // compute the count
            int count = (index + 2) % 50 + 1;

            // compute the letter
            char text = (char) ((index + 2) % 26 + 97);

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            // Actual data
            for (int i = 0; i < 10; i++) {
                memcpy((char *) buffer + offset, &count, sizeof(int));
                offset += sizeof(int);
                for (int j = 0; j < count; j++) {
                    memcpy((char *) buffer + offset, &text, 1);
                    offset += 1;
                }

                // compute the integer
                memcpy((char *) buffer + offset, &index, sizeof(int));
                offset += sizeof(int);

                // compute the floating number
                auto real = (float) (index + 1);
                memcpy((char *) buffer + offset, &real, sizeof(float));
                offset += sizeof(float);
            }
            *size = offset;
        }

        static void createLargeRecordDescriptor2(std::vector<PeterDB::Attribute> &recordDescriptor) {
            char *suffix = (char *) malloc(10);
            for (int i = 0; i < 10; i++) {
                PeterDB::Attribute attr;
                sprintf(suffix, "%d", i);
                attr.name = "Int";
                attr.name += suffix;
                attr.type = PeterDB::TypeInt;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);

                sprintf(suffix, "%d", i);
                attr.name = "Real";
                attr.name += suffix;
                attr.type = PeterDB::TypeReal;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);

                sprintf(suffix, "%d", i);
                attr.name = "Char";
                attr.name += suffix;
                attr.type = PeterDB::TypeVarChar;
                attr.length = (PeterDB::AttrLength) 60;
                recordDescriptor.push_back(attr);

            }
            free(suffix);
        }

        static void prepareLargeRecord2(int fieldCount, unsigned char *nullFieldsIndicator,
                                        const int index, void *buffer, int *size) {
            int offset = 0;

            // compute the count
            int count = (index + 2) % 60 + 1;

            // compute the letter
            char text = (char) ((index + 2) % 26 + 65);

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            for (int i = 0; i < 10; i++) {
                // compute the integer
                memcpy((char *) buffer + offset, &index, sizeof(int));
                offset += sizeof(int);

                // compute the floating number
                auto real = (float) (index + 1);
                memcpy((char *) buffer + offset, &real, sizeof(float));
                offset += sizeof(float);

                // compute the varchar field
                memcpy((char *) buffer + offset, &count, sizeof(int));
                offset += sizeof(int);

                for (int j = 0; j < count; j++) {
                    memcpy((char *) buffer + offset, &text, 1);
                    offset += 1;
                }

            }
            *size = offset;
        }

        static void createLargeRecordDescriptor3(std::vector<PeterDB::Attribute> &recordDescriptor) {
            int index = 0;
            char *suffix = (char *) malloc(10);
            for (int i = 0; i < 8; i++) {
                PeterDB::Attribute attr;
                sprintf(suffix, "%d", index);
                attr.name = "attr";
                attr.name += suffix;
                attr.type = PeterDB::TypeVarChar;
                attr.length = (PeterDB::AttrLength) 60;
                recordDescriptor.push_back(attr);
                index++;

                sprintf(suffix, "%d", index);
                attr.name = "attr";
                attr.name += suffix;
                attr.type = PeterDB::TypeInt;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);
                index++;

                sprintf(suffix, "%d", index);
                attr.name = "attr";
                attr.name += suffix;
                attr.type = PeterDB::TypeReal;
                attr.length = (PeterDB::AttrLength) 4;
                recordDescriptor.push_back(attr);
                index++;

            }
            free(suffix);
        }

        static void prepareLargeRecord3(int fieldCount, unsigned char *nullFieldsIndicator,
                                        const int index, void *buffer, unsigned *size) {
            int offset = 0;

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            int attr_pos = 0;
            int attr_pos_in_nth_byte;
            unsigned attr_pos_in_nth_bit_in_a_byte;

            bool nullBit;

            for (int i = 0; i < index; i++) {
                attr_pos_in_nth_byte = floor((double) attr_pos / CHAR_BIT);
                attr_pos_in_nth_bit_in_a_byte = CHAR_BIT - 1 - attr_pos % CHAR_BIT;

                nullBit = nullFieldsIndicator[attr_pos_in_nth_byte]
                          & ((unsigned) 1 << attr_pos_in_nth_bit_in_a_byte);

                if (!nullBit) {
                    // compute the Varchar field
                    memcpy((char *) buffer + offset, &attr_pos, sizeof(int));
                    offset += sizeof(int);

                    for (int j = 0; j < attr_pos; j++) {
                        char text = (char) ((attr_pos + 3) % 26 + 65);
                        memcpy((char *) buffer + offset, &text, 1);
                        offset += 1;
                    }
                }

                attr_pos++;

                attr_pos_in_nth_byte = floor((double) attr_pos / CHAR_BIT);
                attr_pos_in_nth_bit_in_a_byte = CHAR_BIT - 1 - attr_pos % CHAR_BIT;

                nullBit = nullFieldsIndicator[attr_pos_in_nth_byte]
                          & ((unsigned) 1 << attr_pos_in_nth_bit_in_a_byte);

                if (!nullBit) {
                    // compute the integer
                    memcpy((char *) buffer + offset, &index, sizeof(int));
                    offset += sizeof(int);
                }

                attr_pos++;

                attr_pos_in_nth_byte = floor((double) attr_pos / CHAR_BIT);
                attr_pos_in_nth_bit_in_a_byte = CHAR_BIT - 1 - attr_pos % CHAR_BIT;

                nullBit = nullFieldsIndicator[attr_pos_in_nth_byte]
                          & ((unsigned) 1 << attr_pos_in_nth_bit_in_a_byte);

                if (!nullBit) {
                    // compute the floating number
                    auto real = (float) (attr_pos + 3.001);
                    memcpy((char *) buffer + offset, &real, sizeof(float));
                    offset += sizeof(float);
                }

                attr_pos++;
            }
            *size = offset;
        }

        static void createLargeRecordDescriptor4(std::vector<PeterDB::Attribute> &recordDescriptor) {
            PeterDB::Attribute attr;
            attr.name = "attr0";
            attr.type = PeterDB::TypeVarChar;
            attr.length = (PeterDB::AttrLength) 2200;
            recordDescriptor.push_back(attr);

            attr.name = "attr1";
            attr.type = PeterDB::TypeInt;
            attr.length = (PeterDB::AttrLength) 4;
            recordDescriptor.push_back(attr);
        }

        static void prepareLargeRecord4(int fieldCount, unsigned char *nullFieldsIndicator,
                                        const int index, void *buffer, size_t &size) {
            size_t offset = 0;

            // compute the count
            int count = index % 2200 + 1;

            // compute the letter
            char text = (char) ((index + 2) % 26 + 65);

            // Null-indicators
            int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(
                    fieldCount);

            // Null-indicators
            memcpy((char *) buffer + offset, nullFieldsIndicator,
                   nullFieldsIndicatorActualSize);
            offset += nullFieldsIndicatorActualSize;

            // compute the varchar field
            memcpy((char *) buffer + offset, &count, sizeof(int));
            offset += sizeof(int);

            for (int j = 0; j < count; j++) {
                memcpy((char *) buffer + offset, &text, 1);
                offset += 1;
            }

            // compute the integer
            memcpy((char *) buffer + offset, &index, sizeof(int));
            offset += sizeof(int);

            size = offset;
        }

    };

    class RBFM_Test_2 : public RBFM_Test {

    protected:
        bool destroyFile = true;

    public:
        void SetUp() override {

            fileName = "rbfm_test_file_2";

            if (!fileExists(fileName)) {
                // Create a file
                ASSERT_EQ(rbfm.createFile(fileName), success) << "Creating the file should not fail: " << fileName;
                ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
            }

            // Open the file
            ASSERT_EQ(rbfm.openFile(fileName, fileHandle), success) << "Opening the file should not fail: " << fileName;

        }

        void TearDown() override {
            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);
            free(nullsIndicator);

            // Close the file
            ASSERT_EQ(rbfm.closeFile(fileHandle), success) << "Closing the file should not fail.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(rbfm.destroyFile(fileName), success) << "Destroying the file should not fail.";

                remove("rids_file");
                remove("sizes_file");
            }
        }

    };

} // namespace PeterDBTesting

#endif // RBFM_TEST_UTILS_H