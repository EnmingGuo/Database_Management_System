#include "src/include/rbfm.h"
#include "test/utils/rbfm_test_utils.h"

namespace PeterDBTesting {

    TEST_F(RBFM_Test, insert_and_read_a_record) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record
        // 4. Read Record
        // 5. Close Record-Based File
        // 6. Destroy Record-Based File

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a inBuffer into a file and print the inBuffer
        prepareRecord((int) (int) recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, inBuffer, recordSize);

        std::ostringstream stream;
        rbfm.printRecord(recordDescriptor, inBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: 25, Height: 177.8, Salary: 6200", stream.str()));
        // I insert a breakpoint in front of entering the inserRecord function.
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a inBuffer should succeed.";

        // Given the rid, read the inBuffer from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a inBuffer should succeed.";

        stream.str(std::string());
        stream.clear();
        rbfm.printRecord(recordDescriptor, outBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: 25, Height: 177.8, Salary: 6200", stream.str()));

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "the read data should match the inserted data";

    }

    TEST_F(RBFM_Test, insert_and_read_a_record_with_null) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record - NULL
        // 4. Read Record
        // 5. Close Record-Based File
        // 6. Destroy Record-Based File

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // Initialize a NULL field indicator
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator((int) (int) recordDescriptor.size());
        unsigned char nullsIndicator[nullFieldsIndicatorActualSize];
        memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

        // Setting the age & salary fields value as null
        nullsIndicator[0] = 80; // 01010000

        // Insert a record into a file and print the record
        prepareRecord((int) (int) recordDescriptor.size(), nullsIndicator, 8, "Anteater", 25, 177.8, 6200, inBuffer, recordSize);

        std::ostringstream stream;
        rbfm.printRecord(recordDescriptor, inBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: NULL, Height: 177.8, Salary: NULL", stream.str()));

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        stream.str(std::string());
        stream.clear();
        rbfm.printRecord(recordDescriptor, outBuffer, stream);
        ASSERT_NO_FATAL_FAILURE(
                checkPrintRecord("EmpName: Anteater, Age: NULL, Height: 177.8, Salary: NULL", stream.str()));

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "the read data should match the inserted data";

    }

    TEST_F(RBFM_Test, insert_and_read_multiple_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Multiple Records
        // 4. Reopen Record-Based File
        // 5. Read Multiple Records
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File

        PeterDB::RID rid;
        inBuffer = malloc(1000);
        int numRecords = 2000;

        // clean caches
        rids.clear();
        sizes.clear();

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor(recordDescriptor);

        for (PeterDB::Attribute &i : recordDescriptor) {
            GTEST_LOG_(INFO) << "Attr Name: " << i.name << " Attr Type: " << (PeterDB::AttrType) i.type
                             << " Attr Len: " << i.length;
        }

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert 2000 records into file
        for (int i = 0; i < numRecords; i++) {

            // Test insert Record
            int size = 0;
            memset(inBuffer, 0, 1000);
            prepareLargeRecord((int) (int) recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a inBuffer should succeed.";

            // Leave rid and sizes for next test to examine
            rids.push_back(rid);
            sizes.push_back(size);
        }

        ASSERT_EQ(rids.size(), numRecords) << "Reading records should succeed.";
        ASSERT_EQ(sizes.size(), (unsigned) numRecords) << "Reading records should succeed.";

        outBuffer = malloc(1000);

        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            if (i % 1000 == 0) {
                std::ostringstream stream;
                rbfm.printRecord(recordDescriptor, outBuffer, stream);
                GTEST_LOG_(INFO) << "Returned Data: " << stream.str();
            }

            int size = 0;
            prepareLargeRecord((int) (int) recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);
            ASSERT_EQ(memcmp(outBuffer, inBuffer, sizes[i]), 0) << "the read data should match the inserted data";
        }

    }

    TEST_F(RBFM_Test, insert_and_read_massive_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Massive Records
        // 4. Reopen Record-Based File
        // 5. Read Massive Records
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File
        PeterDB::RID rid;
        inBuffer = malloc(1000);
        int numRecords = 10000;

        // clean caches
        rids.clear();
        sizes.clear();

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor(recordDescriptor);

        for (PeterDB::Attribute &i : recordDescriptor) {
            GTEST_LOG_(INFO) << "Attr Name: " << i.name << " Attr Type: " << (PeterDB::AttrType) i.type
                             << " Attr Len: " << i.length;
        }

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert 2000 records into file
        for (int i = 0; i < numRecords; i++) {

            // Test insert Record
            int size = 0;
            memset(inBuffer, 0, 1000);
            prepareLargeRecord((int) (int) recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a inBuffer should succeed.";

            // Leave rid and sizes for next test to examine
            rids.push_back(rid);
            sizes.push_back(size);
        }

        ASSERT_EQ(rids.size(), numRecords) << "Reading records should succeed.";
        ASSERT_EQ(sizes.size(), (unsigned) numRecords) << "Reading records should succeed.";

        outBuffer = malloc(1000);

        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            if (i % 1000 == 0) {
                std::ostringstream stream;
                rbfm.printRecord(recordDescriptor, outBuffer, stream);
                GTEST_LOG_(INFO) << "Returned Data: " << stream.str();

            }

            int size = 0;
            prepareLargeRecord((int) (int) recordDescriptor.size(), nullsIndicator, i, inBuffer, &size);
            ASSERT_EQ(memcmp(outBuffer, inBuffer, sizes[i]),
                      0) << "the read data should match the inserted data";
        }
    }

    TEST_F(RBFM_Test, delete_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record (3)
        // 4. Delete Record (1)
        // 5. Read Record
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a record into a file
        prepareRecord((int) (int) recordDescriptor.size(), nullsIndicator, 8, "Testcase", 25, 177.8, 6200, inBuffer,
                      recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";
        // save the returned RID
        PeterDB::RID rid0 = rid;

        free(nullsIndicator);
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a record into a file
        nullsIndicator[0] = 128;
        prepareRecord((int) (int) recordDescriptor.size(), nullsIndicator, 0, "", 25, 177.8, 6200, inBuffer,
                      recordSize);


        // Insert three copies
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";
        // save the returned RID
        PeterDB::RID rid1 = rid;

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";


        // Delete the first record
        ASSERT_EQ(rbfm.deleteRecord(fileHandle, recordDescriptor, rid0), success)
                                    << "Deleting a record should succeed.";

        ASSERT_NE(rbfm.readRecord(fileHandle, recordDescriptor, rid0, outBuffer), success)
                                    << "Reading a deleted record should not succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid1, outBuffer), success)
                                    << "Reading a record should succeed.";

        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("EmpName: NULL, Age: 25, Height: 177.8, Salary: 6200", stream.str());

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "The returned record should match the inserted.";


        // Reinsert a record
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";
        ASSERT_EQ(rid.slotNum, rid0.slotNum) << "Inserted record should use previous deleted slot.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("EmpName: NULL, Age: 25, Height: 177.8, Salary: 6200", stream.str());

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "The returned record should match the inserted.";
    }

    TEST_F(RBFM_Test, update_records) {
        // Functions tested
        // 1. Create Record-Based File
        // 2. Open Record-Based File
        // 3. Insert Record
        // 4. Update Record
        // 5. Read Record
        // 6. Close Record-Based File
        // 7. Destroy Record-Based File
        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);
        recordDescriptor[0].length = (PeterDB::AttrLength) 1000;
        PeterDB::RID rid;

        inBuffer = malloc(2000);
        outBuffer = malloc(2000);

        std::string longStr;
        for (int i = 0; i < 1000; i++) {
            longStr.push_back('a');
        }

        std::string shortStr;
        for (int i = 0; i < 10; i++) {
            shortStr.push_back('s');
        }

        std::string midString;
        for (int i = 0; i < 100; i++) {
            midString.push_back('m');
        }

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert short record
        insertRecord(recordDescriptor, rid, shortStr);
        PeterDB::RID shortRID = rid;

        // Insert mid record
        insertRecord(recordDescriptor, rid, midString);
        PeterDB::RID midRID = rid;

        // Insert long record
        insertRecord(recordDescriptor, rid, longStr);

        // update short record
        updateRecord(recordDescriptor, shortRID, midString);

        //read updated short record and verify its content
        readRecord(recordDescriptor, shortRID, midString);

        // insert two more records
        insertRecord(recordDescriptor, rid, longStr);

        insertRecord(recordDescriptor, rid, longStr);

        // read mid record and verify its content
        readRecord(recordDescriptor, midRID, midString);

        // update short record
        updateRecord(recordDescriptor, shortRID, longStr);

        // read the short record and verify its content
        readRecord(recordDescriptor, shortRID, longStr);
       // unsigned temp_size=longStr.size();
        // delete the short record

        rbfm.deleteRecord(fileHandle, recordDescriptor, shortRID);

        // verify the short record has been deleted
        ASSERT_NE(rbfm.readRecord(fileHandle, recordDescriptor, shortRID, outBuffer), success)
                                    << "Read a deleted record should not success.";

    }

    TEST_F(RBFM_Test_2, varchar_compact_size) {
        // Checks whether VarChar is implemented correctly or not.
        //
        // Functions tested
        // 1. Create Two Record-Based File
        // 2. Open Two Record-Based File
        // 3. Insert Multiple Records Into Two files
        // 4. Close Two Record-Based File
        // 5. Compare The File Sizes
        // 6. Destroy Two Record-Based File

        std::string fileNameLarge = fileName + "_large";
        if (!fileExists(fileNameLarge)) {
            // Create a file
            ASSERT_EQ(rbfm.createFile(fileNameLarge), success) << "Creating the file should succeed: " << fileName;
            ASSERT_TRUE(fileExists(fileNameLarge)) << "The file is not found: " << fileName;

        }

        // Open the file
        PeterDB::FileHandle fileHandleLarge;
        ASSERT_EQ(rbfm.openFile(fileNameLarge, fileHandleLarge), success)
                                    << "Opening the file should succeed: " << fileName;

        inBuffer = malloc(PAGE_SIZE);
        memset(inBuffer, 0, PAGE_SIZE);

        int numRecords = 16000;

        std::vector<PeterDB::Attribute> recordDescriptor, recordDescriptorLarge;

        // Each varchar field length - 500
        createRecordDescriptorForTwitterUser(recordDescriptor);

        // Each varchar field length - 800
        createRecordDescriptorForTwitterUser2(recordDescriptorLarge);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        PeterDB::RID rid;

        // Insert records into file
        for (unsigned i = 0; i < numRecords; i++) {
            // Test insert Record
            size_t size;
            memset(inBuffer, 0, 3000);
            prepareLargeRecordForTwitterUser((int) recordDescriptor.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";
            ASSERT_EQ(rbfm.insertRecord(fileHandleLarge, recordDescriptorLarge, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";

            if (i % 1000 == 0 && i != 0) {
                GTEST_LOG_(INFO) << i << "/" << numRecords << " records are inserted.";
                ASSERT_TRUE(compareFileSizes(fileName, fileNameLarge)) << "Files should be the same size";
            }

        }

        // Close the file
        ASSERT_EQ(rbfm.closeFile(fileHandleLarge), success) << "Closing the file should succeed.";

        // Destroy the file
        ASSERT_EQ(rbfm.destroyFile(fileNameLarge), success) << "Destroying the file should succeed.";
    }

    TEST_F(RBFM_Test_2, insert_records_with_empty_and_null_varchar) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with an empty VARCHAR field (not NULL)
        // 4. insertRecord() - with a NULL VARCHAR field
        // 5. Close File
        // 6. Destroy File

        PeterDB::RID rid;
        size_t recordSize;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a record into a file - referred_topics is an empty string - "", not null value.
        prepareRecordForTweetMessage((int) recordDescriptor.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "",
                                     inBuffer, recordSize);
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid,
                                  outBuffer), success) << "Reading a record should succeed.";

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading empty VARCHAR incorrectly.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("tweetid: 1234, referred_topics: , message_text: , userid: 999, hash_tags: ", stream.str());

        memset(inBuffer, 0, 2000);

        free(nullsIndicator);
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);
        setAttrNull(nullsIndicator, 1, true);
        setAttrNull(nullsIndicator, 4, true);

        // Insert a record
        prepareRecordForTweetMessage((int) recordDescriptor.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "", inBuffer,
                                     recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL VARCHAR incorrectly.";

        // An NULL should be printed for the referred_topics field.
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("tweetid: 1234, referred_topics: NULL, message_text: , userid: 999, hash_tags: NULL",
                         stream.str());

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";

    }

    TEST_F(RBFM_Test_2, insert_records_with_all_nulls) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with all NULLs
        // 4. Close File
        // 5. Destroy File

        PeterDB::RID rid;
        size_t recordSize;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // set all fields as NULL
        nullsIndicator[0] = 248; // 11111000

        // Insert a record into a file
        prepareRecordForTweetMessage((int) recordDescriptor.size(), nullsIndicator, 1234, 9, "wildfires", 42,
                                     "Curious ... did the amazon wildfires stop?", 999, 3, "wow", inBuffer,
                                     recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        rids.push_back(rid);
        writeRIDsToDisk(rids);
        destroyFile = false;

    }

    TEST_F(RBFM_Test_2, read_records_with_all_nulls) {

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        readRIDsFromDisk(rids, 1);
        rid = rids[0];

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // set all fields as NULL
        nullsIndicator[0] = 248; // 11111000

        // Insert a record into a file
        prepareRecordForTweetMessage((int) recordDescriptor.size(), nullsIndicator, 1234, 9, "wildfires", 43,
                                     "Curious ... did the amazon wildfires stop?", 999, 3, "wow", inBuffer,
                                     recordSize);

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("tweetid: NULL, referred_topics: NULL, message_text: NULL, userid: NULL, hash_tags: NULL",
                         stream.str());


        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL fields incorrectly.";

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

    TEST_F(RBFM_Test_2, insert_records_with_selected_nulls) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with all NULLs
        // 4. Close File
        // 5. Destroy File


        PeterDB::RID rid;
        unsigned recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor3(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Setting the following bytes as NULL
        // The entire byte representation is: 100011011000001111001000
        //                                    123456789012345678901234
        nullsIndicator[0] = 157; // 10011101
        nullsIndicator[1] = 130; // 10000010
        nullsIndicator[2] = 75;  // 01001011

        // Insert a record into a file
        prepareLargeRecord3((int) recordDescriptor.size(), nullsIndicator, 8, inBuffer, &recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        rids.push_back(rid);
        writeRIDsToDisk(rids);
        destroyFile = false;

    }

    TEST_F(RBFM_Test_2, read_records_with_selected_nulls) {

        PeterDB::RID rid;
        unsigned recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor3(recordDescriptor);

        readRIDsFromDisk(rids, 1);
        rid = rids[0];

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Setting the following bytes as NULL
        // The entire byte representation is: 100011011000001111001000
        //                                    123456789012345678901234
        nullsIndicator[0] = 157; // 10011101
        nullsIndicator[1] = 130; // 10000010
        nullsIndicator[2] = 75;  // 01001011

        // Insert a record into a file
        prepareLargeRecord3((int) recordDescriptor.size(), nullsIndicator, 8, inBuffer, &recordSize);

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord(
                "attr0: NULL, attr1: 8, attr2: 5.001, attr3: NULL, attr4: NULL, attr5: NULL, attr6: JJJJJJ, attr7: NULL, attr8: NULL, attr9: MMMMMMMMM, attr10: 8, attr11: 14.001, attr12: PPPPPPPPPPPP, attr13: 8, attr14: NULL, attr15: SSSSSSSSSSSSSSS, attr16: 8, attr17: NULL, attr18: VVVVVVVVVVVVVVVVVV, attr19: 8, attr20: NULL, attr21: YYYYYYYYYYYYYYYYYYYYY, attr22: NULL, attr23: NULL",
                stream.str());

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL fields incorrectly.";

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

    TEST_F(RBFM_Test_2, insert_large_records) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - a big sized record so that two records cannot fit in a page.
        // 4. Close File
        // 5. Destroy File

        std::vector<PeterDB::RID> rids;

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(3000);
        outBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        unsigned numRecords = 15;

        for (int i = 0; i < numRecords; i++) {
            // Insert a record into the file
            prepareLargeRecord4((int) recordDescriptor.size(), nullsIndicator, 2061,
                                inBuffer, recordSize);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";

            rids.push_back(rid);
        }


        auto fileSize = getFileSize(fileName);
        std::cout<<fileSize<<std::endl;
        // check for file size, (at least 15 pages, possibly with some addition hidden pages)
        ASSERT_TRUE(fileSize >= numRecords * PAGE_SIZE && fileSize <= (numRecords + 3) * PAGE_SIZE)
            << "File Size does not match.";

        auto pageCount = fileHandle.getNumberOfPages();

        // check for page count, page count (excluding hidden pages) should be exactly 15
        ASSERT_EQ(pageCount, numRecords) << "Page count does not match.";

        writeRIDsToDisk(rids);

        destroyFile = false;

    }

    TEST_F(RBFM_Test_2, read_large_records) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - a big sized record so that two records cannot fit in a page.
        // 4. Close File
        // 5. Destroy File

        size_t recordSize = 0;
        inBuffer = malloc(3000);
        outBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        unsigned numRecords = 15;
        std::vector<PeterDB::RID> rids;

        readRIDsFromDisk(rids, numRecords);

        for (int i = 0; i < numRecords; i++) {
            // Insert a record into the file
            prepareLargeRecord4((int) recordDescriptor.size(), nullsIndicator, 2061,
                                inBuffer, recordSize);

            // Given the rid, read the record from file
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading fields incorrectly.";

        }
    }

    TEST_F(RBFM_Test_2, insert_to_trigger_fill_lookup_and_append) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - checks if we can't find enough space in the last page,
        //                     the system needs to append a new page
        // 4. Close File
        // 5. Destroy File

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        int numRecords = 30;

        // Insert 30 records into the file
        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 3000);
            prepareLargeRecord4((int) recordDescriptor.size(), nullsIndicator, 100, inBuffer, recordSize);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";
        }

        auto fileSizeBefore = getFileSize(fileName);

        auto pageCountBefore = fileHandle.getNumberOfPages();

        // One more insertion
        memset(inBuffer, 0, 3000);
        prepareLargeRecord4((int) recordDescriptor.size(), nullsIndicator, 1800, inBuffer, recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        auto fileSizeAfter = getFileSize(fileName);
        auto pageCountAfter = fileHandle.getNumberOfPages();

        ASSERT_EQ(fileSizeAfter - fileSizeBefore, PAGE_SIZE) << "File size does not match";
        ASSERT_EQ(pageCountAfter - pageCountBefore, 1) << "Page count does not match";

    }

    TEST_F(RBFM_Test_2, insert_massive_records) {
        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Insert 160000 records into File

        PeterDB::RID rid;
        inBuffer = malloc(1000);
        outBuffer = malloc(1000);
        memset(inBuffer, 0, 1000);
        memset(outBuffer, 0, 1000);

        int numRecords = 160000;
        int batchSize = 5000;
        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptorForTwitterUser;

        createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptorForTwitterUser);

        // Insert numRecords records into the file
        for (unsigned i = 0; i < numRecords / batchSize; i++) {
            for (unsigned j = 0; j < batchSize; j++) {
                memset(inBuffer, 0, 1000);
                size_t size = 0;
                prepareLargeRecordForTwitterUser((int) recordDescriptorForTwitterUser.size(), nullsIndicator,
                                                 i * batchSize + j, inBuffer, size);
                ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptorForTwitterUser, inBuffer, rid), success)
                                            << "Inserting a record for the file should succeed: " << fileName;
                rids.push_back(rid);
            }

            if (i % 5 == 0 && i != 0) {
                GTEST_LOG_(INFO) << i << " / " << numRecords / batchSize << " batches (" << numRecords
                                 << " records) inserted so far for file: " << fileName;
            }
        }
        writeRIDsToDisk(rids);
        destroyFile = false;
    }

    TEST_F(RBFM_Test_2, read_massive_records) {
        // Functions Tested:
        // 1. Read 160000 records from File

        int numRecords = 160000;
        inBuffer = malloc(1000);
        outBuffer = malloc(1000);
        memset(inBuffer, 0, 1000);
        memset(outBuffer, 0, 1000);

        std::vector<PeterDB::Attribute> recordDescriptorForTwitterUser;
        createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptorForTwitterUser);

        std::vector<PeterDB::RID> rids;
        readRIDsFromDisk(rids, numRecords);

        PeterDB::RID rid;
        ASSERT_EQ(rids.size(), numRecords);
        // Compare records from the disk read with the record created from the method
        for (unsigned i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            rid = rids[i];
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptorForTwitterUser, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";
            size_t size;
            prepareLargeRecordForTwitterUser((int) recordDescriptorForTwitterUser.size(), nullsIndicator, i, inBuffer, size);
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "Reading unmatched data.";
        }
    }

}// namespace PeterDBTesting