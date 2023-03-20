#include "test/utils/rm_test_util.h"

namespace PeterDBTesting {
    TEST_F(RM_Catalog_Test, create_and_delete_tables) {

        // Try to delete the System Catalog.
        // If this is the first time, it will generate an error. It's OK and we will ignore that.
        rm.deleteCatalog();

        std::string tableName = "should_not_be_created";
        // Delete the actual file
        remove(tableName.c_str());

        // Create a table should not succeed without Catalog
        std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                "CREATE TABLE " + tableName + " (field1 INT, field2 REAL, field3 VARCHAR(20), field4 VARCHAR(90))");

        ASSERT_NE(rm.createTable(tableName, table_attrs), success)
                                    << "Create table " << tableName << " should not succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";

        // Create Catalog
        ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";

        for (int i = 1; i < 5; i++) {
            tableName = "rm_test_table_" + std::to_string(i);

            table_attrs = parseDDL(
                    "CREATE TABLE " + tableName + " (emp_name VARCHAR(40), age INT, height REAL, salary REAL))");
            // Delete the actual file
            remove(tableName.c_str());

            // Create a table
            ASSERT_EQ(rm.createTable(tableName, table_attrs), success)
                                        << "Create table " << tableName << " should succeed.";

            ASSERT_TRUE(fileExists(tableName)) << "Table " << tableName << " file should exist now.";

        }


        for (int i = 1; i < 5; i++) {
            tableName = "rm_test_table_" + std::to_string(i);
            // Delete the table
            ASSERT_EQ(rm.deleteTable(tableName), success) << "Delete table " << tableName << " should succeed.";
            ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";
        }

        // Delete the non-existence table
        tableName = "non_existence_table";
        ASSERT_NE(rm.deleteTable(tableName), success)
                                    << "Delete non-existence table " << tableName << " should not succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "Table " << tableName << " file should not exist now.";

        // Delete Catalog
        ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

    }

    TEST_F(RM_Tuple_Test, get_attributes) {
        // Functions Tested
        // 1. getAttributes


        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";



        ASSERT_EQ(attrs[0].name, "emp_name") << "Attribute is not correct.";
        ASSERT_EQ(attrs[0].type, PeterDB::TypeVarChar) << "Attribute is not correct.";
        ASSERT_EQ(attrs[0].length, 50) << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].name, "age") << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].type, PeterDB::TypeInt) << "Attribute is not correct.";
        ASSERT_EQ(attrs[1].length, 4) << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].name, "height") << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].type, PeterDB::TypeReal) << "Attribute is not correct.";
        ASSERT_EQ(attrs[2].length, 4) << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].name, "salary") << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].type, PeterDB::TypeReal) << "Attribute is not correct.";
        ASSERT_EQ(attrs[3].length, 4) << "Attribute is not correct.";

    }

    TEST_F(RM_Tuple_Test, insert_and_read_tuple) {
        // Functions tested
        // 1. Insert Tuple
        // 2. Read Tuple


        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert a tuple into a table
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 27;
        float height = 169.2;
        float salary = 9999.99;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";


        // Given the rid, read the tuple from table
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success) << "Print tuple should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 27, height: 169.2, salary: 9999.99",
                         stream.str());



        // Check the returned tuple
        ASSERT_EQ(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple is not the same as the inserted.";
    }

    TEST_F(RM_Tuple_Test, insert_and_delete_and_read_tuple) {
        // Functions Tested
        // 1. Insert tuple
        // 2. Delete Tuple **
        // 3. Read Tuple

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert the Tuple
        std::string name = "Peter";
        size_t nameLength = name.length();
        unsigned age = 18;
        float height = 157.8;
        float salary = 890.2;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Peter, age: 18, height: 157.8, salary: 890.2",
                         stream.str());

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Delete the tuple
        ASSERT_EQ(rm.deleteTuple(tableName, rid), success)
                                    << "RelationManager::deleteTuple() should succeed.";

        // Read Tuple after deleting it - should not succeed
        memset(outBuffer, 0, 200);
        ASSERT_NE(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "Reading a deleted tuple should not succeed.";

        // Check the returned tuple
        ASSERT_NE(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple should not match the inserted.";

    }

    TEST_F(RM_Tuple_Test, insert_and_update_and_read_tuple) {
        // Functions Tested
        // 1. Insert Tuple
        // 2. Update Tuple
        // 3. Read Tuple

        size_t tupleSize = 0;
        size_t updatedTupleSize = 0;
        PeterDB::RID updatedRID;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);


        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 28;
        float height = 164.7;
        float salary = 7192.8;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        std::ostringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Paul, age: 28, height: 164.7, salary: 7192.8",
                         stream.str());

        // Test Update Tuple
        memset(inBuffer, 0, 200);
        prepareTuple((int) attrs.size(), nullsIndicator, 7, "Barbara", age, height, 12000, inBuffer, updatedTupleSize);
        ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::updateTuple() should succeed.";

        // Test Read Tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.printTuple(attrs, inBuffer, stream), success) << "Print tuple should succeed.";
        checkPrintRecord("emp_name: Barbara, age: 28, height: 164.7, salary: 12000",
                         stream.str());

        // Check the returned tuple
        ASSERT_EQ(memcmp(inBuffer, outBuffer, updatedTupleSize), 0)
                                    << "The returned tuple is not the same as the updated.";

    }

    TEST_F(RM_Tuple_Test, read_attribute) {
        // Functions Tested
        // 1. Insert tuples
        // 2. Read Attribute

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);


        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 57;
        float height = 165.5;
        float salary = 480000;

        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Test Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        float returnedSalary = *(float *) ((uint8_t *) outBuffer + 1);
        ASSERT_EQ(salary, returnedSalary) << "The returned salary does not match the inserted.";

        // Test Read Attribute
        memset(outBuffer, 0, 200);
        ASSERT_EQ(rm.readAttribute(tableName, rid, "age", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
        ASSERT_EQ(age, returnedAge) << "The returned age does not match the inserted.";

    }

    TEST_F(RM_Tuple_Test, delete_table) {
        // Functions Tested
        // 0. Insert tuple;
        // 1. Read Tuple
        // 2. Delete Table
        // 3. Read Tuple
        // 4. Insert Tuple

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Test Insert the Tuple
        std::string name = "Paul";
        size_t nameLength = name.length();
        unsigned age = 28;
        float height = 165.5;
        float salary = 7000;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";


        // Test Read Tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() should succeed.";

        // Test Delete Table
        ASSERT_EQ(rm.deleteTable(tableName), success)
                                    << "RelationManager::deleteTable() should succeed.";

        // Reading a tuple on a deleted table
        memset(outBuffer, 0, 200);
        ASSERT_NE(rm.readTuple(tableName, rid, outBuffer), success)
                                    << "RelationManager::readTuple() on a deleted table should not succeed.";

        // Inserting a tuple on a deleted table
        ASSERT_NE(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() on a deleted table should not succeed.";

        // Check the returned tuple
        ASSERT_NE(memcmp(inBuffer, outBuffer, tupleSize), 0) << "The returned tuple should not match the inserted.";

        destroyFile = false; // the table is already deleted.

    }


    TEST_F(RM_Scan_Test, simple_scan) {
        // Functions Tested
        // 1. Simple scan

        int numTuples = 100;
        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Test Insert Tuple
        PeterDB::RID rids[numTuples];
        std::set<unsigned> ages;
        for (int i = 0; i < numTuples; i++) {
            // Insert Tuple
            auto height = (float) i;
            unsigned age = 20 + i;
            prepareTuple((int) attrs.size(), nullsIndicator, 6, "Tester", age, height, (float) (age * 12.5), inBuffer, tupleSize);
            ages.insert(age);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;
            memset(inBuffer, 0, 200);
        }

        // Set up the iterator
        std::vector<std::string> attributes{"age"};

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
            auto target = ages.find(returnedAge);
            ASSERT_NE(target, ages.end()) << "Returned age is not from the inserted ones.";
            ages.erase(target);
        }
        
    }

    TEST_F(RM_Scan_Test, simple_scan_after_table_deletion) {
        // Functions Tested
        // 1. Simple scan
        // 2. Delete the given table
        // 3. Simple scan

        int numTuples = 65536;
        outBuffer = malloc(200);

        std::set<int> ages;

        for (int i = 0; i < numTuples; i++) {
            int age = 50 - i;
            ages.insert(age);
        }

        // Set up the iterator
        std::vector<std::string> attributes{"age"};

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            unsigned returnedAge = *(unsigned *) ((uint8_t *) outBuffer + 1);
            auto target = ages.find((int) returnedAge);
            ASSERT_NE(target, ages.end()) << "Returned age is not from the inserted ones.";
            ages.erase(target);
        }

        // Close the iterator
        rmsi.close();

        // Delete a Table
        ASSERT_EQ(rm.deleteTable(tableName), success) << "RelationManager::deleteTable() should succeed.";

        // Scan on a deleted table
        ASSERT_NE(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "RelationManager::scan() should not succeed on a deleted table.";

        destroyFile = false; // the table is already deleted.

    }

    TEST_F(RM_Large_Table_Test, insert_large_tuples) {
        // Functions Tested for large tables:
        // 1. getAttributes
        // 2. insert tuple

        // Remove the leftover files from previous runs
        remove("rid_files");
        remove("size_files");
        remove(tableName.c_str());

        // Try to delete the System Catalog.
        // If this is the first time, it will generate an error. It's OK and we will ignore that.
        rm.deleteCatalog();

        // Create Catalog
        ASSERT_EQ(rm.createCatalog(), success) << "Creating the Catalog should succeed.";
        createLargeTable(tableName);

        inBuffer = malloc(bufSize);
        int numTuples = 5000;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert numTuples tuples into table
        for (int i = 0; i < numTuples; i++) {
            // Test insert Tuple
            size_t size = 0;
            memset(inBuffer, 0, bufSize);
            prepareLargeTuple((int) attrs.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            sizes.emplace_back(size);
        }

        writeRIDsToDisk(rids);
        writeSizesToDisk(sizes);

    }

    TEST_F(RM_Large_Table_Test, read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. read tuple

        size_t size = 0;
        int numTuples = 5000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        // read the saved rids and the sizes of records
        readRIDsFromDisk(rids, numTuples);
        readSizesFromDisk(sizes, numTuples);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            size = 0;
            prepareLargeTuple((int) attrs.size(), nullsIndicator, i, inBuffer, size);
            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the inserted tuple";

        }

    }

    TEST_F(RM_Large_Table_Test, update_and_read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. update tuple
        // 2. read tuple

        int numTuples = 5000;
        unsigned numTuplesToUpdate1 = 2000;
        unsigned numTuplesToUpdate2 = 2000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        readRIDsFromDisk(rids, numTuples);
        readSizesFromDisk(sizes, numTuples);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Update the first numTuplesToUpdate1 tuples
        size_t size = 0;
        for (int i = 0; i < numTuplesToUpdate1; i++) {
            memset(inBuffer, 0, bufSize);
            rid = rids[i];

            prepareLargeTuple((int) attrs.size(), nullsIndicator, i + 10, inBuffer, size);

            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::updateTuple() should succeed.";

            sizes[i] = size;
            rids[i] = rid;
        }

        // Update the last numTuplesToUpdate2 tuples
        for (unsigned i = numTuples - numTuplesToUpdate2; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            rid = rids[i];

            prepareLargeTuple((int) attrs.size(), nullsIndicator, i - 10, inBuffer, size);

            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::updateTuple() should succeed.";

            sizes[i] = size;
            rids[i] = rid;
        }

        // Read the updated records and check the integrity
        for (unsigned i = 0; i < numTuplesToUpdate1; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple((int) attrs.size(), nullsIndicator, i + 10, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the updated tuple";

        }

        for (unsigned i = numTuples - numTuplesToUpdate2; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple((int) attrs.size(), nullsIndicator, i - 10, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the updated tuple";

        }

        // Read the non-updated records and check the integrity
        for (unsigned i = numTuplesToUpdate1; i < numTuples - numTuplesToUpdate2; i++) {
            memset(inBuffer, 0, bufSize);
            memset(outBuffer, 0, bufSize);
            prepareLargeTuple((int) attrs.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "the read tuple should match the inserted tuple";

        }

    }

    TEST_F(RM_Large_Table_Test, delete_and_read_large_tuples) {
        // This test is expected to be run after RM_Large_Table_Test::insert_large_tuples

        // Functions Tested for large tables:
        // 1. delete tuple
        // 2. read tuple

        unsigned numTuples = 5000;
        unsigned numTuplesToDelete = 2000;
        outBuffer = malloc(bufSize);

        readRIDsFromDisk(rids, numTuples);

        // Delete the first numTuplesToDelete tuples
        for (unsigned i = 0; i < numTuplesToDelete; i++) {


            ASSERT_EQ(rm.deleteTuple(tableName, rids[i]), success) << "RelationManager::deleteTuple() should succeed.";
            //std::cerr<<i<<std::endl;
        }

        // Try to read the first numTuplesToDelete deleted tuples
        for (unsigned i = 0; i < numTuplesToDelete; i++) {

            ASSERT_NE(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() on a deleted tuple should not succeed.";

        }

        // Read the non-deleted tuples
        for (unsigned i = numTuplesToDelete; i < numTuples; i++) {
            ASSERT_EQ(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should succeed.";

        }

    }

    TEST_F(RM_Large_Table_Test, scan_large_tuples) {

        // Functions Tested for large tables
        // 1. scan

        destroyFile = true;   // To clean up after test.

        std::vector<std::string> attrs{
                "attr29", "attr15", "attr25"
        };

        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attrs, rmsi), success) <<
                                                                                      "RelationManager::scan() should succeed.";

        unsigned count = 0;
        outBuffer = malloc(bufSize);

        size_t nullAttributesIndicatorActualSize = getActualByteForNullsIndicator((int) attrs.size());

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            if(count==2999){

            }
            size_t offset = 0;

            float attr29 = *(float *) ((uint8_t *) outBuffer + nullAttributesIndicatorActualSize);
            offset += 4;

            unsigned size = *(unsigned *) ((uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize);
            offset += 4;

            auto *attr15 = (uint8_t *) malloc(size + 1);
            memcpy(attr15, (uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize, size);
            attr15[size] = 0;
            offset += size;
            unsigned char target;
            for (size_t k = 0; k < size; k++) {
                if (k == 0) {
                    target = attr15[k];
                } else {
                    ASSERT_EQ(target, attr15[k]) << "Scanned VARCHAR has incorrect value";
                }
            }
            unsigned attr25 = *(unsigned *) ((uint8_t *) outBuffer + offset + nullAttributesIndicatorActualSize);

            ASSERT_EQ(attr29, attr25 + 1);
            free(attr15);
            count++;
            memset(outBuffer, 0, bufSize);
        }

        ASSERT_EQ(count, 3000) << "Number of scanned tuples is incorrect.";

    }

    TEST_F(RM_Scan_Test, conditional_scan) {
        // Functions Tested:
        // 1. Conditional scan

        bufSize = 100;
        size_t tupleSize = 0;
        unsigned numTuples = 1500;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        unsigned ageVal = 25;
        unsigned age;

        PeterDB::RID rids[numTuples];
        std::vector<uint8_t *> tuples;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            auto height = (float) i;

            age = (rand() % 10) + 23;

            prepareTuple((int) attrs.size(), nullsIndicator, 6, "Tester", age, height, 123, inBuffer, tupleSize);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;
        }

        // Set up the iterator
        std::string attr = "age";
        std::vector<std::string> attributes{attr};

        ASSERT_EQ(rm.scan(tableName, attr, PeterDB::GT_OP, &ageVal, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            age = *(unsigned *) ((uint8_t *) outBuffer + 1);
            ASSERT_GT(age, ageVal) << "Returned value from a scan is not correct.";
            memset(outBuffer, 0, bufSize);
        }
    }

    TEST_F(RM_Scan_Test, conditional_scan_with_null) {
        // Functions Tested:
        // 1. Conditional scan - including NULL values

        bufSize = 200;
        size_t tupleSize = 0;
        unsigned numTuples = 1500;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        unsigned ageVal = 25;
        unsigned age;

        PeterDB::RID rids[numTuples];
        std::vector<uint8_t *> tuples;
        std::string tupleName;

        bool nullBit;

        // GetAttributes
        std::vector<PeterDB::Attribute> attrs;
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicatorWithNull = initializeNullFieldsIndicator(attrs);

        // age field : NULL
        nullsIndicatorWithNull[0] = 64; // 01000000

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            auto height = (float) i;

            age = (rand() % 20) + 15;

            std::string suffix = std::to_string(i);

            if (i % 10 == 0) {
                tupleName = "TesterNull" + suffix;
                prepareTuple((int) attrs.size(), nullsIndicatorWithNull, tupleName.length(), tupleName, 0, height, 456,
                             inBuffer,
                             tupleSize);
            } else {
                tupleName = "Tester" + suffix;
                prepareTuple((int) attrs.size(), nullsIndicator, tupleName.length(), tupleName, age, height, 123, inBuffer,
                             tupleSize);
            }
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";

            rids[i] = rid;

        }

        // Set up the iterator
        std::string attr = "age";
        std::vector<std::string> attributes{attr};
        ASSERT_EQ(rm.scan(tableName, attr, PeterDB::GT_OP, &ageVal, attributes, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            // Check the first bit of the returned data since we only return one attribute in this test case
            // However, the age with NULL should not be returned since the condition NULL > 25 can't hold.
            // All comparison operations with NULL should return FALSE
            // (e.g., NULL > 25, NULL >= 25, NULL <= 25, NULL < 25, NULL == 25, NULL != 25: ALL FALSE)
            nullBit = *(bool *) ((uint8_t *) outBuffer) & ((unsigned) 1 << (unsigned) 7);
            ASSERT_FALSE(nullBit) << "NULL value should not be returned from a scan.";

            age = *(unsigned *) ((uint8_t *) outBuffer + 1);
            ASSERT_GT(age, ageVal) << "Returned value from a scan is not correct.";
            memset(outBuffer, 0, bufSize);

        }

    }

    TEST_F(RM_Catalog_Scan_Test, catalog_tables_table_check) {
        // Functions Tested:
        // 1. System Catalog Implementation - Tables table
        // Get Catalog Attributes

        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";


        // There should be at least three attributes: table-id, table-name, file-name
        ASSERT_GE((int) attrs.size(), 3) << "Tables table should have at least 3 attributes.";

        std::vector<std::string> expectedAttrs {"table-id", "table-name", "file-name"};
        std::vector<std::string> actualAttrs;
        std::for_each(attrs.begin(), attrs.end(),
                      [&](const PeterDB::Attribute& attr){actualAttrs.push_back(attr.name);});
        std::sort(expectedAttrs.begin(), expectedAttrs.end());
        std::sort(actualAttrs.begin(), actualAttrs.end());

        ASSERT_TRUE(std::includes(actualAttrs.begin(), actualAttrs.end(),
                                  expectedAttrs.begin(), expectedAttrs.end()))
                                  << "Tables table's schema is not correct.";

        PeterDB::RID rid;
        bufSize = 1000;
        outBuffer = malloc(bufSize);

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        projected_attrs.reserve((int) attrs.size());
        for (PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }

        ASSERT_EQ(rm.scan("Tables", "", PeterDB::NO_OP, nullptr, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        int count = 0;

        // Check Tables table
        checkCatalog("table-id: x, table-name: Tables, file-name: Tables");

        // Check Columns table
        checkCatalog("table-id: x, table-name: Columns, file-name: Columns");

        // Keep scanning the remaining records
        memset(outBuffer, 0, bufSize);

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least one more table
        ASSERT_GE(count, 1) << "There should be at least one more table.";

        // Deleting the catalog should fail.
        ASSERT_NE(rm.deleteTable("Tables"),
                  success && "RelationManager::deleteTable() on the system catalog table should not succeed.");

    }

    TEST_F(RM_Catalog_Scan_Test, catalog_columns_table_check) {

        // Functions Tested:
        // 1. System Catalog Implementation - Columns table

        // Get Catalog Attributes
        ASSERT_EQ(rm.getAttributes("Columns", attrs), success)
                                    << "RelationManager::getAttributes() should succeed.";

        // There should be at least five attributes: table-id, column-name, column-type, column-length, column-position
        std::vector<std::string> expectedAttrs {"table-id", "column-name", "column-type", "column-length", "column-position"};
        std::vector<std::string> actualAttrs;
        std::for_each(attrs.begin(), attrs.end(),
                      [&](const PeterDB::Attribute& attr){actualAttrs.push_back(attr.name);});
        std::sort(expectedAttrs.begin(), expectedAttrs.end());
        std::sort(actualAttrs.begin(), actualAttrs.end());

        ASSERT_GE((int) attrs.size(), 5) << "Columns table should have at least 5 attributes.";
        ASSERT_TRUE(std::includes(actualAttrs.begin(), actualAttrs.end(),
                                  expectedAttrs.begin(), expectedAttrs.end()))
                                    << "Columns table's schema is not correct.";

        bufSize = 1000;
        outBuffer = malloc(bufSize);

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        for (const PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }

        ASSERT_EQ(rm.scan("Columns", "", PeterDB::NO_OP, nullptr, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";

        // Check Tables table
        checkCatalog("table-id: x, column-name: table-id, column-type: 0, column-length: 4, column-position: 1");
        checkCatalog("table-id: x, column-name: table-name, column-type: 2, column-length: 50, column-position: 2");
        checkCatalog("table-id: x, column-name: file-name, column-type: 2, column-length: 50, column-position: 3");

        // Check Columns table
        checkCatalog("table-id: x, column-name: table-id, column-type: 0, column-length: 4, column-position: 1");
        checkCatalog(
                "table-id: x, column-name: column-name, column-type: 2, column-length: 50, column-position: 2");
        checkCatalog("table-id: x, column-name: column-type, column-type: 0, column-length: 4, column-position: 3");
        checkCatalog(
                "table-id: x, column-name: column-length, column-type: 0, column-length: 4, column-position: 4");
        checkCatalog(
                "table-id: x, column-name: column-position, column-type: 0, column-length: 4, column-position: 5");


        // Keep scanning the remaining records
        unsigned count = 0;
        memset(outBuffer, 0, bufSize);
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least 4 more records for created table
        ASSERT_GE(count, 4) << "at least 4 more records for " << tableName;

        // Deleting the catalog should fail.
        ASSERT_NE(rm.deleteTable("Columns"),
                  success && "RelationManager::deleteTable() on the system catalog table should not succeed.");

    }


    TEST_F(RM_Catalog_Scan_Test_2, read_attributes) {
        // Functions tested
        // 1. Insert 100,000 tuples
        // 2. Read Attribute

        bufSize = 1000;
        size_t tupleSize = 0;
        int numTuples = 100000;

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::default_random_engine generator(std::random_device{}());
        std::uniform_int_distribution<unsigned> dist8(0, 7);
        std::uniform_int_distribution<unsigned> dist256(0, 255);


        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicators.clear();
        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            nullsIndicator[0] = dist256(generator);
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            nullsIndicators.emplace_back(nullsIndicator[0]);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far." << std::endl;
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted." << std::endl;

        // validate a attribute of each tuple randomly
        for (int i = 0; i < numTuples; i = i + 10) {
            unsigned attrID = dist8(generator);
            validateAttribute(attrID, i, i, i + 100);

        }
    }

    TEST_F(RM_Catalog_Scan_Test_2, scan) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. scan - NO_OP
        // 3. scan - GT_OP

        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> user_ids;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            lngs.emplace_back(tweet.lng);
            if (tweet.hash_tags > "A") {
                user_ids.emplace_back(tweet.user_id);
            }
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";
        // Set up the iterator
        std::vector<std::string> attributes{"lng", "lat"};

        // Scan
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        float latReturned, lngReturned;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            latReturned = *(float *) ((char *) outBuffer + 5);
            lngReturned = *(float *) ((char *) outBuffer + 1);

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            ASSERT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);
            auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

            ASSERT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
            lngs.erase(targetLng);

        }

        ASSERT_TRUE(lats.empty()) << "returned lat does not match inserted";
        ASSERT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

        char value[5] = {0, 0, 0, 0, 'A'};
        unsigned msgLength = 1;
        memcpy((char *) value, &msgLength, sizeof(unsigned));
        // Scan
        attributes = {"user_id"};
        ASSERT_EQ(rm.scan(tableName, "hash_tags", PeterDB::GT_OP, value, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            unsigned userIdReturned = *(unsigned *) ((char *) outBuffer + 1);
            auto targetUserId = std::find(user_ids.begin(), user_ids.end(), userIdReturned);
            ASSERT_NE(targetUserId, user_ids.end()) << "returned user_id value is not from inserted.";
            user_ids.erase(targetUserId);

        }

        ASSERT_TRUE(user_ids.empty()) << "returned user_id does not match inserted";

    }

    TEST_F(RM_Catalog_Scan_Test_2, scan_with_null) {
        // Functions tested
        // 1. insert 100,000 tuples - will nulls
        // 2. scan - NO_OP
        // 3. scan - LE_OP

        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> tweet_ids;
        float targetSentiment = 71234.5;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple

            // make some tuple to have null fields
            if (i % 37 == 0) {
                nullsIndicator[0] = 53; // 00110101
            } else {
                nullsIndicator[0] = 0; // 00000000
            }

            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            if (i % 37 != 0) {
                lngs.emplace_back(tweet.lng);
            }
            if (tweet.sentiment != -1 && tweet.sentiment <= targetSentiment) {
                tweet_ids.emplace_back(tweet.tweet_id);
            }
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";
        // Set up the iterator
        std::vector<std::string> attributes{"lng", "lat", "user_id"};
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        // Scan
        float latReturned, lngReturned;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            if ((*(char *) outBuffer) >> 7 & 1u) {
                latReturned = *(float *) ((char *) outBuffer + 1);
                lngReturned = -1;
            } else {
                latReturned = *(float *) ((char *) outBuffer + 5);
                lngReturned = *(float *) ((char *) outBuffer + 1);
            }

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            ASSERT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);

            if (lngReturned != -1) {
                auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

                ASSERT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
                lngs.erase(targetLng);
            }

        }
        ASSERT_TRUE(lats.empty()) << "returned lat does not match inserted";
        ASSERT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

        // Scan
        attributes = {"tweet_id"};
        ASSERT_EQ(rm.scan(tableName, "sentiment", PeterDB::LE_OP, &targetSentiment, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            unsigned tweetIdReturned = *(unsigned *) ((char *) outBuffer + 1);
            auto targetTweetId = std::find(tweet_ids.begin(), tweet_ids.end(), tweetIdReturned);
            ASSERT_NE(targetTweetId, tweet_ids.end()) << "returned tweet_id value is not from inserted.";
            tweet_ids.erase(targetTweetId);

        }

        ASSERT_TRUE(tweet_ids.empty()) << "returned tweet_id does not match inserted";

    }

    TEST_F(RM_Catalog_Scan_Test_2, scan_after_update) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. update some tuples
        // 3. scan - NO_OP
        size_t tupleSize;
        bufSize = 1000;
        int numTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        std::vector<float> lats;
        std::vector<float> lngs;
        std::vector<unsigned> user_ids;

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            lats.emplace_back(tweet.lat);
            lngs.emplace_back(tweet.lng);
            rids.emplace_back(rid);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All records have been inserted.";
        // update tuples
        unsigned updateCount = 0;

        for (int i = 0; i < numTuples; i = i + 100) {
            memset(inBuffer, 0, bufSize);

            // Update Tuple
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 100, tupleSize, tweet);
            ASSERT_EQ(rm.updateTuple(tableName, inBuffer, rids[i]), success)
                                        << "RelationManager::updateTuple() should succeed.";
            lats[i] = tweet.lat;
            lngs[i] = tweet.lng;
            updateCount++;
            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << updateCount << "/" << numTuples << " records have been updated so far." << std::endl;
            }
        }
        GTEST_LOG_(INFO) << "All records have been processed - update count: " << updateCount << std::endl;

        // Set up the iterator
        std::vector<std::string> attributes{"lng", "user_id", "lat"};

        // Scan
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        float latReturned, lngReturned;
        unsigned counter=0;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {

            latReturned = *(float *) ((char *) outBuffer + 9);
            lngReturned = *(float *) ((char *) outBuffer + 1);

            auto targetLat = std::find(lats.begin(), lats.end(), latReturned);

            ASSERT_NE(targetLat, lats.end()) << "returned lat value is not from inserted.";
            lats.erase(targetLat);
            auto targetLng = std::find(lngs.begin(), lngs.end(), lngReturned);

            ASSERT_NE(targetLng, lngs.end()) << "returned lnt value is not from inserted.";
            lngs.erase(targetLng);

        }
        ASSERT_TRUE(lats.empty()) << "returned lat does not match inserted";
        ASSERT_TRUE(lngs.empty()) << "returned lng does not match inserted";

        ASSERT_EQ(rmsi.close(), success) << "close iterator should succeed.";

    }

    TEST_F(RM_Catalog_Scan_Test_2, scan_after_delete) {
        // Functions tested
        // 1. insert 100,000 tuples
        // 2. delete tuples
        // 3. scan - NO_OP


        bufSize = 1000;
        size_t tupleSize = 0;
        int numTuples = 100000;

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::default_random_engine generator(std::random_device{}());
        std::uniform_int_distribution<unsigned> dist8(0, 7);
        std::uniform_int_distribution<unsigned> dist256(0, 255);


        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        nullsIndicators.clear();
        for (int i = 0; i < numTuples; i++) {
            memset(inBuffer, 0, bufSize);

            // Insert Tuple
            nullsIndicator[0] = dist256(generator);
            Tweet tweet;
            generateTuple(nullsIndicator, inBuffer, i, i + 78, tupleSize, tweet);
            ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                        << "RelationManager::insertTuple() should succeed.";
            rids.emplace_back(rid);
            nullsIndicators.emplace_back(nullsIndicator[0]);

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << "/" << numTuples << " records have been inserted so far.";
            }
        }
        GTEST_LOG_(INFO) << "All tuples have been inserted.";

        for (int i = 0; i < numTuples; i++) {

            ASSERT_EQ(rm.deleteTuple(tableName, rids[i]), success) << "RelationManager::deleteTuple() should succeed.";

            ASSERT_NE(rm.readTuple(tableName, rids[i], outBuffer), success)
                                        << "RelationManager::readTuple() should not succeed on deleted Tuple.";

            if (i % 10000 == 0) {
                GTEST_LOG_(INFO) << (i + 1) << " / " << numTuples << " have been processed.";
            }
        }
        GTEST_LOG_(INFO) << "All tuples have been deleted.";

        // Set up the iterator
        std::vector<std::string> attributes{"tweet_id", "sentiment"};
        ASSERT_EQ(rm.scan(tableName, "", PeterDB::NO_OP, nullptr, attributes, rmsi), success)
                                    << "relationManager::scan() should succeed.";

        ASSERT_EQ(rmsi.getNextTuple(rid, outBuffer), RM_EOF)
                                    << "RM_ScanIterator::getNextTuple() should not succeed at this point, since there should be no tuples.";

        // Close the iterator
        ASSERT_EQ(rmsi.close(), success) << "RM_ScanIterator should be able to close.";

    }

    TEST_F(RM_Catalog_Scan_Test_2, try_to_modify_catalog) {
        // Functions tested
        // An attempt to modify System Catalogs tables - should no succeed

        bufSize = 1000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Try to insert a row - should not succeed
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        int offset = 1;
        int intValue = 0;
        int varcharLength = 7;
        std::string varcharStr = "Testing";
        float floatValue = 0.0;

        for (auto &attr : attrs) {
            // Generating INT value
            if (attr.type == PeterDB::TypeInt) {
                intValue = 9999;
                memcpy((char *) inBuffer + offset, &intValue, sizeof(int));
                offset += sizeof(int);
            } else if (attr.type == PeterDB::TypeReal) {
                // Generating FLOAT value
                floatValue = 9999.9;
                memcpy((char *) inBuffer + offset, &floatValue, sizeof(float));
                offset += sizeof(float);
            } else if (attr.type == PeterDB::TypeVarChar) {
                // Generating VarChar value
                memcpy((char *) inBuffer + offset, &varcharLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) inBuffer + offset, varcharStr.c_str(), varcharLength);
                offset += varcharLength;
            }
        }

        ASSERT_NE(rm.insertTuple("Tables", inBuffer, rid), success)
                                    << "The system catalog should not be altered by a user's insertion call.";

        // Try to delete the system catalog
        ASSERT_NE (rm.deleteTable("Tables"), success) << "The system catalog should not be deleted by a user call.";


        // GetAttributes
        attrs.clear();
        ASSERT_EQ(rm.getAttributes("Columns", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Try to insert a row - should not succeed
        free(nullsIndicator);
        nullsIndicator = initializeNullFieldsIndicator(attrs);
        memset(inBuffer, 0, bufSize);
        for (auto &attr : attrs) {
            // Generating INT value
            if (attr.type == PeterDB::TypeInt) {
                intValue = 9999;
                memcpy((char *) inBuffer + offset, &intValue, sizeof(int));
                offset += sizeof(int);
            } else if (attr.type == PeterDB::TypeReal) {
                // Generating FLOAT value
                floatValue = 9999.9;
                memcpy((char *) inBuffer + offset, &floatValue, sizeof(float));
                offset += sizeof(float);
            } else if (attr.type == PeterDB::TypeVarChar) {
                // Generating VarChar value
                memcpy((char *) inBuffer + offset, &varcharLength, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) inBuffer + offset, varcharStr.c_str(), varcharLength);
                offset += varcharLength;
            }
        }

        ASSERT_NE(rm.insertTuple("Columns", inBuffer, rid), success)
                                    << "The system catalog should not be altered by a user's insertion call.";

        // Try to delete the system catalog
        ASSERT_NE (rm.deleteTable("Columns"), success) << "The system catalog should not be deleted by a user call.";


        attrs.clear();
        // GetAttributes
        ASSERT_EQ(rm.getAttributes("Tables", attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Set up the iterator
        std::vector<std::string> projected_attrs;
        projected_attrs.reserve((int) attrs.size());
        for (PeterDB::Attribute &attr : attrs) {
            projected_attrs.push_back(attr.name);
        }
        ASSERT_EQ(rm.scan("Tables", "", PeterDB::NO_OP, nullptr, projected_attrs, rmsi), success)
                                    << "RelationManager::scan() should succeed.";


        // Check Tables table
        checkCatalog("table-id: x, table-name: Tables, file-name: Tables");

        // Check Columns table
        checkCatalog("table-id: x, table-name: Columns, file-name: Columns");

        // Keep scanning the remaining records
        memset(outBuffer, 0, bufSize);
        int count = 0;
        while (rmsi.getNextTuple(rid, outBuffer) != RM_EOF) {
            count++;
            memset(outBuffer, 0, bufSize);
        }

        // There should be at least one more table
        ASSERT_GE(count, 1) << "There should be at least one more table.";

    }

    TEST_F(RM_Catalog_Scan_Test_2, create_table_with_same_name) {
        std::vector<PeterDB::Attribute> table_attrs = parseDDL(
                "CREATE TABLE " + tableName +
                " (tweet_id INT, text VARCHAR(400), user_id INT, sentiment REAL, hash_tags VARCHAR(100), embedded_url VARCHAR(200), lat REAL, lng REAL)");
        ASSERT_NE(rm.createTable(tableName, table_attrs), success)
                                    << "Create table " << tableName << " should fail, table should already exist.";

    }

    TEST_F(RM_Version_Test, extra_multiple_add_drop_mix) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185.7;
        float salary = 23333.3;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_FLOAT_EQ(*(float *) ((uint8_t *) outBuffer + 1), salary)
                                    << "Returned height does not match the inserted.";

        // Drop the attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "salary"), success) << "RelationManager::dropAttribute() should succeed.";


        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be greater than the current one.
        ASSERT_GT((int) attrs.size(), attrs2.size()) << "attributes should be less than the previous version.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";
        checkPrintRecord("emp_name: Peter Anteater, age: 24, height: 185.7", stream.str());

        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // Add the Attribute back
        PeterDB::Attribute attr = attrs[3];
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";

        // Drop another attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "age"), success) << "RelationManager::dropAttribute() should succeed.";

        // GetAttributes again
        attrs.clear();
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        ASSERT_EQ((int) attrs.size(), attrs2.size())
                                    << "attributes count should remain the same after dropping and adding one.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, height: 185.7, salary: NULL",
                         stream.str());

    }

    TEST_F(RM_Version_Test, extra_insert_and_read_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185.7;
        float salary = 23333.3;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Drop the Attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "salary"), success) << "RelationManager::dropAttribute() should succeed.";

        // Add the Attribute back
        PeterDB::Attribute attr = attrs[3];
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";

        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        ASSERT_EQ((int) attrs.size(), attrs2.size())
                                    << "attributes count should remain the same after dropping and adding one.";

        std::string name2 = "John Doe";
        size_t nameLength2 = name2.length();
        unsigned age2 = 22;
        float height2 = 178.3;
        float salary2 = 800.23;
        PeterDB::RID rid2;

        prepareTuple(attrs2.size(), nullsIndicator, nameLength2, name2, age2, height2, salary2, inBuffer, tupleSize);
        std::stringstream stream;
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid2), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // read the second tuple
        ASSERT_EQ(rm.readTuple(tableName, rid2, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: John Doe, age: 22, height: 178.3, salary: 800.23", stream.str());

        // read the first tuple
        memset(outBuffer, 0, bufSize);
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 24, height: 185.7, salary: NULL", stream.str());

        // read the second tuple's attribute
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(rm.readAttribute(tableName, rid2, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_EQ(*(char *) outBuffer, 0u) << "returned salary should not be NULL";

        ASSERT_FLOAT_EQ(*(float *) ((char *) outBuffer + 1), 800.23) << "returned salary should match inserted.";

        // read the first tuple's attribute
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(rm.readAttribute(tableName, rid, "salary", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_EQ(*(char *) outBuffer, (char)128u) << "returned salary should be NULL";

    }

    TEST_F(RM_Version_Test, read_after_drop_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 24;
        float height = 185;
        float salary = 23333.3;
        prepareTuple((int) attrs.size(), nullsIndicator, nameLength, name, age, height, salary, inBuffer, tupleSize);
        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Attribute
        ASSERT_EQ(rm.readAttribute(tableName, rid, "height", outBuffer), success)
                                    << "RelationManager::readAttribute() should succeed.";

        ASSERT_FLOAT_EQ(*(float *) ((uint8_t *) outBuffer + 1), height)
                                    << "Returned height does not match the inserted.";

        // Drop the attribute
        ASSERT_EQ(rm.dropAttribute(tableName, "height"), success) << "RelationManager::dropAttribute() should succeed.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        // Get the attribute from the table again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be greater than the current one.
        ASSERT_GT((int) attrs.size(), attrs2.size()) << "attributes should be less than the previous version.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";
        checkPrintRecord("emp_name: Peter Anteater, age: 24, salary: 23333.3", stream.str());
    }

    TEST_F(RM_Version_Test, read_after_add_attribute) {
        // Extra Credit Test Case - Functions Tested:
        // 1. Insert tuple
        // 2. Read Attributes
        // 3. Drop Attributes

        size_t tupleSize = 0;
        inBuffer = malloc(200);
        outBuffer = malloc(200);

        // GetAttributes
        ASSERT_EQ(rm.getAttributes(tableName, attrs), success) << "RelationManager::getAttributes() should succeed.";

        // Test Add Attribute
        PeterDB::Attribute attr{
                "ssn", PeterDB::TypeInt, 4
        };
        ASSERT_EQ(rm.addAttribute(tableName, attr), success) << "RelationManager::addAttribute() should succeed.";


        // GetAttributes again
        std::vector<PeterDB::Attribute> attrs2;
        ASSERT_EQ(rm.getAttributes(tableName, attrs2), success) << "RelationManager::getAttributes() should succeed.";

        // The size of the original attribute vector size should be less than the current one.
        ASSERT_GT(attrs2.size(), (int) attrs.size()) << "attributes should be more than the previous version.";

        // Initialize two NULL field indicators
        nullsIndicator = initializeNullFieldsIndicator(attrs);

        // Insert Tuple
        std::string name = "Peter Anteater";
        size_t nameLength = name.length();
        unsigned age = 34;
        float height = 175.3;
        float salary = 24123.90;
        int ssn = 123479765;

        prepareTupleAfterAdd((int) attrs.size(), nullsIndicator, (int) nameLength, name, age, height, salary, ssn, inBuffer,
                             tupleSize);

        ASSERT_EQ(rm.insertTuple(tableName, inBuffer, rid), success)
                                    << "RelationManager::insertTuple() should succeed.";

        // Read Tuple and print the tuple
        ASSERT_EQ(rm.readTuple(tableName, rid, outBuffer), success) << "RelationManager::readTuple() should succeed.";

        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs2, outBuffer, stream), success)
                                    << "RelationManager::printTuple() should succeed.";

        checkPrintRecord("emp_name: Peter Anteater, age: 34, height: 175.3, salary: 24123.90, ssn: 123479765",
                         stream.str());
    }


} // namespace PeterDBTesting