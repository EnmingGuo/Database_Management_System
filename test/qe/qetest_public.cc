#include "test/utils/qe_test_util.h"

namespace PeterDBTesting {
    TEST_F(QE_Test, create_and_delete_table_with_index) {
        // Tables created: left
        // Indexes created: left.B, left.C
        // 1. Create an Index
        // 2. Load Data
        // 3. Create an Index
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        tableNames.emplace_back(tableName);

        // Create a table
        ASSERT_EQ(rm.createTable(tableName, attrsMap[tableName]), success)
                                    << "Create table " << tableName << " should succeed.";

        // Create an index before inserting tuples.
        ASSERT_EQ(rm.createIndex(tableName, "B"), success) << "RelationManager.createIndex() should succeed.";
        ASSERT_EQ(glob(".idx").size(), 1) << "There should be two index files now.";

        // Insert tuples.
        populateTable(tableName, 100);

        // Create an index after inserting tuples - should reflect the currently existing tuples.
        ASSERT_EQ(rm.createIndex(tableName, "C"), success) << "RelationManager.createIndex() should succeed.";
        ASSERT_EQ(glob(".idx").size(), 2) << "There should be two index files now.";

        destroyFile = false; // prevent from double deletion

        // Destroy the file
        ASSERT_EQ(rm.deleteTable(tableName), success) << "Destroying the file should succeed.";
        ASSERT_FALSE(fileExists(tableName)) << "The file " << tableName << " should not exist now.";
        ASSERT_EQ(glob(".idx").size(), 0) << "There should be no index file now.";

        // Delete Catalog
        ASSERT_EQ(rm.deleteCatalog(), success) << "Deleting the Catalog should succeed.";

    }

    TEST_F(QE_Test, table_scan_with_int_filter) {

        // Filter -- TableScan as input, on an Integer Attribute
        // SELECT * FROM LEFT WHERE B <= 51
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"A", "B", "C"}, 1000);

        PeterDB::TableScan ts(rm, tableName);

        // Set up condition
        unsigned compVal = 51;
        PeterDB::Condition cond{"left.B", PeterDB::LE_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&ts, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned a = i % 203;
            unsigned b = (i + 10) % 197;
            float c = (float) (i % 167) + 50.5f;
            if (b <= 51)
                expected.emplace_back(
                        "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b) + ", left.C: " +
                        std::to_string(c));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

    TEST_F(QE_Test, table_scan_with_varchar_filter) {
        // Mandatory for all
        // 1. Filter -- on TypeVarChar Attribute
        // SELECT * FROM leftvarchar where B = "llllllllllll"

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "leftvarchar";
        createAndPopulateTable(tableName, {"B", "A"}, 1000);

        // Set up TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Set up condition
        PeterDB::Condition cond{"leftvarchar.B", PeterDB::EQ_OP, false, "", {PeterDB::TypeVarChar, inBuffer}};
        unsigned length = 12;
        *(unsigned *) ((char *) cond.rhsValue.data) = length;
        // "llllllllllll"
        for (unsigned i = 0; i < length; ++i) {
            *(char *) ((char *) cond.rhsValue.data + sizeof(unsigned) + i) = 12 + 96;
        }

        // Create Filter
        PeterDB::Filter filter(&ts, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned a = i + 20;
            unsigned len = (i % 26) + 1;
            std::string b = std::string(len, '\0');
            for (int j = 0; j < len; j++) {
                b[j] = (char) (96 + len);
            }
            if (len == 12) {
                expected.emplace_back("leftvarchar.A: " + std::to_string(a) + ", leftvarchar.B: " + b);
            }
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }
    }

    TEST_F(QE_Test, index_scan_with_real_filter) {
        // 1. Filter -- IndexScan as input, on TypeReal attribute
        // SELECT * FROM RIGHT WHERE C >= 110.0

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {"D", "C", "B"}, 1000);

        // Set up IndexScan
        PeterDB::IndexScan is(rm, tableName, "C");

        // Set up condition
        float compVal = 110.0;
        PeterDB::Condition cond{"right.C", PeterDB::GE_OP, false, "", {PeterDB::TypeReal, inBuffer}};
        *(float *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&is, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 1000; i++) {
            unsigned b = i % 251 + 20;
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;
            if (c >= 110) {
                expected.emplace_back(
                        "right.B: " + std::to_string(b) + ", right.C: " + std::to_string(c) + ", right.D: " +
                        std::to_string(d));
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

    TEST_F(QE_Test, table_scan_with_project) {
        // Project -- TableScan as input
        // SELECT C,D FROM RIGHT

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {}, 1000);

        // Set up TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Projector
        PeterDB::Project project(&ts, {"right.D", "right.C"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(project.getAttributes(attrs), success) << "Project.getAttributes() should succeed.";
        while (project.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        expected.reserve(1000);
        for (int i = 0; i < 1000; i++) {
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;

            expected.emplace_back("right.D: " + std::to_string(d) + ", right.C: " + std::to_string(c));

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

    TEST_F(QE_Test, bnljoin) {
        // 1. BNLJoin -- on TypeInt Attribute
        // SELECT * FROM left, right where left.B = right.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "A", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"D", "B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::TableScan rightIn(rm, rightTableName);
        PeterDB::Condition cond{"left.B", PeterDB::EQ_OP, true, "right.B"};

        // Create BNLJoin
        PeterDB::BNLJoin bnlJoin(&leftIn, &rightIn, cond, 5);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(bnlJoin.getAttributes(attrs), success) << "BNLJoin.getAttributes() should succeed.";
        while (bnlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 == b2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i]);
        }

    }

    TEST_F(QE_Test, bnljoin_with_filter) {
        // Functions Tested
        // 1. BNLJoin -- on TypeInt Attribute
        // 2. Filter -- on TypeInt Attribute
        // SELECT * FROM left, right WHERE left.B = right.B AND right.B >= 100

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");

        // Create BNLJoin
        PeterDB::BNLJoin bnlJoin(&leftIn, &rightIn, {"left.B", PeterDB::EQ_OP, true, "right.B"}, 5);

        int compVal = 100;

        // Create Filter
        PeterDB::Condition cond{"right.B", PeterDB::GE_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;
        PeterDB::Filter filter(&bnlJoin, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b2 >= 100 && b1 == b2) {

                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i]);
        }
    }

    TEST_F(QE_Test, inljoin) {
        // 1. INLJoin -- on TypeReal Attribute
        // SELECT * from left, right WHERE left.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::IndexScan rightIn(rm, "right", "C");
        PeterDB::Condition cond{"left.C", PeterDB::EQ_OP, true, "right.C"};

        // Create INLJoin
        PeterDB::INLJoin inlJoin(&leftIn, &rightIn, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(inlJoin.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (inlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }
    }

    TEST_F(QE_Test, inljoin_with_filter_and_project) {
        // 1. Filter
        // 2. Project
        // 3. INLJoin
        // SELECT A1.A, A1.C, right.* FROM (SELECT * FROM left WHERE left.B < 75) A1, right WHERE A1.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, 100);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, 100);

        // Create Filter
        PeterDB::IndexScan leftIn(rm, leftTableName, "B");

        int compVal = 75;
        PeterDB::Condition cond{"left.B", PeterDB::LT_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;

        leftIn.setIterator(nullptr, cond.rhsValue.data, true, false);
        PeterDB::Filter filter(&leftIn, cond);

        // Create Project
        PeterDB::Project project(&filter, {"left.A", "left.C"});

        // Create Join
        PeterDB::IndexScan rightIn(rm, rightTableName, "C");
        PeterDB::INLJoin inlJoin(&project, &rightIn, {"left.C", PeterDB::EQ_OP, true, "right.C"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(inlJoin.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (inlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 100; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 100; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 < 75 && c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.C: " + std::to_string(c1) + ", right.B: " +
                            std::to_string(b2) + ", right.C: " + std::to_string(c2) + ", right.D: " +
                            std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

    TEST_F(QE_Test, table_scan_with_max_aggregation) {
        // 1. Basic aggregation - max
        // SELECT max(left.B) from left

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"B", "C"}, 3000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"left.B", PeterDB::TypeInt, 4}, PeterDB::MAX);

        ASSERT_EQ(agg.getAttributes(attrs), success) << "Aggregate.getAttributes() should succeed.";
        ASSERT_NE(agg.getNextTuple(outBuffer), QE_EOF) << "Aggregate.getNextTuple() should succeed.";
        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager.printTuple() should succeed.";
        checkPrintRecord("MAX(left.B): 196", stream.str());
        ASSERT_EQ(agg.getNextTuple(outBuffer), QE_EOF) << "Only 1 tuple should be returned for MAX.";

    }

    TEST_F(QE_Test, index_scan_with_avg_aggregation) {
        // 1. Basic aggregation - AVG
        // SELECT AVG(right.B) from left

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "right";
        createAndPopulateTable(tableName, {"B", "C"}, 3000);

        // Create IndexScan
        PeterDB::IndexScan is(rm, tableName, "B");

        // Create Aggregate
        PeterDB::Aggregate agg(&is, {"right.B", PeterDB::TypeInt, 4}, PeterDB::AVG);

        ASSERT_EQ(agg.getAttributes(attrs), success) << "Aggregate.getAttributes() should succeed.";
        ASSERT_NE(agg.getNextTuple(outBuffer), QE_EOF) << "Aggregate.getNextTuple() should succeed.";
        std::stringstream stream;
        ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                    << "RelationManager.printTuple() should succeed.";
        checkPrintRecord("AVG(right.B): 144.522", stream.str());
        ASSERT_EQ(agg.getNextTuple(outBuffer), QE_EOF) << "Only 1 tuple should be returned for AVG.";

    }

    TEST_F(QE_Test, scan_with_update_and_delete) {
        // Function Tested
        // Insert 25 records into table
        // update two records
        // delete two records
        // Index scan with filter

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        char data1[bufSize];
        char data2[bufSize];

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"A", "B", "C"}, 25);

        // update a tuple: 10 updates to 100
        prepareLeftTuple(nullsIndicator, 90, inBuffer);
        rm.updateTuple("left", inBuffer, rids[0]);

        // delete two tuples: 13 and 14 are removed
        rm.deleteTuple("left", rids[3]);
        rm.deleteTuple("left", rids[4]);

        // update a tuple: 11 updates to 13
        prepareLeftTuple(nullsIndicator, 3, inBuffer);
        rm.updateTuple("left", inBuffer, rids[1]);

        int compVal = 15;

        // Set up condition
        PeterDB::Condition cond{"left.B", PeterDB::LE_OP, false, "", {PeterDB::TypeInt, data1}};
        *(unsigned *) cond.rhsValue.data = compVal;

        PeterDB::IndexScan is(rm, "left", "B");

        // Create Filter
        PeterDB::Filter filter(&is, cond);


        // Should now have 12, 13 and 15.

        std::stringstream stream;
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord("A: 2, B: 12, C: 52.5", stream.str());

        stream.str(std::string());
        stream.clear();
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord("A: 3, B: 13, C: 53.5", stream.str());

        stream.str(std::string());
        stream.clear();
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord(stream.str(), "A: 5, B: 15, C: 55.5");

        ASSERT_EQ(filter.getNextTuple(outBuffer), EOF);

    }

    TEST_F(QE_Test, scan_with_update_to_null) {
        // Function Tested
        // Insert 25 records into table
        // update three records to contain Null
        // Table scan with filter
        // Index scan with filter

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);
        char data1[bufSize];
        char data2[bufSize];

        std::string tableName = "left";
        createAndPopulateTable(tableName, {"A", "B", "C"}, 25);

        // 11100000
        *nullsIndicator = 128 + 64 + 32;

        // update a tuple: 11 updates to NULLs
        prepareLeftTuple(nullsIndicator, 11, inBuffer);
        rm.updateTuple("left", inBuffer, rids[1]);

        // update a tuple: 12 updates to NULLs
        prepareLeftTuple(nullsIndicator, 12, inBuffer);
        rm.updateTuple("left", inBuffer, rids[2]);

        // update a tuple: 14 updates to NULLs
        prepareLeftTuple(nullsIndicator, 14, inBuffer);
        rm.updateTuple("left", inBuffer, rids[4]);

        int compVal = 15;

        // Set up condition
        PeterDB::Condition cond{"left.B", PeterDB::LE_OP, false, "", {PeterDB::TypeInt, data1}};
        *(unsigned *) cond.rhsValue.data = compVal;

        PeterDB::IndexScan is(rm, "left", "B");

        // Create Filter
        PeterDB::Filter filter(&is, cond);


        // Should now have 10, 13 and 15.

        std::stringstream stream;
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord("A: 0, B: 10, C: 50.5", stream.str());

        stream.str(std::string());
        stream.clear();
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord("A: 3, B: 13, C: 53.5", stream.str());

        stream.str(std::string());
        stream.clear();
        memset(outBuffer, 0, bufSize);
        ASSERT_EQ(filter.getNextTuple(outBuffer), success);
        rm.printTuple(attrs, outBuffer, stream);
        checkPrintRecord(stream.str(), "A: 5, B: 15, C: 55.5");

        ASSERT_EQ(filter.getNextTuple(outBuffer), EOF);

    }

    TEST_F(QE_Test, index_scan_with_range_real_cond_on_same_attr) {
        // 1. Filter -- IndexScan as input, on TypeReal attribute
        // SELECT * FROM RIGHT WHERE C >= 185.9 AND C < 193.4;

        unsigned numOfTuples = 20000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        char data1[bufSize];
        char data2[bufSize];

        std::string tableName = "right";
        createAndPopulateTable(tableName, {"D", "C", "B"}, numOfTuples);

        // Set up IndexScan
        PeterDB::IndexScan is(rm, tableName, "C");

        // Set up condition
        float compVal = 185.9;
        PeterDB::Condition cond{"right.C", PeterDB::GE_OP, false, "", {PeterDB::TypeReal, data1}};
        *(float *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&is, cond);


        // Create another Filter
        float compVal2 = 193.4;
        PeterDB::Condition cond2{"right.C", PeterDB::LT_OP, false, "", {PeterDB::TypeReal, data2}};
        *(float *) cond2.rhsValue.data = compVal2;
        PeterDB::Filter filter2(&filter, cond2);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter2.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter2.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuples; i++) {
            unsigned b = i % 251 + 20;
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;
            if (c >= compVal && c < compVal2) {
                expected.emplace_back(
                        "right.B: " + std::to_string(b) + ", right.C: " + std::to_string(c) + ", right.D: " +
                        std::to_string(d));
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 100 == 0);
        }
    }

    TEST_F(QE_Test, table_scan_with_range_int_cond_on_different_attr) {
        // 1. Filter -- TableScan as input, on TypeInt attribute
        // SELECT * FROM RIGHT WHERE D < 10 AND B <= 25;

        unsigned numOfTuples = 100000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        char data1[bufSize];
        char data2[bufSize];

        std::string tableName = "right";
        createAndPopulateTable(tableName, {}, numOfTuples);

        // Set up TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Set up condition
        unsigned compVal = 10;
        PeterDB::Condition cond{"right.D", PeterDB::LT_OP, false, "", {PeterDB::TypeInt, data1}};
        *(unsigned *) cond.rhsValue.data = compVal;

        // Create Filter
        PeterDB::Filter filter(&ts, cond);


        // Create another Filter
        unsigned compVal2 = 25;
        PeterDB::Condition cond2{"right.B", PeterDB::LE_OP, false, "", {PeterDB::TypeInt, data2}};
        *(unsigned *) cond2.rhsValue.data = compVal2;
        PeterDB::Filter filter2(&filter, cond2);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter2.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter2.getNextTuple(outBuffer) != QE_EOF) {
            // Null indicators should be placed in the beginning.
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuples; i++) {
            unsigned b = i % 251 + 20;
            float c = (float) (i % 261) + 25.5f;
            unsigned d = i % 179;
            if (d < compVal && b <= compVal2) {
                expected.emplace_back(
                        "right.B: " + std::to_string(b) + ", right.C: " + std::to_string(c) + ", right.D: " +
                        std::to_string(d));
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 30 == 0);
        }
    }

    TEST_F(QE_Test, large_bnljoin_with_filter) {
        // Functions Tested
        // 1. BNLJoin -- on TypeInt Attribute
        // 2. Filter -- on TypeInt Attribute
        // SELECT * FROM left, right WHERE left.B = right.B AND right.D < 10

        unsigned numOfTuplesLeft = 10000;
        unsigned numOfTuplesRight = 12000;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, numOfTuplesLeft);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {}, numOfTuplesRight);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");

        // Create BNLJoin
        PeterDB::BNLJoin bnlJoin(&leftIn, &rightIn, {"left.B", PeterDB::EQ_OP, true, "right.B"}, 10);

        // Create Filter
        int compVal = 10;
        PeterDB::Condition cond{"right.D", PeterDB::LT_OP, false, "", {PeterDB::TypeInt, inBuffer}};
        *(unsigned *) cond.rhsValue.data = compVal;
        PeterDB::Filter filter(&bnlJoin, cond);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(filter.getAttributes(attrs), success) << "Filter.getAttributes() should succeed.";
        while (filter.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuplesLeft; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < numOfTuplesRight; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (d < compVal && b1 == b2) {

                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }
            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 3000 == 0);
        }
    }

    TEST_F(QE_Test, deep_bnljoin) {
        // Functions Tested
        // 1. BNLJoin -- on TypeInt Attribute
        // SELECT * FROM left, right, leftvarchar WHERE left.B = right.B AND left.A = leftvarchar.A

        unsigned numOfTuplesLeft = 3000;
        unsigned numOfTuplesRight = 100;
        unsigned numOfTuplesAnotherRight = 50;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {"B", "C"}, numOfTuplesLeft);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, numOfTuplesRight);

        std::string anotherRightTableName = "leftvarchar";
        createAndPopulateTable(anotherRightTableName, {}, numOfTuplesAnotherRight);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::TableScan rightIn(rm, rightTableName);
        PeterDB::TableScan anotherRightIn(rm, anotherRightTableName);

        // Create child BNLJoin
        PeterDB::BNLJoin childBnlJoin(&leftIn, &rightIn, {"left.B", PeterDB::EQ_OP, true, "right.B"}, 10);

        // Create top BNLJoin
        PeterDB::BNLJoin bnlJoin(&childBnlJoin, &anotherRightIn, {"left.A", PeterDB::EQ_OP, true, "leftvarchar.A"}, 5);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(bnlJoin.getAttributes(attrs), success) << "bnlJoin.getAttributes() should succeed.";
        while (bnlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuplesLeft; i++) {
            unsigned a1 = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < numOfTuplesRight; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;

                for (int k = 0; k < numOfTuplesAnotherRight; k++) {
                    unsigned a3 = k + 20;

                    unsigned length = (k % 26) + 1;
                    std::string b3 = std::string(length, '\0');
                    for (int p = 0; p < length; p++) {
                        b3[p] = (char) (96 + length);
                    }

                    if (b1 == b2 && a1 == a3) {

                        expected.emplace_back(
                                "left.A: " + std::to_string(a1) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                                std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                                std::to_string(c2) + ", right.D: " + std::to_string(d) + ", leftvarchar.A: " +
                                std::to_string(a3) + ", leftvarchar.B: " + b3);
                    }
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 50 == 0);
        }
    }

    TEST_F(QE_Test, deep_inljoin) {
        // Functions Tested
        // 1. INLJoin -- on TypeInt and TypeReal Attribute
        // SELECT * FROM left, right, leftvarchar WHERE left.C = right.C AND left.A = leftvarchar.A

        unsigned numOfTuplesLeft = 3000;
        unsigned numOfTuplesRight = 100;
        unsigned numOfTuplesAnotherRight = 50;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, numOfTuplesLeft);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, numOfTuplesRight);

        std::string anotherRightTableName = "leftvarchar";
        createAndPopulateTable(anotherRightTableName, {"A"}, numOfTuplesAnotherRight);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::IndexScan rightIn(rm, rightTableName, "C");
        PeterDB::IndexScan anotherRightIn(rm, anotherRightTableName, "A");

        // Create child INLJoin
        PeterDB::INLJoin childInlJoin(&leftIn, &rightIn, {"left.C", PeterDB::EQ_OP, true, "right.C"});

        // Create top INLJoin
        PeterDB::INLJoin inlJoin(&childInlJoin, &anotherRightIn, {"left.A", PeterDB::EQ_OP, true, "leftvarchar.A"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(inlJoin.getAttributes(attrs), success) << "bnlJoin.getAttributes() should succeed.";
        while (inlJoin.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuplesLeft; i++) {
            unsigned a1 = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < numOfTuplesRight; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;

                for (int k = 0; k < numOfTuplesAnotherRight; k++) {
                    unsigned a3 = k + 20;

                    unsigned length = (k % 26) + 1;
                    std::string b3 = std::string(length, '\0');
                    for (int p = 0; p < length; p++) {
                        b3[p] = (char) (96 + length);
                    }

                    if (c1 == c2 && a1 == a3) {

                        expected.emplace_back(
                                "left.A: " + std::to_string(a1) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                                std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                                std::to_string(c2) + ", right.D: " + std::to_string(d) + ", leftvarchar.A: " +
                                std::to_string(a3) + ", leftvarchar.B: " + b3);
                    }
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 50 == 0);
        }
    }

    TEST_F(QE_Test, inljoin_bnljoin_with_project) {
        // Functions Tested
        // 1. INLJoin -- on TypeInt and TypeReal Attribute
        // SELECT right.D, leftvarchar.B FROM left, right, leftvarchar WHERE left.C = right.C AND left.A = leftvarchar.A

        unsigned numOfTuplesLeft = 3000;
        unsigned numOfTuplesRight = 100;
        unsigned numOfTuplesAnotherRight = 50;
        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, numOfTuplesLeft);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {"B", "C"}, numOfTuplesRight);

        std::string anotherRightTableName = "leftvarchar";
        createAndPopulateTable(anotherRightTableName, {"A"}, numOfTuplesAnotherRight);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::TableScan rightIn(rm, rightTableName);
        PeterDB::IndexScan anotherRightIn(rm, anotherRightTableName, "A");

        // Create child BNLJoin
        PeterDB::BNLJoin childBnlJoin(&leftIn, &rightIn, {"left.C", PeterDB::EQ_OP, true, "right.C"}, 10);

        // Create project
        PeterDB::Project project(&childBnlJoin, {"left.A", "right.D"});

        // Create top INLJoin
        PeterDB::INLJoin inlJoin(&project, &anotherRightIn, {"left.A", PeterDB::EQ_OP, true, "leftvarchar.A"});

        // Create another project
        PeterDB::Project project2(&inlJoin, {"right.D", "leftvarchar.B"});

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(project2.getAttributes(attrs), success) << "bnlJoin.getAttributes() should succeed.";
        while (project2.getNextTuple(outBuffer) != QE_EOF) {
            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuplesLeft; i++) {
            unsigned a1 = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < numOfTuplesRight; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;

                for (int k = 0; k < numOfTuplesAnotherRight; k++) {
                    unsigned a3 = k + 20;

                    unsigned length = (k % 26) + 1;
                    std::string b3 = std::string(length, '\0');
                    for (int p = 0; p < length; p++) {
                        b3[p] = (char) (96 + length);
                    }

                    if (c1 == c2 && a1 == a3) {

                        expected.emplace_back("right.D: " + std::to_string(d) + ", leftvarchar.B: " + b3);
                    }
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 50 == 0);
        }
    }

    TEST_F(QE_Test, ghjoin_on_varchar) {
        // Extra credit
        // 1. GHJoin -- on TypeVARCHAR Attribute
        // SELECT * from leftvarchar, rightvarchar WHERE leftvarchar.B = rightvarchar.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        unsigned numPartitions = 10;
        unsigned numOfTuplesLeft = 500;
        unsigned numOfTuplesRight = 400;

        std::string leftTableName = "leftvarchar";
        createAndPopulateTable(leftTableName, {}, numOfTuplesLeft);

        std::string rightTableName = "rightvarchar";
        createAndPopulateTable(rightTableName, {}, numOfTuplesRight);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, leftTableName);
        PeterDB::TableScan rightIn(rm, rightTableName);

        PeterDB::Condition cond{"leftvarchar.B", PeterDB::EQ_OP, true, "rightvarchar.B"};

        int numFiles = (int) glob("").size();
        // Create GHJoin - on heap so we can control its life cycle
        auto *ghJoin = new PeterDB::GHJoin(&leftIn, &rightIn, cond, numPartitions);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(ghJoin->getAttributes(attrs), success) << "GHJoin.getAttributes() should succeed.";
        while (ghJoin->getNextTuple(outBuffer) != QE_EOF) {

            ASSERT_GE(glob("").size(), numFiles + 20) << "There should be at least 20 partition files created.";

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < numOfTuplesLeft; i++) {
            unsigned a = i + 20;

            unsigned length = (i % 26) + 1;
            std::string b1 = std::string(length, '\0');
            for (int n = 0; n < length; n++) {
                b1[n] = (char) (96 + length);
            }

            for (int j = 0; j < numOfTuplesRight; j++) {
                unsigned length2 = (j % 26) + 1;
                std::string b2 = std::string(length2, '\0');
                for (int m = 0; m < length2; m++) {
                    b2[m] = (char) (96 + length2);
                }
                auto c = (float) (j + 28.2);
                if (b1 == b2) {
                    std::string expectedStr;
                    expectedStr
                        .append("leftvarchar.A: ").append(std::to_string(a)).append(", leftvarchar.B: ")
                        .append(b1).append(", rightvarchar.B: ").append(b2).append(", rightvarchar.C: ")
                        .append(std::to_string(c));
                    expected.emplace_back(expectedStr);
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, i % 1000 == 0);
        }

        delete ghJoin;
        ASSERT_EQ(glob("").size(), numFiles) << "GHJoin should clean after itself.";
    }

    TEST_F(QE_Test, index_scan_with_group_count_aggregation) {
        // Extra credit
        // Aggregate -- COUNT (with GroupBy)
        // SELECT group.B, COUNT(group.A) FROM group GROUP BY group.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "group";
        createAndPopulateTable(tableName, {"A", "B"}, 2000);

        // Create TableScan
        PeterDB::IndexScan is(rm, tableName, "A");

        // Create Aggregate
        PeterDB::Aggregate agg(&is, {"group.A", PeterDB::TypeInt, 4}, {"group.B", PeterDB::TypeInt, 4}, PeterDB::COUNT);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(agg.getAttributes(attrs), success) << "Aggregate.getAttributes() should succeed.";

        while (agg.getNextTuple(outBuffer) != QE_EOF) {

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 1; i < 6; i++) {
            expected.emplace_back("group.B: " + std::to_string(i) + ", COUNT(group.A): " + std::to_string(400));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {}, true);
        }

    }

    TEST_F(QE_Test, ghjoin_on_int) {
        // Extra credit
        // 1. GHJoin -- on TypeInt Attribute
        // SELECT * from left, right WHERE left.B = right.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        unsigned numPartitions = 10;

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, 10000);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {}, 10000);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");

        PeterDB::Condition cond{"left.B", PeterDB::EQ_OP, true, "right.B"};

        int numFiles = (int) glob("").size();
        // Create GHJoin - on heap so we can control its life cycle
        auto *ghJoin = new PeterDB::GHJoin(&leftIn, &rightIn, cond, numPartitions);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(ghJoin->getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (ghJoin->getNextTuple(outBuffer) != QE_EOF) {

            ASSERT_GE(glob("").size(), numFiles + 20) << "There should be at least 20 partition files created.";

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 10000; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 10000; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (b1 == b2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " +
                            std::to_string(c2) + ", right.D: " + std::to_string(d));
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

        delete ghJoin;
        ASSERT_EQ(glob("").size(), numFiles) << "GHJoin should clean after itself.";
    }

    TEST_F(QE_Test, ghjoin_on_real) {
        // Extra credit
        // 1. GHJoin -- on TypeReal Attribute
        // SELECT * from left, right WHERE left.C = right.C

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        unsigned numPartitions = 7;

        std::string leftTableName = "left";
        createAndPopulateTable(leftTableName, {}, 10000);

        std::string rightTableName = "right";
        createAndPopulateTable(rightTableName, {}, 10000);

        // Prepare the iterator and condition
        PeterDB::TableScan leftIn(rm, "left");
        PeterDB::TableScan rightIn(rm, "right");
        PeterDB::Condition cond{"left.C", PeterDB::EQ_OP, true, "right.C"};

        int numFiles = (int) glob("").size();
        // Create GHJoin - on heap so we can control its life cycle
        auto *ghJoin = new PeterDB::GHJoin(&leftIn, &rightIn, cond, numPartitions);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(ghJoin->getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (ghJoin->getNextTuple(outBuffer) != QE_EOF) {

            ASSERT_GE(glob("").size(), numFiles + 14) << "There should be at least 20 partition files created.";

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 0; i < 10000; i++) {
            unsigned a = i % 203;
            unsigned b1 = (i + 10) % 197;
            float c1 = (float) (i % 167) + 50.5f;
            for (int j = 0; j < 10000; j++) {
                unsigned b2 = j % 251 + 20;
                float c2 = (float) (j % 261) + 25.5f;
                unsigned d = j % 179;
                if (c1 == c2) {
                    expected.emplace_back(
                            "left.A: " + std::to_string(a) + ", left.B: " + std::to_string(b1) + ", left.C: " +
                            std::to_string(c1) + ", right.B: " + std::to_string(b2) + ", right.C: " + std::to_string(c2)
                            + ", right.D: " + std::to_string(d));
                }

            }

        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

        delete ghJoin;
        ASSERT_EQ(glob("").size(), numFiles) << "GHJoin should clean after itself.";
    }

    TEST_F(QE_Test, table_scan_with_group_min_aggregation) {
        // Extra credit
        // Aggregate -- MIN (with GroupBy)
        // SELECT group.B, MIN(group.A) FROM group GROUP BY group.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "group";
        createAndPopulateTable(tableName, {}, 10000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"group.A", PeterDB::TypeInt, 4}, {"group.B", PeterDB::TypeInt, 4}, PeterDB::MIN);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(agg.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (agg.getNextTuple(outBuffer) != QE_EOF) {

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 1; i < 6; i++) {
            expected.emplace_back("group.B: " + std::to_string(i) + ", MIN(group.A): " + std::to_string(i));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

    TEST_F(QE_Test, table_scan_with_group_sum_aggregation) {
        // Extra credit
        // Aggregate -- SUM (with GroupBy)
        // SELECT group.B, SUM(group.A) FROM group GROUP BY group.B

        inBuffer = malloc(bufSize);
        outBuffer = malloc(bufSize);

        std::string tableName = "group";
        createAndPopulateTable(tableName, {}, 10000);

        // Create TableScan
        PeterDB::TableScan ts(rm, tableName);

        // Create Aggregate
        PeterDB::Aggregate agg(&ts, {"group.A", PeterDB::TypeInt, 4}, {"group.B", PeterDB::TypeInt, 4}, PeterDB::SUM);

        // Go over the data through iterator
        std::vector<std::string> printed;
        ASSERT_EQ(agg.getAttributes(attrs), success) << "INLJoin.getAttributes() should succeed.";
        while (agg.getNextTuple(outBuffer) != QE_EOF) {

            std::stringstream stream;
            ASSERT_EQ(rm.printTuple(attrs, outBuffer, stream), success)
                                        << "RelationManager.printTuple() should succeed.";
            printed.emplace_back(stream.str());
            memset(outBuffer, 0, bufSize);
        }

        std::vector<std::string> expected;
        for (int i = 1; i < 6; i++) {
            expected.emplace_back("group.B: " + std::to_string(i) + ", SUM(group.A): " + std::to_string(i * 2000));
        }
        sort(expected.begin(), expected.end());
        sort(printed.begin(), printed.end());

        ASSERT_EQ(expected.size(), printed.size()) << "The number of returned tuple is not correct.";

        for (int i = 0; i < expected.size(); ++i) {
            checkPrintRecord(expected[i], printed[i], false, {});
        }

    }

} // namespace PeterDBTesting