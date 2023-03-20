#include "src/include/pfm.h"
#include "test/utils/pfm_test_utils.h"

namespace PeterDBTesting {

    TEST_F (PFM_File_Test, create_file) {

        ASSERT_FALSE (fileExists(fileName)) << "The file should not exist now: " << fileName;
        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating file should succeed: " << fileName;;
        ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
    }

    TEST_F (PFM_File_Test, create_existing_file) {

        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating file should succeed: " << fileName;;
        ASSERT_TRUE(fileExists(fileName)) << "The file should exist now: " << fileName;

        // Create the same file again, should not succeed
        ASSERT_NE(pfm.createFile(fileName), success) << "Creating a duplicated file should not succeed: " << fileName;

    }

    TEST_F (PFM_File_Test, destroy_file) {

        // Create the file to be destroyed
        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating file should succeed: " << fileName;;

        // Test for destroy
        ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
        ASSERT_EQ(pfm.destroyFile(fileName), success) << "Destroying the file should success: " << fileName;
        ASSERT_FALSE(fileExists(fileName)) << "The file should not exist now: " << fileName;

    }

    TEST_F (PFM_File_Test, destroy_nonexistent_file) {

        // Create the file to be destroyed
        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating file should succeed: " << fileName;;

        // Test for destroy
        ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
        ASSERT_EQ(pfm.destroyFile(fileName), success) << "Destroying the file should success: " << fileName;
        ASSERT_FALSE(fileExists(fileName)) << "The file should not exist now: " << fileName;

        // Attempt to destroy the file again, should not succeed
        ASSERT_NE(pfm.destroyFile(fileName), success) << "Destroying the same file should not succeed: " << fileName;
        ASSERT_FALSE(fileExists(fileName)) << "The file should not exist now: " << fileName;

    }

    TEST_F (PFM_File_Test, open_and_close_file) {

        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Close File

        // Create a file
        ASSERT_EQ(pfm.createFile(fileName), success) << "Creating the file should succeed: " << fileName;
        ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Open the file
        PeterDB::FileHandle fileHandle;
        ASSERT_EQ(pfm.openFile(fileName, fileHandle), success)
                                    << "Opening the file should succeed: " << fileName;

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Close the file
        ASSERT_EQ(pfm.closeFile(fileHandle), success) << "Closing the file should succeed.";

    }

    TEST_F (PFM_Page_Test, get_page_numbers) {

        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Get Number Of Pages
        // 4. Close File

        ASSERT_TRUE(fileExists(fileName)) << "The file should exist now: " << fileName;

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Get the number of pages in the test file. In this case, it should be zero.
        ASSERT_EQ(fileHandle.getNumberOfPages(), 0) << "The page count should be zero at this moment.";

    }

    TEST_F(PFM_Page_Test, read_nonexistent_page) {
        // Read a nonexistent page
        ASSERT_NE(fileHandle.readPage(1, outBuffer), success) << "Reading a nonexistent page should not succeed.";

    }

    TEST_F (PFM_Page_Test, append_and_read_pages) {
        // Functions Tested:
        // 1. Open File
        // 2. Append Page
        // 3. Get Number Of Pages
        // 4. Get Counter Values
        // 5. Reopen File
        // 6. Read Page
        // 7. Close File

        unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
        unsigned updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;

        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Append the first page
        size_t fileSizeBeforeAppend = getFileSize(fileName);
        inBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, PAGE_SIZE);
        ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
        ASSERT_GT(getFileSize(fileName), fileSizeBeforeAppend) << "File size should have been increased";

        // Collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(
                updatedReadPageCount, updatedWritePageCount, updatedAppendPageCount), success)
                                    << "Collecting counters should succeed.";

        ASSERT_LT(appendPageCount, updatedAppendPageCount) << "The appendPageCount should have been increased.";

        // Get the number of pages
        ASSERT_EQ(fileHandle.getNumberOfPages(), 1) << "The count should be one at this moment.";

        reopenFile();
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Reset counters
        readPageCount = updatedReadPageCount, writePageCount = updatedWritePageCount, appendPageCount = updatedAppendPageCount;
        updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;

        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Read the first page
        outBuffer = malloc(PAGE_SIZE);
        ASSERT_EQ(fileHandle.readPage(0, outBuffer), success) << "Reading a page should succeed.";

        // Collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(
                updatedReadPageCount, updatedWritePageCount, updatedAppendPageCount), success)
                                    << "Collecting counters should succeed.";
        ASSERT_LT(readPageCount, updatedReadPageCount) << "The readPageCount should have been increased.";

        // Check the integrity of the page
        free(inBuffer);
        inBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, PAGE_SIZE);
        ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0)
                                    << "Checking the integrity of the page should succeed.";

    }

    TEST_F(PFM_Page_Test, write_nonexistent_page) {
        // Write a nonexistent page
        inBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, PAGE_SIZE);
        ASSERT_NE(fileHandle.writePage(1, inBuffer), success) << "Reading a nonexistent page should not succeed.";

    }

    TEST_F (PFM_Page_Test, write_and_read_pages) {
        // Functions Tested:
        // 1. Open File
        // 2. Write Page
        // 3. Reopen File
        // 4. Read Page
        // 5. Close File
        // 6. Destroy File

        unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
        unsigned updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;

        // Append the first page
        size_t fileSizeBeforeAppend = getFileSize(fileName);
        inBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, PAGE_SIZE);
        ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
        ASSERT_GT(getFileSize(fileName), fileSizeBeforeAppend) << "File size should have been increased";

        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Update and write the first page
        size_t fileSizeBeforeWrite = getFileSize(fileName);
        free(inBuffer);
        inBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, PAGE_SIZE, 10);
        ASSERT_EQ(fileHandle.writePage(0, inBuffer), success) << "Writing a page should succeed.";
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
        ASSERT_EQ(getFileSize(fileName), fileSizeBeforeWrite) << "File size should not have been increased";

        reopenFile();
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Read the page
        outBuffer = malloc(PAGE_SIZE);
        ASSERT_EQ(fileHandle.readPage(0, outBuffer), success) << "Reading a page should succeed.";

        // Collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(
                updatedReadPageCount, updatedWritePageCount, updatedAppendPageCount), success)
                                    << "Collecting counters should succeed.";
        ASSERT_LT(readPageCount, updatedReadPageCount) << "The readPageCount should have been increased.";
        ASSERT_LT(writePageCount, updatedWritePageCount) << "The writePageCount should have been increased.";

        // Check the integrity of the page
        ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0)
                                    << "Checking the integrity of the page should succeed.";

    }

    TEST_F (PFM_Page_Test, append_and_write_and_read_pages) {
        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Append Page
        // 4. Reopen File
        // 5. Get Number Of Pages
        // 6. Read Page
        // 7. Reopen File
        // 8. Write Page
        // 9. Reopen File
        // 10. Read Page
        // 11. Close File
        // 12. Destroy File

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
        unsigned updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;

        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Append 100 pages
        inBuffer = malloc(PAGE_SIZE);
        size_t fileSizeBeforeAppend = getFileSize(fileName);
        for (unsigned j = 0; j < 100; j++) {
            generateData(inBuffer, PAGE_SIZE, j + 1);
            ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
            ASSERT_GT(getFileSize(fileName), fileSizeBeforeAppend) << "File size should have been increased";
            fileSizeBeforeAppend = getFileSize(fileName);
        }
        GTEST_LOG_(INFO) << "100 Pages have been successfully appended!";

        // Collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(
                updatedReadPageCount, updatedWritePageCount, updatedAppendPageCount), success)
                                    << "Collecting counters should succeed.";
        ASSERT_LT(appendPageCount, updatedAppendPageCount) << "The appendPageCount should have been increased.";

        reopenFile();
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Get the number of pages
        ASSERT_EQ(fileHandle.getNumberOfPages(), 100) << "The count should be 100 at this moment.";

        // Read the 86th page and check integrity
        unsigned pageNumForCheck = 86;
        outBuffer = malloc(PAGE_SIZE);

        ASSERT_EQ(fileHandle.readPage(pageNumForCheck - 1, // pageNum start from 0
                                      outBuffer), success) << "Reading a page should succeed.";

        reopenFile();
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        generateData(inBuffer, PAGE_SIZE, pageNumForCheck);
        ASSERT_EQ(memcmp(outBuffer, inBuffer, PAGE_SIZE), 0) << "Checking the integrity of a page should succeed.";

        // Update the 86th page
        generateData(inBuffer, PAGE_SIZE, 60);
        ASSERT_EQ(fileHandle.writePage(pageNumForCheck - 1, inBuffer), success) << "Writing a page should succeed.";

        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        reopenFile();
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";

        // Read the 86th page and check integrity
        ASSERT_EQ(fileHandle.readPage(pageNumForCheck - 1, outBuffer), success) << "Reading a page should succeed.";

        ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0) << "Checking the integrity of a page should succeed.";

    }

    TEST_F (PFM_Page_Test, check_page_num_after_appending) {
        // Test case procedure:
        // 1. Append 39 Pages
        // 2. Check Page Number after each append
        // 3. Keep the file for the next test case

        size_t currentFileSize = getFileSize(fileName);
        inBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        for (int i = 1; i <= numPages; i++) {
            generateData(inBuffer, PAGE_SIZE, 53 + i, 47 - i);
            ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
            ASSERT_GT(getFileSize(fileName), currentFileSize) << "File size should have been increased";
            currentFileSize = getFileSize(fileName);
            ASSERT_EQ(fileHandle.getNumberOfPages(), i)
                                        << "The page count should be " << i << " at this moment";
        }
        destroyFile = false;

    }

    TEST_F (PFM_Page_Test, check_page_num_after_writing) {
        // Test case procedure:
        // 1. Overwrite the 39 Pages from the previous test case
        // 2. Check Page Number after each write
        // 3. Keep the file for the next test case

        inBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; i++) {
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 - i);
            ASSERT_EQ(fileHandle.writePage(i, inBuffer), success) << "Writing a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should not have been increased";
        }
        destroyFile = false;
    }

    TEST_F (PFM_Page_Test, check_page_num_after_reading) {
        // Test case procedure:
        // 1. Read the 39 Pages from the previous test case
        // 2. Check Page Number after each read

        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; i++) {
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 - i);
            ASSERT_EQ(fileHandle.readPage(i, outBuffer), success) << "Reading a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File size should always be multiples of PAGE_SIZE.";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should not have been increased.";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased.";
            ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0)
                                        << "Checking the integrity of the page should succeed.";
        }
    }

} // namespace PeterDBTesting