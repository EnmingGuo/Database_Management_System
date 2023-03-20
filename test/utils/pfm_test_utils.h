#ifndef PFM_TEST_UTILS_H
#define PFM_TEST_UTILS_H

#include <gtest/gtest.h>

#include "src/include/pfm.h"
#include "test/utils/general_test_utils.h"

namespace PeterDBTesting {

    class PFM_File_Test : public ::testing::Test {
    public:
        PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();

        void SetUp() override {
            // Remove files that might be created by previous test run
            remove(fileName.c_str());
        }

        std::string fileName = "pfm_test_file";
    };

    class PFM_Page_Test : public PFM_File_Test {
        void SetUp() override {

            if (!fileExists(fileName)) {
                // Create a file
                ASSERT_EQ(pfm.createFile(fileName), success) << "Creating the file should not fail: " << fileName;
                ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;

            }

            // Open the file
            ASSERT_EQ(pfm.openFile(fileName, fileHandle), success) << "Opening the file should not fail: " << fileName;

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);

            // Close the file
            ASSERT_EQ(pfm.closeFile(fileHandle), success) << "Closing the file should not fail.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(pfm.destroyFile(fileName), success) << "Destroying the file should not fail.";
            }

        }

    protected:
        PeterDB::FileHandle fileHandle;
        void *inBuffer = nullptr, *outBuffer = nullptr;
        bool destroyFile = true;

        void reopenFile() {
            // Close the file
            ASSERT_EQ(pfm.closeFile(fileHandle), success) << "Closing the file should not fail.";
            // Open the file
            fileHandle = PeterDB::FileHandle();
            ASSERT_EQ(pfm.openFile(fileName, fileHandle), success) << "Opening the file should not fail: " << fileName;
        }
    };

    class PFM_Page_Test_2 : public PFM_Page_Test {
        void SetUp() override {

            fileName = "pfm_test_file_2";

            if (!fileExists(fileName)) {
                // Create a file
                ASSERT_EQ(pfm.createFile(fileName), success) << "Creating the file should not fail: " << fileName;
                ASSERT_TRUE(fileExists(fileName)) << "The file is not found: " << fileName;

            }

            // Open the file
            ASSERT_EQ(pfm.openFile(fileName, fileHandle), success) << "Opening the file should not fail: " << fileName;

        }

        void TearDown() override {

            // Destruct the buffers
            free(inBuffer);
            free(outBuffer);

            // Close the file
            ASSERT_EQ(pfm.closeFile(fileHandle), success) << "Closing the file should not fail.";

            if (destroyFile) {
                // Destroy the file
                ASSERT_EQ(pfm.destroyFile(fileName), success) << "Destroying the file should not fail.";
            }

        }
    };
} // namespace PeterDBTesting

#endif // PFM_TEST_UTILS_H
