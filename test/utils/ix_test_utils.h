#pragma clang diagnostic push
#ifndef IX_TEST_UTILS_H
#define IX_TEST_UTILS_H

#include <cmath>
#include <fstream>

#include "src/include/ix.h"
#include "gtest/gtest.h"
#include "test/utils/general_test_utils.h"
#include "test/utils/json.hpp"

#define EXPECT_IN_RANGE(VAL, MIN, MAX) \
    EXPECT_GE((VAL), (MIN));           \
    EXPECT_LE((VAL), (MAX))

#define ASSERT_IN_RANGE(VAL, MIN, MAX) \
    ASSERT_GE((VAL), (MIN));           \
    ASSERT_LE((VAL), (MAX))

namespace PeterDBTesting {

    struct KeyEntry {
        std::string key;
        std::vector<std::string> RIDs;

        explicit KeyEntry(const std::string &key) {
            unsigned long pivot = key.find(':');
            if (pivot != -1) {
                this->key = key.substr(0, pivot);
                this->RIDs = split(key.substr(pivot + 3, key.length() - pivot - 5), "),(");
                for (std::string &rid: this->RIDs) {
                    rid = std::string("(").append(rid).append(")");
                }
            } else {
                this->key = key;
            }

        }

    };

    typedef struct TreeNode {
        std::vector<KeyEntry> keys;
        std::vector<TreeNode> children;

        size_t height() {
            if (children.empty())return 0;
            else {
                size_t m = 0;
                for (auto child : children) {
                    size_t h = child.height();
                    if (h > m) m = h;
                }
                return m + 1;
            }
        }

        size_t totalKeyCount() {
            if (children.empty())return keys.size();
            else {
                size_t s = 0;
                for (auto child : children) {
                    s += child.totalKeyCount();
                }
                return s;
            }
        }

        size_t childrenCount() const {
            return children.size();
        }

        size_t keyCount() const {
            return keys.size();
        }

        std::vector<KeyEntry> allLeafKeys() {
            if (children.empty())return keys;
            else {
                std::vector<KeyEntry> keysFromChildren;
                for (auto child : children) {
                    auto childAllKeys = child.allLeafKeys();
                    keysFromChildren.insert(keysFromChildren.end(), childAllKeys.begin(), childAllKeys.end());
                }
                return keysFromChildren;
            }
        }

        size_t totalRIDCount() {
            if (children.empty()) {
                size_t r = 0;
                for (auto &keyEntry: keys) {
                    r += keyEntry.RIDs.size();
                }
                return r;
            } else {
                size_t r = 0;
                for (auto child : children) {
                    r += child.totalRIDCount();
                }
                return r;
            }
        }

        std::vector<std::string> projectKeys() {
            if (children.empty()) {
                std::vector<std::string> projectedKeys;
                for (const auto &keyEntry : keys) {
                    projectedKeys.emplace_back(keyEntry.key);
                }
                return projectedKeys;

            } else {
                std::vector<std::string> projectedKeys;
                for (int i = 0; i < children.size(); i++) {
                    if (i != 0) {
                        projectedKeys.emplace_back(keys[i - 1].key);
                    }

                    std::vector<std::string> childProjectedKeys = children[i].projectKeys();
                    projectedKeys.insert(projectedKeys.end(), childProjectedKeys.begin(), childProjectedKeys.end());

                }
                return projectedKeys;
            }
        }

        TreeNode &operator=(const TreeNode &other) = default;
    } TreeNode;

    TreeNode buildTree(nlohmann::ordered_json &jsonNode) {

        TreeNode t;
        for (const auto &k : jsonNode["keys"]) {
            t.keys.emplace_back(KeyEntry(k));
        }
        if (jsonNode.contains("children")) {
            for (auto k : jsonNode["children"]) {
                t.children.emplace_back(buildTree(k));
            }
        }
        return t;
    }

    class IX_File_Test : public ::testing::Test {
    protected:
        std::string indexFileName = "ix_test_index_file";
        PeterDB::IXFileHandle ixFileHandle;
    public:

        void SetUp() override {
            remove(indexFileName.c_str());
        }

        PeterDB::IndexManager &ix = PeterDB::IndexManager::instance();
    };

    class IX_Test : public IX_File_Test {
    protected:
        PeterDB::RID rid;
        PeterDB::Attribute ageAttr{"age", PeterDB::TypeInt, 4};
        PeterDB::Attribute heightAttr{"height", PeterDB::TypeReal, 4};
        PeterDB::Attribute empNameAttr{"emp_name", PeterDB::TypeVarChar, 1000};

        unsigned rc, wc, ac, rcAfter, wcAfter, acAfter;
        PeterDB::IX_ScanIterator ix_ScanIterator;
        bool destroyFile = true;
        bool closeFile = true;
        std::vector<PeterDB::RID> rids;

    public:
        void SetUp() override {
            remove(indexFileName.c_str());
            // create index file
            ASSERT_EQ(ix.createFile(indexFileName), success) << "indexManager::createFile() should success";

            // open index file
            ASSERT_EQ(ix.openFile(indexFileName, ixFileHandle), success) << "indexManager::openFile() should succeed.";
        }

        void TearDown() override {

            if (closeFile) {
                // close index file
                ASSERT_EQ(ix.closeFile(ixFileHandle), success) << "indexManager::closeFile() should succeed.";
            }
            if (destroyFile) {
                // destroy index file
                ASSERT_EQ(ix.destroyFile(indexFileName), success) << "indexManager::destroyFile() should succeed.";
            }
        }

        void reopenIndexFile() {
            // close index file
            ASSERT_EQ(ix.closeFile(ixFileHandle), success) << "indexManager::closeFile() should succeed.";

//            ixFileHandle = PeterDB::IXFileHandle();
            // open index file
            ASSERT_EQ(ix.openFile(indexFileName, ixFileHandle), success) << "indexManager::openFile() should succeed.";
        }

        template<typename T>
        void generateAndInsertEntries(unsigned int numOfTuples, const PeterDB::Attribute &attr, T seed = 0,
                                      T salt = 2, T fixedKey = 0) {

            for (unsigned i = 0; i < numOfTuples; i++) {
                T value = i + seed;
                rid.pageNum = (unsigned) (value * salt + seed) % INT_MAX;
                rid.slotNum = (unsigned) (value * salt * seed + seed) % SHRT_MAX;
                rids.emplace_back(rid);
                T key = fixedKey == 0 ? value : fixedKey;
                ASSERT_EQ(ix.insertEntry(ixFileHandle, attr, &key, rid), success)
                                            << "indexManager::insertEntry() should succeed.";
            }
        }

        static void prepareKeyAndRid(const unsigned seed, char *key, PeterDB::RID &rid, unsigned fixedLength = 0) {
            unsigned length = (fixedLength == 0 ? seed : fixedLength) % 1000;
            *(unsigned *) key = length;
            for (int i = 0; i < length; i++) {
                key[4 + i] = (char) ('a' + seed % 26);
            }
            rid.pageNum = seed;
            rid.slotNum = seed;
        }

        void validateTree(std::stringstream &in, unsigned keyCount, unsigned ridCount, unsigned height, unsigned D,
                          bool allowUnderFit = false) {
            nlohmann::ordered_json j;
            in >> j;
            LOG(INFO) << j.dump(2);
            TreeNode root = buildTree(j);
            EXPECT_EQ(root.totalKeyCount(), keyCount) << "key count should match.";
            EXPECT_EQ(root.totalRIDCount(), ridCount) << "RID count should match.";
            checkTree(root, height, D, allowUnderFit, true);
            std::string s;
            for (const auto &key: root.projectKeys()) {
                if (!s.empty()) ASSERT_LE(s, key) << "keys should be sorted ASC.";
                s = key;
            }
        }

        void checkTree(TreeNode &root, unsigned height, unsigned D, bool allowUnderFit, bool isTreeRoot = false) {
            EXPECT_EQ(root.height(), height) << "tree height should match.";
            EXPECT_LE(root.keyCount(), 2 * D) << "number of keys should be less than or equal to 2D.";

            if (root.childrenCount() > 0) {
                // internal node
                EXPECT_EQ(root.keyCount() + 1, root.childrenCount())
                                    << "number of children should be 1 more than the number of keys.";

                EXPECT_LE(root.childrenCount(), 2 * D + 1)
                                    << "number of children should be less than or equal to 2D+1.";
                if (!isTreeRoot && !allowUnderFit)
                    EXPECT_GE(root.keyCount(), D) << "number of children should be more than or equal to D.";
            } else {
                // leaf node
                EXPECT_LE(root.keyCount(), 2 * D) << "number of entries should be less than or equal to 2D.";
                if (!allowUnderFit)
                    EXPECT_GE(root.keyCount(), D) << "number of entries should be more than or equal to D.";
            }

            for (auto child: root.children)checkTree(child, height - 1, D, allowUnderFit);

        }

        template<typename T>
        void validateRID(T key, T seed, T salt) const {
            EXPECT_EQ(rid.pageNum, (unsigned) (key * salt + seed) % INT_MAX)
                                << "returned pageNum does not match inserted.";
            EXPECT_EQ(rid.slotNum, (unsigned) (key * salt * seed + seed) % SHRT_MAX)
                                << "returned slotNum does not match inserted.";
        }

        void
        validateUnorderedRID(unsigned int key, int expected, std::vector<PeterDB::RID> &ridsForCheck) const {
            auto target = std::find_if(ridsForCheck.begin(), ridsForCheck.end(), [&](const PeterDB::RID &r) {
                return r.slotNum == rid.slotNum && r.pageNum == rid.pageNum;
            });
            EXPECT_NE(target, ridsForCheck.end()) << "RID is not from inserted.";
            ridsForCheck.erase(target);
            EXPECT_EQ(key, expected) << "key does not match.";
        }

    };

    class IX_Test_2 : public IX_Test {
    protected:
        PeterDB::IXFileHandle ixFileHandle2;
        std::string indexFileName2 = "ix_test_index_file_2";
        PeterDB::RID rid2;
        unsigned rc2, wc2, ac2, rcAfter2, wcAfter2, acAfter2;
        PeterDB::IX_ScanIterator ix_ScanIterator2;
        std::vector<PeterDB::RID> rids2;

        PeterDB::Attribute shortEmpNameAttr{"short_emp_name", PeterDB::TypeVarChar, 20};
        PeterDB::Attribute longEmpNameAttr{"long_emp_name", PeterDB::TypeVarChar, 100};

    public:
        void SetUp() override {
            remove(indexFileName.c_str());
            remove(indexFileName2.c_str());

            // create index file
            ASSERT_EQ(ix.createFile(indexFileName), success) << "indexManager::createFile() should success";
            ASSERT_EQ(ix.createFile(indexFileName2), success) << "indexManager::createFile() should success";

            // open index file
            ASSERT_EQ(ix.openFile(indexFileName, ixFileHandle), success) << "indexManager::openFile() should succeed.";
            ASSERT_EQ(ix.openFile(indexFileName2, ixFileHandle2), success) << "indexManager::openFile() should succeed.";
        }

        void TearDown() override {
            if (closeFile) {
                // close index file
                ASSERT_EQ(ix.closeFile(ixFileHandle), success) << "indexManager::closeFile() should succeed.";
                ASSERT_EQ(ix.closeFile(ixFileHandle2), success) << "indexManager::closeFile() should succeed.";
            }
            if (destroyFile) {
                // destroy index file
                ASSERT_EQ(ix.destroyFile(indexFileName), success) << "indexManager::destroyFile() should succeed.";
                ASSERT_EQ(ix.destroyFile(indexFileName2), success) << "indexManager::destroyFile() should succeed.";
            }
        }

    };
} // namespace PeterDBTesting

#endif // IX_TEST_UTILS_H
