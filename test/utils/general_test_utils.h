#ifndef GENERAL_TEST_UTILS_H
#define GENERAL_TEST_UTILS_H

#include <string>
#include <cstring>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sstream>
#include <fstream>
#include <random>
#include <dirent.h>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "ordered_map.h"

namespace PeterDBTesting {
    int success = 0;

    static bool isFloat(const std::string &myString) {
        if (myString.find('.') == std::string::npos) return false;
        std::istringstream iss(myString);
        float f;
        iss >> std::noskipws >> f; // noskipws considers leading whitespace invalid
        // Check the entire string was consumed and if either failbit or badbit is set
        return iss.eof() && !iss.fail();
    }

    // Check whether the given file exists
    bool fileExists(const std::string &fileName) {
        struct stat stFileInfo{};

        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

    // Get the given file's size
    static std::ifstream::pos_type getFileSize(const std::string &fileName) {
        std::ifstream in(fileName.c_str(),
                         std::ifstream::in | std::ifstream::binary);
        in.seekg(0, std::ifstream::end);
        return in.tellg();
    }

    // Compare the sizes of two files
    static bool compareFileSizes(const std::string &fileName1, const std::string &fileName2) {
        std::streampos s1, s2;
        std::ifstream in1(fileName1.c_str(), std::ifstream::in | std::ifstream::binary);
        in1.seekg(0, std::ifstream::end);
        s1 = in1.tellg();

        std::ifstream in2(fileName2.c_str(), std::ifstream::in | std::ifstream::binary);
        in2.seekg(0, std::ifstream::end);
        s2 = in2.tellg();
        return s1 == s2;
    }

    // Generate some test data and fill the provided buffer
    void generateData(void *buffer, size_t size, unsigned seed = 96, unsigned salt = 30) {
        for (unsigned i = 0; i < size; i++) {
            *((char *) buffer + i) = i % seed + salt;
        }
    }

    // trim from start (in place)
    static inline void ltrim(std::string &s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
            return !std::isspace(ch);
        }));
    }

    // trim from end (in place)
    static inline void rtrim(std::string &s) {
        s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
            return !std::isspace(ch);
        }).base(), s.end());
    }

    // trim from both ends (in place)
    static inline void trim(std::string &s) {
        ltrim(s);
        rtrim(s);
    }

    // trim from start (copying)
    static inline std::string ltrim_copy(std::string s) {
        ltrim(s);
        return s;
    }

    // trim from end (copying)
    static inline std::string rtrim_copy(std::string s) {
        rtrim(s);
        return s;
    }

    // trim from both ends (copying)
    static inline std::string trim_copy(std::string s) {
        trim(s);
        return s;
    }

    static inline std::string to_lower(std::string s) {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c){ return std::tolower(c); });
        return s;
    }

    static void
    convertToMap(const std::string &keyValuePairsStr, tsl::ordered_map<std::string, std::string> &outMap) {

        std::string::size_type key_pos = 0;
        std::string::size_type key_end;
        std::string::size_type val_pos;
        std::string::size_type val_end;

        while ((key_end = keyValuePairsStr.find(':', key_pos)) != std::string::npos) {
            if ((val_pos = keyValuePairsStr.find_first_not_of(": ", key_end)) == std::string::npos) {
                // Handle the case of empty string
                outMap.emplace(trim_copy(keyValuePairsStr.substr(key_pos, key_end - key_pos)), std::string());
                break;
            }

            val_end = keyValuePairsStr.find(',', val_pos);
            // make attrName all lower case to perform case-insensitive check on attribute names
            auto attrName = to_lower(trim_copy(keyValuePairsStr.substr(key_pos, key_end - key_pos)));
            auto value = trim_copy(keyValuePairsStr.substr(val_pos, val_end - val_pos));
            outMap.emplace(attrName, value);
            key_pos = val_end;
            if (key_pos != std::string::npos)
                ++key_pos;
        }
    }

    static std::string
    serializeMap(const tsl::ordered_map<std::string, std::string> &inMap) {
        std::string res;
        for (auto const &pair: inMap)
            res += pair.first + ": " + pair.second + ", ";
        res.substr(res.find_last_of(", "));
        return trim_copy(res.substr(0, res.find_last_of(", ") - 1));
    }

    static std::string normalizeKVString(const std::string &inString) {
        tsl::ordered_map<std::string, std::string> m;
        convertToMap(inString, m);
        return serializeMap(m);
    }

    void checkPrintRecord(const std::string &expected, const std::string &target, bool containsMode = false,
                          const std::vector<std::string> &ignoreValues = std::vector<std::string>(),
                          const bool caseInsensitive = false) {
        GTEST_LOG_(INFO) << "Target string: " << target;
        if (std::strcmp(normalizeKVString(expected).c_str(), target.c_str()) == 0)
            return;

        tsl::ordered_map<std::string, std::string> expectedMap;
        convertToMap(expected, expectedMap);

        tsl::ordered_map<std::string, std::string> targetMap;
        convertToMap(target, targetMap);

        if (!containsMode) {
            ASSERT_EQ(targetMap.size(), expectedMap.size()) << "Fields count should be equal.";
        } else {
            ASSERT_GE(targetMap.size(), expectedMap.size()) << "Fields count should be greater or equal to expected.";
        }

        for (const auto & expectedIter : expectedMap)
        {
            auto expectedKey =  expectedIter.first;
            auto expectedValue = expectedIter.second;

            ASSERT_TRUE((targetMap.contains(expectedKey)))
                                        << "Field (" << expectedKey << ") is not found.";

            auto targetValue = targetMap[expectedKey];
            if (std::find(ignoreValues.begin(), ignoreValues.end(), expectedKey) == ignoreValues.end()) {
                if (isFloat(targetValue)) {
                    ASSERT_FLOAT_EQ(std::stof(targetValue), std::stof(expectedValue))
                                                << "Field (" << expectedKey
                                                << ") value should be equal, float values are checked in a range.";
                } else {
                    if (caseInsensitive) {
                        targetValue = to_lower(targetValue);
                        expectedValue = to_lower(expectedValue);
                    }
                    ASSERT_EQ(targetValue, expectedValue)
                                                << "Field (" << expectedKey << ") value should be equal.";
                }
            }
        }
    }

    static void getByteOffset(unsigned pos, unsigned &bytes, unsigned &offset) {
        bytes = pos / 8;

        offset = 7 - pos % 8;
    }

    static void setBit(char &src, bool value, unsigned offset) {
        if (value) {
            src |= 1u << offset;
        } else {
            src &= ~(1u << offset);
        }
    }

    static void setAttrNull(void *src, ushort attrNum, bool isNull) {
        unsigned bytes = 0;
        unsigned pos = 0;
        getByteOffset(attrNum, bytes, pos);
        setBit(*((char *) src + bytes), isNull, pos);
    }

    // This code is required for testing to measure the memory usage of your code.
    // If you can't compile the codebase because of this function, you can safely comment this function or remove it.
    // void memProfile() {
    //     int who = RUSAGE_SELF;
    //     struct rusage usage{};
    //     getrusage(who, &usage);
    //     std::cout << usage.ru_maxrss << "KB" << std::endl;
    // }

    std::vector<std::string> split(std::string str, const std::string &token) {
        std::vector<std::string> result;
        while (!str.empty()) {
            int index = str.find(token);
            if (index != std::string::npos) {
                result.push_back(str.substr(0, index));
                str = str.substr(index + token.size());
                if (str.empty())result.emplace_back(str);
            } else {
                result.push_back(str);
                str = "";
            }
        }
        return result;
    }

    std::vector<std::string> glob(const std::string &suffix) {
        std::vector<std::string> files;
        DIR *dpdf;
        struct dirent *epdf;

        dpdf = opendir("./");
        if (dpdf != nullptr) {
            while ((epdf = readdir(dpdf))) {
                if (strstr(epdf->d_name, suffix.c_str())) {
                    files.emplace_back(epdf->d_name);
                }
            }
        }
        closedir(dpdf);
        return files;
    }

} // namespace PeterDBTesting


#endif //GENERAL_TEST_UTILS_H
