#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include<map>
#include <sstream>
#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {
    friend class IX_ScanIterator;

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        int find(IXFileHandle &ixFileHandle,const void *key, const Attribute &attribute, const RID &rid) ;

        int treeSearch(IXFileHandle &ixFileHandle,int curNode, const void *key,const Attribute &attribute, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
        unsigned nonleafHeaderSize=4;
        unsigned leafHeaderSize=6;
        int CheckNewOrOriginal(int Label,void* buffer, void * newBuffer, const int EntryLen, const void* Entry,const Attribute& attribute);
        RC CreateLeafPage(void* buffer,int Label, int Parent, int Left,int Right);
        RC CreateNonLeafPage(void*buffer,int Label,int Parent);
        RC CreateLeafEntry(int & length,void* Entry,const Attribute &attribute, const void *key, const RID &rid);
        RC WriteEntry(IXFileHandle &ixFileHandle, void* buffer,int Label,void*Entry,int EntryLen,const Attribute &attribute);
        int compare(int Label, int EntryLen,const void* Entry, const Attribute & attribute,const void* key,const RID &rid);
        int compareEntry(int EntryLabel, int EntryLen,const void* Entry,int EntryLabel2,int EntryLen2,const void* Entry2,const Attribute & attribute);
        int compareRid(const RID & mine,const RID & given);
        void insertTree(IXFileHandle &ixFileHandle, int node,int EntryLen,void*Entry,const Attribute &attribute,int &ChildEntryLen,void* &ChildEntry);
        RC deleteTree(IXFileHandle &ixFileHandle,int parentNode, int node , int EntryLen,void* Entry,const Attribute& attribute,int & flag);
        template<typename T>
        void print(IXFileHandle &ixFileHandle,int node,Attribute attribute,std::ostream &out,std::map<T,std::vector<RID>>&key,std::map<std::string,std::vector<RID>>&keyVar) ;
        template<typename T>
        std::string Convert2String(const T &value)
        {
            std::stringstream ss;
            ss << value;
            return ss.str();
        }
        template<typename T>
        int findFirstSlot(IXFileHandle & ixFileHandle,const Attribute&attribute,const void* key,int startNode,bool inclusive);//return the first slotNum;
        template<typename T>
        void collectRID(IXFileHandle & ixFileHandle,void*buffer,int id,void*Entry,int EntryLen,const Attribute&attribute,std::map<T,std::vector<RID>>&key,std::map<std::string,std::vector<RID>>&keyVar);
        RC CompactEntry(IXFileHandle &ixFileHandle,int Label,void* buffer,void*Entry,int EntryLen,const Attribute &attribute,int& flag);
        void splitPage(int Label,void* buffer,void* newBuffer);
    private:
        bool isLeaf(void* buffer);
        bool hasEnoughSpace(void * buffer ,int EntryLen);
        void debugBufferFind(void* buffer,void* Entry,int EntryLen,const Attribute& attribute);
    };

    class IX_ScanIterator {friend class IndexManager;
    public:
        RC initScanIterator(IXFileHandle &ixFileHandle,
                            const Attribute attribute,
                            const void *lowKey,
                            const void *highKey,
                            bool lowKeyInclusive,
                            bool highKeyInclusive);
        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    private:
        int curPageNum = 0;
        int curSlotNum = 0;
        int curCount = 0;
        char* currentPage= nullptr;
        IXFileHandle* ixFileHandle = NULL;
        Attribute attribute;
        char *lowKey = nullptr;
        char *highKey = nullptr;
        bool lowKeyInclusive;
        bool highKeyInclusive;
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        unsigned pageNumCounter = 0;
        int deleted=0;
        int root = -1;

        RC readPage(PageNum pageNum, void *data);

        RC writePage(PageNum pageNum, const void *data);

        RC appendPage(const void *data);

        unsigned getNumberOfPages();

        bool checkSlotZero(int node);

        int getRoot() const;

        void setRoot(unsigned int root);

        FILE* fp = NULL;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };
}// namespace PeterDB
#endif // _ix_h_
