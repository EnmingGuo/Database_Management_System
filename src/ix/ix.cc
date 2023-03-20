#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        FILE* fp = fopen(fileName.c_str(), "r");
        if(fp) {
            fclose(fp);
            return -1;
        }
        fp = fopen(fileName.c_str(),"wb");
        //Create a hidden page which is used to save counters
        void* data = malloc(PAGE_SIZE);
        memset(data,0,sizeof(unsigned char)*PAGE_SIZE);
        int root = -1;
        memcpy((char*)data+16,&root,sizeof(int));
        fseek(fp,0,SEEK_SET);
        fwrite(data, sizeof(unsigned char), PAGE_SIZE,fp);
        free(data);
        fclose(fp);
        return 0;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if(remove(fileName.c_str()) != 0)
            return -1;
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        FILE* fp = fopen(fileName.c_str(), "r+b");
        //file does not exist or this fileHandle has already been bound to an open file
        if(fp == NULL || ixFileHandle.fp != NULL){
            return -1;
        }
        ixFileHandle.fp = fp;
        unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
        memset(buffer,0,sizeof(buffer));
        fseek(ixFileHandle.fp, 0, SEEK_SET);
        //Read 4 bytes a time, and transform to unsigned
        //The order is readPageCounter, writePageCounter,appendPageCounter and pageNum
        for(int i = 0; i < 5; i++){
            fseek(ixFileHandle.fp, i * 4, SEEK_SET);
            fread(buffer, sizeof(unsigned char), sizeof(unsigned), ixFileHandle.fp);
            switch (i){
                case 0:
                    memcpy(&ixFileHandle.ixReadPageCounter,buffer, sizeof(unsigned));
                    break;
                case 1:
                    memcpy(&ixFileHandle.ixWritePageCounter,buffer, sizeof(unsigned));
                    break;
                case 2:
                    memcpy(&ixFileHandle.ixAppendPageCounter,buffer, sizeof(unsigned));
                    break;
                case 3:
                    memcpy(&ixFileHandle.pageNumCounter,buffer, sizeof(unsigned));
                    break;
                case 4:
                    memcpy(&ixFileHandle.root,buffer, sizeof(unsigned));
                    break;
            }
        }
        free(buffer);
        fseek(ixFileHandle.fp, 0, SEEK_SET);
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if(!ixFileHandle.fp)
            return -1;

        fseek(ixFileHandle.fp,0,SEEK_SET);
        for(int i = 0; i < 5; i++){
            unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
            if(i==0){
                //for readPageCounter
                memcpy(buffer, &ixFileHandle.ixReadPageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back readPageCounter "<<*buffer<<std::endl;
            }
            else if(i==1){
                //for writePageCounter
                memcpy(buffer, &ixFileHandle.ixWritePageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back writePageCounter "<<*buffer<<std::endl;
            }
            else if(i==2){
                //for appendPageCounter
                memcpy(buffer, &ixFileHandle.ixAppendPageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back appendPageCounter "<<*buffer<<std::endl;
            }
            else if(i==3){
                //for pageNum
                memcpy(buffer,&ixFileHandle.pageNumCounter, sizeof(unsigned));
                //std::cout<<"I have finished the back pageNumber "<<*buffer<<std::endl;
            }
            else{
                memcpy(buffer,&ixFileHandle.root, sizeof(unsigned));
            }
            fseek(ixFileHandle.fp,4 * i, SEEK_SET);
            fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), ixFileHandle.fp);
            free(buffer);
        }
        fseek(ixFileHandle.fp,0,SEEK_SET);
        fclose(ixFileHandle.fp);
        ixFileHandle.fp = NULL;
        return 0;
    }

    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
       int root=ixFileHandle.getRoot();
       if(root==-1){
           void * buffer = malloc(PAGE_SIZE);
           CreateLeafPage(buffer,0,0,-1,-1);
           ixFileHandle.appendPage(buffer);
           ixFileHandle.appendPage(buffer);
           free(buffer);
           ixFileHandle.setRoot(0);
       }
       root=ixFileHandle.getRoot();
       void * Entry = malloc(PAGE_SIZE);
       int EntryLen;
       CreateLeafEntry(EntryLen,Entry,attribute,key,rid);
       void * ChildEntry =NULL;
       int ChildEntryLen=0;
       insertTree(ixFileHandle,root,EntryLen,Entry,attribute,ChildEntryLen,ChildEntry);
       free(Entry);
       return 0;
    }

    void IndexManager::splitPage(int Label ,void *buffer, void *newBuffer) {
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        int freeSpace;
        memcpy(&freeSpace,(char*)buffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        int reservedNum ;
        if(slotNum%2==1){
            reservedNum=slotNum/2+1;
        }
        else
            reservedNum=slotNum/2;
        void * slotBuffer= malloc (PAGE_SIZE);
        void * recordBuffer = malloc (PAGE_SIZE);
        int newLength;
        if(reservedNum+1<=slotNum){
            int start,lastOff,lastLen;
            memcpy(&start,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*(reservedNum+1)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&lastOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&lastLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum-1)*sizeof(unsigned),sizeof(unsigned));
            int end = lastOff+lastLen;
            memcpy(recordBuffer,(char*)buffer+start,end-start);
            memcpy(slotBuffer,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*slotNum*sizeof(unsigned),2*(slotNum-reservedNum)*sizeof(unsigned));

            /*修改现有reservedNum 成为slotNum*/
            memcpy((char*)buffer+PAGE_SIZE-3*sizeof(unsigned),&reservedNum,sizeof(unsigned));
            int reservedNewFreeSpace = freeSpace+(end-start)+2*(slotNum-reservedNum)*sizeof(unsigned);
            newLength=(end-start)+2*(slotNum-reservedNum)*sizeof(unsigned);
            memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&reservedNewFreeSpace,sizeof(unsigned));
        }
        //更新freespace的数值
        memcpy(&freeSpace,(char*)newBuffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        if(slotNum-reservedNum>0){
            memcpy(newBuffer,recordBuffer,newLength-2*(slotNum-reservedNum)*sizeof(unsigned));
            memcpy((char*)newBuffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*(slotNum-reservedNum)*sizeof(unsigned),slotBuffer,2*(slotNum-reservedNum)*sizeof(unsigned));
            int point =0;
            for(int i=1;i<=slotNum-reservedNum;i++){
                int len;
                memcpy(&len,(char*)newBuffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy((char*)newBuffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i)*sizeof(unsigned),&point,sizeof(unsigned));
                point+=len;
            }
            int NewBufferFreeSpace = freeSpace-newLength;
            memcpy((char*)newBuffer+PAGE_SIZE-sizeof(unsigned),&NewBufferFreeSpace,sizeof(unsigned));
            int NewSlotNum=slotNum-reservedNum;
            memcpy((char*)newBuffer+PAGE_SIZE-3*sizeof(unsigned),&NewSlotNum,sizeof(unsigned));
        }
        free(recordBuffer);
        free(slotBuffer);
    }

    RC IndexManager::deleteTree(IXFileHandle &ixFileHandle, int parentNode, int node, int EntryLen, void *Entry,
                                  const Attribute &attribute, int &flag) {
        void * buffer = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node,buffer);
        if(isLeaf(buffer)){
            ixFileHandle.readPage(node,buffer);
            int result= CompactEntry(ixFileHandle,0,buffer,Entry,EntryLen,attribute,flag);
            if(flag==1){
                int right,left;
                memcpy(&right,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned),sizeof(unsigned));
                memcpy(&left,(char*)buffer+PAGE_SIZE-5*sizeof(unsigned),sizeof(unsigned));
                void * newBuffer=malloc(PAGE_SIZE);
                if(left!=-1){
                    ixFileHandle.readPage(left,newBuffer);
                    memcpy((char*)newBuffer+PAGE_SIZE-6*sizeof(unsigned),&right,sizeof(unsigned));
                    ixFileHandle.writePage(left,newBuffer);
                }
                if(right!=-1){
                    ixFileHandle.readPage(right,newBuffer);
                    memcpy((char*)newBuffer+PAGE_SIZE-5*sizeof(unsigned),&left,sizeof(unsigned));
                    ixFileHandle.writePage(right,newBuffer);
                }
                free(newBuffer);
            }
            ixFileHandle.writePage(node,buffer);
            free(buffer);
            return result;
        }
        else{
            int slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            if(slotNum==0){
                free(buffer);
                return 0;
            }
            int position=slotNum+1;
            for(int i=1;i<=slotNum;i++){
                void * tempBuffer= malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-i*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(i*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                int result= compareEntry(0,EntryLen,Entry,1,Len,tempBuffer,attribute);
                if(result==-1){
                    position=i;
                    free(tempBuffer);
                    break;
                }
                free(tempBuffer);
            }
            int newNode;
            if(position==1+slotNum){
                void * tempBuffer= malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-slotNum*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(slotNum*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                memcpy(&newNode,(char*)tempBuffer+sizeof(unsigned),sizeof(unsigned));
                free(tempBuffer);
            }
            else{
                void * tempBuffer= malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-position*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(position*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                memcpy(&newNode,(char*)tempBuffer,sizeof(unsigned));
                free(tempBuffer);
            }
            int result=deleteTree(ixFileHandle,node,newNode,EntryLen,Entry,attribute,flag);
            if(flag==0){
                free(buffer);
                return result;
            }
            else{
                flag=0;
                if(slotNum==1){
                    void*tempBuffer=malloc(PAGE_SIZE);
                    int lastOff,lastLen;
                    memcpy(&lastOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*position*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&lastLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*position-1)*sizeof(unsigned),sizeof(unsigned));
                    memcpy(tempBuffer,(char*)buffer+lastOff,lastLen);
                    int left,right;
                    memcpy(&left,tempBuffer,sizeof(unsigned));
                    memcpy(&right,(char*)tempBuffer+sizeof(unsigned),sizeof(unsigned));
                    if(ixFileHandle.checkSlotZero(left)==true&&ixFileHandle.checkSlotZero(right)==true){
                        CompactEntry(ixFileHandle,1,buffer,tempBuffer,lastLen,attribute,flag);
                        ixFileHandle.writePage(node,buffer);
                    }
                    free(tempBuffer);
                }
                else if(slotNum>1){
                    if(position==slotNum+1){
                        void * tempBuffer=malloc(PAGE_SIZE);
                        int lastOff,lastLen;
                        memcpy(&lastOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*slotNum*sizeof(unsigned),sizeof(unsigned));
                        memcpy(&lastLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*slotNum-1)*sizeof(unsigned),sizeof(unsigned));
                        memcpy(tempBuffer,(char*)buffer+lastOff,lastLen);
                        CompactEntry(ixFileHandle,1,buffer,tempBuffer,lastLen,attribute,flag);
                        free(tempBuffer);
                    }
                    else if(position==1){
                        void * tempBuffer=malloc(PAGE_SIZE);
                        int lastOff,lastLen;
                        memcpy(&lastOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*position*sizeof(unsigned),sizeof(unsigned));
                        memcpy(&lastLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*position-1)*sizeof(unsigned),sizeof(unsigned));
                        memcpy(tempBuffer,(char*)buffer+lastOff,lastLen);
                        CompactEntry(ixFileHandle,1,buffer,tempBuffer,lastLen,attribute,flag);
                        free(tempBuffer);
                    }
                    else{
                        void * tempBuffer=malloc(PAGE_SIZE);
                        int lastOff,lastLen;
                        memcpy(&lastOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*position*sizeof(unsigned),sizeof(unsigned));
                        memcpy(&lastLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*position-1)*sizeof(unsigned),sizeof(unsigned));
                        memcpy(tempBuffer,(char*)buffer+lastOff,lastLen);
                        int right ;
                        memcpy(&right,(char*)tempBuffer+sizeof(unsigned),sizeof(unsigned));
                        int previousOff,previousLen;
                        memcpy(&previousOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*(position-1)*sizeof(unsigned),sizeof(unsigned));
                        memcpy(&previousLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*(position-1)-1)*sizeof(unsigned),sizeof(unsigned));
                        memcpy((char*)buffer+previousOff+sizeof(unsigned),&right,sizeof(unsigned));
                        CompactEntry(ixFileHandle,1,buffer,tempBuffer,lastLen,attribute,flag);
                        free(tempBuffer);
                    }
                    ixFileHandle.writePage(node,buffer);
                }

                if(flag== 1 && node == ixFileHandle.getRoot()){
                    void* zeroBuffer= malloc(PAGE_SIZE);
                    ixFileHandle.readPage(0,zeroBuffer);
                    int left=-1,right=-1,zeroSlotNum=0,zeroFreeSpace=PAGE_SIZE-leafHeaderSize*sizeof(unsigned),zeroLabel=0;
                    memcpy((char*)zeroBuffer+PAGE_SIZE-sizeof(unsigned),&zeroFreeSpace,sizeof(unsigned));
                    memcpy((char*)zeroBuffer+PAGE_SIZE-2*sizeof(unsigned),&zeroLabel,sizeof(unsigned));
                    memcpy((char*)zeroBuffer+PAGE_SIZE-6*sizeof(unsigned),&right,sizeof(unsigned));
                    memcpy((char*)zeroBuffer+PAGE_SIZE-5*sizeof(unsigned),&left,sizeof(unsigned));
                    ixFileHandle.writePage(0,zeroBuffer);
                    ixFileHandle.setRoot(0);
                }
                free(buffer);
                return result;
            }
        }

    }

    void IndexManager::insertTree(IXFileHandle &ixFileHandle, int node, int EntryLen, void *Entry, const Attribute &attribute,int &ChildEntryLen, void * &ChildEntry) {
        void * buffer = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node,buffer);
        if(isLeaf(buffer)){
            if(hasEnoughSpace(buffer,EntryLen)){
                debugBufferFind(buffer,Entry,EntryLen,attribute);
                //write,排序地写@！！！
                WriteEntry(ixFileHandle,buffer,0,Entry,EntryLen,attribute);
                ixFileHandle.writePage(node,buffer);
                free(buffer);
                ChildEntryLen=0;
                ChildEntry=NULL;
                return ;
            }
            else{
                //split 当前的buffer,把一部分移动出去，并且建立一个新的leafnode
                //创建一个新的索引节点（非叶节点），把这两个叶页的双向链条记得更改了，以及把对应的父亲页编号记得更改了。
               std::cout<<"即将分裂叶子结点"<<std::endl;
                //创建一个新的页，用于split !!!设置左右双向链表
                int right;
                memcpy(&right,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned),sizeof(unsigned));
                void * newBuffer = malloc(PAGE_SIZE);
                CreateLeafPage(newBuffer,0,-1,node,right);

                //用于分隔之前的Page
                splitPage(0,buffer,newBuffer);

                std::cout<<"-------叶子结点分裂了---------"<<std::endl;
                debugBufferFind(buffer,Entry,EntryLen,attribute);
                debugBufferFind(newBuffer,Entry,EntryLen,attribute);
                std::cout<<"----------------------------"<<std::endl;

                int debugBufferSlotNum,debugNewBufferSlotNum;
                memcpy(&debugBufferSlotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
                memcpy(&debugNewBufferSlotNum,(char*)newBuffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));

                //把新的Entry插入后面一页，有bug
                int result=CheckNewOrOriginal(0,buffer,newBuffer,EntryLen,Entry,attribute);
                if(result==1){
                    WriteEntry(ixFileHandle,newBuffer,0,Entry,EntryLen,attribute);
                }
                else if(result==0){
                    WriteEntry(ixFileHandle,buffer,0,Entry,EntryLen,attribute);
                }
                else{
                    std::cerr<<"出现问题了！！，前后两页都塞不进去这个东西！！！"<<std::endl;
                    free(buffer);
                    free(newBuffer);
                    ChildEntryLen=0;
                    ChildEntry=NULL;
                    return;
                }
                /*debug区域*/
                std::cout<<"-------分裂之后的聚合---------"<<std::endl;
                debugBufferFind(buffer,Entry,EntryLen,attribute);
                debugBufferFind(newBuffer,Entry,EntryLen,attribute);
                std::cout<<"----------------------------"<<std::endl;
                /*把更改的NewBuffer加在文件中*/
                ixFileHandle.appendPage(newBuffer);
                /*修改Buffer的右面编号*/
                int newBufferPageNum=ixFileHandle.getNumberOfPages()-1;

                /*修改之前右侧的左链表*/
                if(right!=-1){
                    void * rightBuffer=malloc(PAGE_SIZE);
                    ixFileHandle.readPage(right,rightBuffer);
                    memcpy((char*)rightBuffer+PAGE_SIZE-5*sizeof(unsigned),&newBufferPageNum,sizeof(unsigned));
                    ixFileHandle.writePage(right,rightBuffer);
                }
                /*把原始界面的右结点改成新生成的界面*/
                memcpy((char*)buffer+PAGE_SIZE-6*sizeof(unsigned),&newBufferPageNum,sizeof(unsigned));
                /*完成之后，把初始界面给我写回去，冲呀*/
                ixFileHandle.writePage(node,buffer);
                free(buffer);

                /*制作ChildEntry结点*/
                ChildEntry = malloc(PAGE_SIZE);
                memcpy(ChildEntry,&node,sizeof(unsigned));
                memcpy((char*)ChildEntry+sizeof(unsigned),&newBufferPageNum,sizeof(unsigned));
                int firstOff,firstLen;
                memcpy(&firstOff,(char*)newBuffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&firstLen,(char*)newBuffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-sizeof(unsigned),sizeof(unsigned));
                memcpy((char*)ChildEntry+sizeof(unsigned)*2,(char*)newBuffer+firstOff,firstLen);
                ChildEntryLen=2*sizeof(unsigned)+firstLen;
                free(newBuffer);

                /*判断当前的是不是根，如果是根就再换一个根节点*/
                if(node==ixFileHandle.getRoot()){
                    void * RootBuffer = malloc(PAGE_SIZE);
                    memset(RootBuffer,0,PAGE_SIZE);
                    CreateNonLeafPage(RootBuffer,1,-1);

                    WriteEntry(ixFileHandle,RootBuffer,1,ChildEntry,ChildEntryLen,attribute);
                    ixFileHandle.appendPage(RootBuffer);
                    int newRoot=ixFileHandle.getNumberOfPages()-1;
                    ixFileHandle.setRoot(newRoot);
                    free(ChildEntry);
                    ChildEntry=NULL;
                    ChildEntryLen=0;
                    return ;
                }
                return ;
            }
        }
        else{
            int slotNum ;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            int position=slotNum+1;
            for(int i=1;i<=slotNum;i++){
                void * tempBuffer=malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-i*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(i*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                int result= compareEntry(0,EntryLen,Entry,1,Len,tempBuffer,attribute);
                if(result==-1){
                    position=i;
                    free(tempBuffer);
                    break;
                }
                free(tempBuffer);
            }
            int newNode;
            if(position==slotNum+1){
                void * tempBuffer= malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-slotNum*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(slotNum*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                memcpy(&newNode,(char*)tempBuffer+sizeof(unsigned),sizeof(unsigned));
                free(tempBuffer);
            }
            else{
                void * tempBuffer= malloc(PAGE_SIZE);
                int Off,Len;
                memcpy(&Off,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-position*2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&Len,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(position*2-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempBuffer,(char*)buffer+Off,Len);
                memcpy(&newNode,(char*)tempBuffer,sizeof(unsigned));
                free(tempBuffer);
            }
            /*insert(.R;, entry, newchildentry); / / recurs'ively, insert entry*/
            insertTree(ixFileHandle,newNode,EntryLen,Entry,attribute,ChildEntryLen,ChildEntry);
            /*if newchildentry is null, return; / / usual case; didn't split child*/
            if(ChildEntry==NULL&&ChildEntryLen==0){
                free(buffer);
                return;
            }
            /*else, / / we split child, must insert *newchildentry in N*/
            else{
                /*if N has space, / / usual case*/
                if(hasEnoughSpace(buffer,ChildEntryLen)){
                    /*put *newchildentry on it, set newchildentry to null, return;*/
                    WriteEntry(ixFileHandle,buffer,1,ChildEntry,ChildEntryLen,attribute);
                    ixFileHandle.writePage(node,buffer);
                    free(ChildEntry);
                    free(buffer);
                    ChildEntry=NULL;
                    ChildEntryLen=0;
                    return;
                }
                else{
                    /*split N: / / 2d + 1 key values and 2d + 2 nodepointers
                     first d key values and d + 1 nodepointers stay,
                     last d keys and d + 1 pointers move to new node, N2;
                     / / *newchildentry set to guide searches between Nand*/
                    void * newBuffer = malloc(PAGE_SIZE);
                    CreateNonLeafPage(newBuffer,1,-1);
                    splitPage(1,buffer,newBuffer);
                    int result=CheckNewOrOriginal(1,buffer,newBuffer,ChildEntryLen,ChildEntry,attribute);
                    if(result==1){
                        WriteEntry(ixFileHandle,newBuffer,1,ChildEntry,ChildEntryLen,attribute);
                    }
                    else if(result==0){
                        WriteEntry(ixFileHandle,buffer,1,ChildEntry,ChildEntryLen,attribute);
                    }
                    ixFileHandle.appendPage(newBuffer);
                    ixFileHandle.writePage(node,buffer);
                    free(buffer);
                    /*newchildentry = & ((smallest key value on N2, pointer to N2));*/
                    /*制作ChildEntry结点*/
                    int newBufferPageNum= ixFileHandle.getNumberOfPages()-1;
                    int firstOff,firstLen;
                    memcpy(&firstOff,(char*)newBuffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&firstLen,(char*)newBuffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-sizeof(unsigned),sizeof(unsigned));
                    /*制作孩子*/

                    ChildEntryLen=firstLen;
                    memcpy(ChildEntry,(char*)newBuffer+firstOff,firstLen);
                    /*push_up的工作*/
                    CompactEntry(ixFileHandle,1,newBuffer,ChildEntry,ChildEntryLen,attribute,firstLen);
                    /*再修改一下下对应的指针*/
                    memcpy(ChildEntry,&node,sizeof(unsigned));
                    memcpy((char*)ChildEntry+sizeof(unsigned),&newBufferPageNum,sizeof(unsigned));
                    /*我再把这个更改写回去*/
                    ixFileHandle.writePage(newBufferPageNum,newBuffer);
                    free(newBuffer);

                    /*if N is the root, / / root node was just split create new node with (pointer to N, *newchildentry);
                     * make the tree's root-node pointer point to the new node;*/

                    if(node==ixFileHandle.getRoot()){
                        void * RootBuffer = malloc(PAGE_SIZE);
                        CreateNonLeafPage(RootBuffer,1,-1);
                        WriteEntry(ixFileHandle,RootBuffer,1,ChildEntry,ChildEntryLen,attribute);
                        ixFileHandle.appendPage(RootBuffer);
                        int newRoot=ixFileHandle.getNumberOfPages()-1;
                        ixFileHandle.setRoot(newRoot);
                        free(ChildEntry);
                        ChildEntry=NULL;
                        ChildEntryLen=0;
                        return ;
                    }
                    return ;

                }
            }

        }
    }

    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int root =ixFileHandle.getRoot();
        if(root==-1)return -1;
        void * Entry = malloc (PAGE_SIZE);
        int EntryLen;
        CreateLeafEntry(EntryLen,Entry,attribute,key,rid);
        int Vacant=0;
        int result=deleteTree(ixFileHandle,-1,root,EntryLen,Entry,attribute,Vacant);
        free(Entry);
        if(result==0){
            ixFileHandle.deleted=1;
        }
        return result;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return ix_ScanIterator.initScanIterator(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    }

    template<typename T>
    void IndexManager::print(IXFileHandle &ixFileHandle, int node, Attribute attribute, std::ostream &out,std::map<T,std::vector<RID>>&key,std::map<std::string,std::vector<RID>>&keyVar) {
        void * buffer = malloc (PAGE_SIZE);
        ixFileHandle.readPage(node,buffer);
        if(IndexManager::isLeaf(buffer)){
            //{"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]}
            out<<"{\"keys\": [";
            int slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            int cnt=0;
            for(int i=1;i<=slotNum;i++){
                int tempOff,tempLen;
                memcpy(&tempOff,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-2*i*sizeof(unsigned),sizeof(unsigned));
                memcpy(&tempLen,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
                void *tempEntry=malloc(PAGE_SIZE);
                memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
                T tempkey;
                std::string tempVar;
                if(attribute.type!=TypeVarChar){
                    memcpy(&tempkey,tempEntry,sizeof(unsigned));
                    auto result=key.find(tempkey);
                    if(result==key.end()){
                        if(cnt!=0)out<<",";
                        cnt++;
                        key[tempkey]=std::vector<RID>();
                        out<<"\""+Convert2String(tempkey)+":[";
                        collectRID(ixFileHandle,buffer,i,tempEntry,tempLen,attribute,key,keyVar);
                        int counter=0;
                        for(auto item:key[tempkey]){
                            if(counter!=0)out<<",";
                            out<<"("+ Convert2String(item.pageNum)+","+ Convert2String(item.slotNum)+")";
                            counter++;
                        }
                        out<<"]\"";
                    }
                }
                else{
                    tempVar=std::string((char*)tempEntry,tempLen-2*sizeof(unsigned));
                    auto result=keyVar.find(tempVar);
                    if(result==keyVar.end()){
                        if(cnt!=0)out<<",";
                        cnt++;
                        keyVar[tempVar]=std::vector<RID>();
                        out<<"\""+tempVar+":[";
                        collectRID(ixFileHandle,buffer,i,tempEntry,tempLen,attribute,key,keyVar);
                        int counter=0;
                        for(auto item:keyVar[tempVar]){
                            if(counter!=0)out<<",";
                            out<<"("+ Convert2String(item.pageNum)+","+ Convert2String(item.slotNum)+")";
                            counter++;
                        }
                        out<<"]\"";
                    }
                }
                free(tempEntry);
            }
            out<<"]}";
        }
        else{
            out<<"{\"keys\": [";
            /*这里塞key*/
            int slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            for(int i=1;i<=slotNum;i++){
                if(i!=1)out<<",";
                int tempOff,tempLen;
                void * tempEntry=malloc(PAGE_SIZE);
                memcpy(&tempOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*i*sizeof(unsigned),sizeof(unsigned));
                memcpy(&tempLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
                if(attribute.type==TypeVarChar){
                    std::string key=std::string((char*)tempEntry+2*sizeof(unsigned),tempLen-4*sizeof(unsigned));
                    out<<"\""+key+"\"";
                }
                else{
                    T Real;
                    memcpy(&Real,(char*)tempEntry+2*sizeof(unsigned),tempLen-4*sizeof(unsigned));
                    out<<"\""+ Convert2String(Real)+"\"";
                }
                free(tempEntry);
            }
            out<<"],\"children\": [";
            /*这里塞孩子，递归调用*/
            for(int i=1;i<=slotNum;i++){
                if(i!=1)out<<",";
                int tempOff,tempLen;
                void* tempEntry = malloc(PAGE_SIZE);
                memcpy(&tempOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*i*sizeof(unsigned),sizeof(unsigned));
                memcpy(&tempLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
                int left;
                memcpy(&left,(char*)tempEntry,sizeof(unsigned));
                print(ixFileHandle,left,attribute,out,key,keyVar);
                if(i==slotNum){
                    out<<",";
                    int right;
                    memcpy(&right,(char*)tempEntry+sizeof(unsigned),sizeof(unsigned));
                    print(ixFileHandle,right,attribute,out,key,keyVar);
                }
                free(tempEntry);
            }
            out<<"]}";
        }
        free(buffer);
        return ;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        int root=ixFileHandle.getRoot();
        if(root==-1){
            return -1;
        }
        IndexManager p;
        std::map<std::string,std::vector<RID>>keyVar;
        if(attribute.type==TypeInt){
            std::map<int,std::vector<RID>>keyInt;
            p.print(ixFileHandle,root,attribute,out,keyInt,keyVar);
        }
        else{
            std::map<float,std::vector<RID>>keyReal;
            p.print(ixFileHandle,root,attribute,out,keyReal,keyVar);
        }
        return 0;
    }

    RC IndexManager::CreateLeafPage(void *buffer, int Label, int Parent, int Left, int Right) {
        memset(buffer,0,PAGE_SIZE);
        unsigned free= PAGE_SIZE-leafHeaderSize*sizeof(unsigned);
        memcpy((char*)buffer+PAGE_SIZE-2*sizeof(unsigned),&Label,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&free,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-4*sizeof(unsigned),&Parent,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-5*sizeof(unsigned),&Left,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-6*sizeof(unsigned),&Right,sizeof(unsigned));
        return 0;
    }

    RC IndexManager::CreateNonLeafPage(void *buffer, int Label, int Parent) {
        memset(buffer,0,PAGE_SIZE);
        unsigned free = PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned);
        memcpy((char*)buffer+PAGE_SIZE-2*sizeof(unsigned),&Label,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&free,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-4*sizeof(unsigned),&Parent,sizeof(unsigned));
        return 0;
    }

    RC IndexManager::CreateLeafEntry(int &length, void* Entry, const Attribute &attribute, const void *key, const RID &rid) {
        switch(attribute.type){
            case TypeInt:
            case TypeReal:
                length = sizeof(unsigned) + 2*sizeof(unsigned);
                memcpy(Entry,key,sizeof(unsigned));
                memcpy((char*)Entry+sizeof(unsigned),&rid.pageNum,sizeof(unsigned));
                memcpy((char*)Entry+2*sizeof(unsigned),&rid.slotNum,sizeof(unsigned));
                break;
            case TypeVarChar:
                unsigned VarLength;
                memcpy(&VarLength,key,sizeof(unsigned));
                length= VarLength + 2*sizeof(unsigned);
                memcpy((char*)Entry,(char*)key+sizeof(unsigned),VarLength);
                memcpy((char*)Entry+VarLength,&rid.pageNum,sizeof(unsigned));
                memcpy((char*)Entry+sizeof(unsigned)+VarLength,&rid.slotNum,sizeof(unsigned));
                break;
        }
        return 0;
    }

    RC IndexManager::WriteEntry(IXFileHandle &ixFileHandle, void* buffer, int Label, void *Entry, int EntryLen,const Attribute &attribute) {
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        int freeSpace;
        memcpy(&freeSpace,(char*)buffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        int position=slotNum+1;
        for(int i=1;i<=slotNum;i++){
            int tempOff,tempLen;
            memcpy(&tempOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-i*2*sizeof(unsigned),sizeof(unsigned));
            memcpy(&tempLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(i*2-1)*sizeof(unsigned),sizeof(unsigned));
            void * tempEntry=malloc(PAGE_SIZE); //!!!
            memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
            int result= compareEntry(Label,tempLen,tempEntry,Label,EntryLen,Entry,attribute);
            free(tempEntry);
            if(result==1){
                position=i;
                break;
            }
        }
        if(position==slotNum+1){
            int lastOff,lastLen,newOff=0;
            if(slotNum>0){
                memcpy(&lastOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*slotNum*sizeof(unsigned),sizeof(unsigned));
                memcpy(&lastLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum-1)*sizeof(unsigned),sizeof(unsigned));
                newOff=lastLen+lastOff;
            }
            memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*position*sizeof(unsigned),&newOff,sizeof(unsigned));
            memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*position-1)*sizeof(unsigned),&EntryLen,sizeof(unsigned));
            memcpy((char*)buffer+newOff,Entry,EntryLen);
            int newSlotNum=slotNum+1;
            int newFreeSpace= freeSpace-(2*sizeof(unsigned)+EntryLen);
            memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&newFreeSpace,sizeof(unsigned));
            memcpy((char*)buffer+PAGE_SIZE-3*sizeof(unsigned),&newSlotNum,sizeof(unsigned));
        }
        else{
            void * temp=malloc(PAGE_SIZE);
            int start,end,lastOff,lastLen;
            memcpy(&start,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*position)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&lastOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&lastLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum-1)*sizeof(unsigned),sizeof(unsigned));
            end=lastOff+lastLen;
            memcpy(temp,(char*)buffer+start,end-start);
            memcpy((char*)buffer+start,Entry,EntryLen);
            memcpy((char*)buffer+start+EntryLen,temp,end-start);
            free(temp);
            for(int i=slotNum+1;i>=position;i--){
                if(i==position){
                    memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i)*sizeof(unsigned),&start,sizeof(unsigned));
                    memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),&EntryLen,sizeof(unsigned));
                }
                else{
                    int previousOff,previousLen;
                    memcpy(&previousOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(i-1))*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&previousLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(i-1)-1)*sizeof(unsigned),sizeof(unsigned));
                    int newAfterOff=previousOff+EntryLen;
                    memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i)*sizeof(unsigned),&newAfterOff,sizeof(unsigned));
                    memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),&previousLen,sizeof(unsigned));

                }
            }

            if(Label==1){
                int right ;
                memcpy(&right,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                int NextOff,NextLen;
                memcpy(&NextOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(1+position))*sizeof(unsigned),sizeof(unsigned));
                memcpy(&NextLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(1+position)-1)*sizeof(unsigned),sizeof(unsigned));
                memcpy((char*)buffer+NextOff,&right,sizeof(unsigned));
            }
            int newSlotNum=slotNum+1;
            int newFreeSpace= freeSpace-(2*sizeof(unsigned)+EntryLen);
            memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&newFreeSpace,sizeof(unsigned));
            memcpy((char*)buffer+PAGE_SIZE-3*sizeof(unsigned),&newSlotNum,sizeof(unsigned));
        }
        return 0;
    }


    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if(this->ixFileHandle->deleted==1){
            this->curSlotNum--;
            this->ixFileHandle->deleted=0;
        }
        if(this->curPageNum==-1)return -1;
        void * buffer = malloc(PAGE_SIZE);
        this->ixFileHandle->readPage(this->curPageNum,buffer);
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        if(this->curSlotNum>slotNum){
            int right;
            memcpy(&right,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned),sizeof(unsigned));
            if(right==-1){
                free(buffer);
                return -1;
            }
            else{
                this->curPageNum=right;
                this->curSlotNum=1;
            }
        }
        this->ixFileHandle->readPage(this->curPageNum,buffer);
        int tempOff,tempLen;
        memcpy(&tempOff,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned)-2*this->curSlotNum*sizeof(unsigned),sizeof(unsigned));
        memcpy(&tempLen,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned)-(2*this->curSlotNum-1)*sizeof(unsigned),sizeof(unsigned));
        void* tempEntry=malloc(PAGE_SIZE);
        memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
        memcpy(&rid.pageNum,(char*)tempEntry+tempLen-2*sizeof(unsigned),sizeof(unsigned));
        memcpy(&rid.slotNum,(char*)tempEntry+tempLen-sizeof(unsigned),sizeof(unsigned));
        if(this->attribute.type==TypeVarChar){
            int strLen=tempLen-2*sizeof(unsigned);
            if(this->highKey == nullptr){
                memcpy(key,&strLen,sizeof(unsigned));
                memcpy((char*)key+sizeof(unsigned),(char*)tempEntry,tempLen-2*sizeof(unsigned));
            }
            else{
                int highKeyLen;
                memcpy(&highKeyLen,this->highKey,sizeof(unsigned));
                std::string Var1,Var2;
                Var1=std::string((char*)this->highKey+sizeof(unsigned),highKeyLen);
                Var2=std::string((char*)tempEntry,strLen);
                if(this->highKeyInclusive==true){
                    if(Var2>Var1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,&strLen,sizeof(unsigned));
                    memcpy((char*)key+sizeof(unsigned),(char*)tempEntry,tempLen-2*sizeof(unsigned));
                }
                else{
                    if(Var2>=Var1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,&strLen,sizeof(unsigned));
                    memcpy((char*)key+sizeof(unsigned),(char*)tempEntry,tempLen-2*sizeof(unsigned));
                }
            }
        }
        else if(this->attribute.type==TypeInt){
            if(this->highKey== nullptr){
                memcpy(key,(char*)tempEntry,sizeof(unsigned));
            }
            else{
                int Int1,Int2;
                memcpy(&Int2,tempEntry,sizeof(unsigned));
                memcpy(&Int1,this->highKey,sizeof(unsigned));
                if(this->highKeyInclusive==true){
                    if(Int2>Int1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,(char*)tempEntry,sizeof(unsigned));
                }
                else{
                    if(Int2>=Int1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,(char*)tempEntry,sizeof(unsigned));

                }
            }
        }
        else{
            if(this->highKey== nullptr){
                memcpy(key,(char*)tempEntry,sizeof(unsigned));
            }
            else{
                float Real1,Real2;
                memcpy(&Real2,tempEntry,sizeof(unsigned));
                memcpy(&Real1,this->highKey,sizeof(unsigned));
                if(this->highKeyInclusive==true){
                    if(Real2>Real1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,(char*)tempEntry,sizeof(unsigned));
                }
                else{
                    if(Real2>=Real1){
                        free(tempEntry);
                        free(buffer);
                        return -1;
                    }
                    memcpy(key,(char*)tempEntry,sizeof(unsigned));

                }
            }
        }
        free(tempEntry);
        free(buffer);
        this->curSlotNum++;
        return 0;
    }

    RC IX_ScanIterator::close() {
        if(this->currentPage != nullptr)free(this->currentPage);
        if(this->lowKey != nullptr)free(this->lowKey);
        if(this->highKey!= nullptr)free(this->highKey);
        if(this->ixFileHandle!= nullptr)this->ixFileHandle= nullptr;
        return 0;
    }

    RC IX_ScanIterator::initScanIterator(IXFileHandle &ixFileHandle, const Attribute attribute, const void *lowKey,
                                         const void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        if(ixFileHandle.fp==NULL)return -1;
        this->attribute=attribute;
        int strLength =0;
        if(lowKey != nullptr){
            this->lowKey=(char*)malloc(PAGE_SIZE);
            switch(attribute.type){
                case TypeVarChar:
                    memcpy(&strLength,lowKey,sizeof(unsigned));
                    memcpy(this->lowKey,lowKey,sizeof(unsigned)+strLength);
                    break;
                case TypeInt:
                case TypeReal:
                    memcpy(this->lowKey,lowKey,sizeof(unsigned));
                    break;
            }

        }
        if(highKey != nullptr){
            this->highKey=(char*)malloc(PAGE_SIZE);
            switch(attribute.type){
                case TypeVarChar:
                    memcpy(&strLength,highKey,sizeof(unsigned));
                    memcpy(this->highKey,highKey,sizeof(unsigned)+strLength);
                    break;
                case TypeReal:
                case TypeInt:
                    memcpy(this->highKey,highKey,sizeof(unsigned));
                    break;
            }
        }
        this->lowKeyInclusive=lowKeyInclusive;
        this->highKeyInclusive=highKeyInclusive;
        this->ixFileHandle=&ixFileHandle;
        this->ixFileHandle->deleted=0;
        this->currentPage=(char*)malloc(PAGE_SIZE);
        if(this->lowKey!= nullptr){
            RID temp;
            if(this->lowKeyInclusive==true){
                temp.slotNum=0;
                temp.pageNum=0;
            }
            else{
                temp.slotNum=5000;
                temp.pageNum=100000;
            }

            IndexManager a;
            this->curPageNum=a.find(ixFileHandle, this->lowKey,attribute,temp);
            if(this->curPageNum==-1)return -1;
            if(this->attribute.type==TypeInt){
                this->curSlotNum=a.findFirstSlot<int>(ixFileHandle,attribute,this->lowKey,this->curPageNum,this->lowKeyInclusive);
            }
            else{
                this->curSlotNum=a.findFirstSlot<float>(ixFileHandle,attribute,this->lowKey,this->curPageNum,this->lowKeyInclusive);
            }
            if(this->curSlotNum==-1)return -1;
        }
        else{
            this->curPageNum=0;
            this->curSlotNum=1;
            void * buffer = malloc (PAGE_SIZE);
            this->ixFileHandle->readPage(this->curPageNum,buffer);
            int slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            while(this->curSlotNum>slotNum){
                int right;
                memcpy(&right,(char*)buffer+PAGE_SIZE-6*sizeof(unsigned),sizeof(unsigned));
                if(right==-1){
                    this->curPageNum=0;
                    this->curSlotNum=1;
                    break;
                }
                this->ixFileHandle->readPage(right,buffer);
                memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
                this->curPageNum=right;
            }
            free(buffer);
        }
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
        pageNumCounter = 0;
        root = -1;

    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        if(!fp)
            return -1;
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

    int IXFileHandle::getRoot() const {
        return root;
    }


    void IXFileHandle::setRoot(unsigned int root) {
        IXFileHandle::root = root;
    }

    RC IXFileHandle::readPage(PageNum pageNum, void *data) {
        //first check the fl
        if(fp == NULL){
            return -1;
        }
        //second check whether the pageNum is proper.
        if(pageNum>=pageNumCounter){//pageNum start with 0; while pageNumber is a counter.
            return -1;
        }
        fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        unsigned bufferNum=fread(data,sizeof(unsigned char),PAGE_SIZE,fp);
        if(bufferNum != PAGE_SIZE){
            return -1;
        }
        ixReadPageCounter=ixReadPageCounter+1;
        fseek(fp,0,SEEK_SET);
        return 0;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
        if(fp==NULL){
            return -1;
        }
        if(pageNum>=pageNumCounter){
            return -1;
        }
        fseek(fp, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        unsigned bufferNum= fwrite(data,sizeof(unsigned char),PAGE_SIZE,fp);
        if(bufferNum!=PAGE_SIZE){
            return -1;
        }
        ixWritePageCounter= ixWritePageCounter+1;
        fseek(fp,0,SEEK_SET);
        return 0;
    }

    RC IXFileHandle::appendPage(const void *data) {
        if(fp==NULL){
            return -1;
        }
        if(fseek(fp,((pageNumCounter + 1) * PAGE_SIZE), SEEK_SET)!=0){//fseek error
            return -1;
        }
        unsigned bufferNum= fwrite(data,sizeof(unsigned char),PAGE_SIZE,fp);
        if(bufferNum!=PAGE_SIZE){
            return -1;
        }
        pageNumCounter=pageNumCounter+1;
        ixAppendPageCounter= ixAppendPageCounter+1;
        fseek(fp, 0, SEEK_SET);
        return 0;
    }

    unsigned IXFileHandle::getNumberOfPages() {
        if(fp==NULL)
            return -1;
        return pageNumCounter;
    }

    bool IXFileHandle::checkSlotZero(int node) {
       void * buffer=malloc(PAGE_SIZE);
        readPage(node,buffer);
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        free(buffer);
        if(slotNum==0)return true;
        else return false;
    }

    bool IndexManager::isLeaf(void * buffer) {
        int label;
        memcpy(&label,(char*)buffer+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
        if(label==0)return true;
        else return false;
    }

    bool IndexManager::hasEnoughSpace(void * buffer,int EntryLen) {
        int freeSpace;
        memcpy(&freeSpace,(char*)buffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        int free=freeSpace -(8+EntryLen);
        if(free>5)return true;
        return false;

    }

    int IndexManager::find(IXFileHandle &ixFileHandle,const void *key, const Attribute &attribute, const RID &rid) {
        return treeSearch(ixFileHandle,ixFileHandle.getRoot(),key,attribute,rid);
    }

    int IndexManager::treeSearch(IXFileHandle &ixFileHandle, int curNode, const void *key, const Attribute &attribute,const RID &rid) {
        if(curNode == -1)return -1;
        void* buffer= malloc(PAGE_SIZE);//!!!!
        ixFileHandle.readPage(curNode,buffer);
        if(isLeaf(buffer)){
            free(buffer);
            return curNode;
        }
        else{
            int slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
            if(slotNum>0){
                void * Entry=malloc(PAGE_SIZE);//!!!!
                int firstOff,firstLen;
                memcpy(&firstOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&firstLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-sizeof(unsigned),sizeof(unsigned));
                memcpy(Entry,(char*)buffer+firstOff,firstLen);
                if(compare(1,firstLen,Entry,attribute,key,rid)==1){
                    int left;
                    memcpy(&left,Entry,sizeof(unsigned));
                    free(buffer);
                    free(Entry);
                    return treeSearch(ixFileHandle,left,key,attribute,rid);
                }
                else{
                    int lastOff,lastLen;
                    memcpy(&lastOff,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-slotNum*2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&lastLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(slotNum*2-1)*sizeof(unsigned),sizeof(unsigned));
                    memset(Entry,0,PAGE_SIZE);
                    memcpy(Entry,(char*)buffer+lastOff,lastLen);
                    int result=compare(1,lastLen,Entry,attribute,key,rid);
                    if(result==-1||result==0){
                        int right;
                        memcpy(&right,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                        free(buffer);
                        free(Entry);
                        return treeSearch(ixFileHandle,right,key,attribute,rid);
                    }
                    else{
                        int left = -1;
                        for(int i=2;i<slotNum;i++){
                            int tempOff,tempLen;
                            memcpy(&tempOff,(char*) buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-i*2*sizeof(unsigned),sizeof(unsigned));
                            memcpy(&tempLen,(char*)buffer+PAGE_SIZE-nonleafHeaderSize*sizeof(unsigned)-(i*2-1)*sizeof(unsigned),sizeof(unsigned));
                            memset(Entry,0,PAGE_SIZE);
                            memcpy(Entry,(char*)buffer+tempOff,tempLen);
                            result=compare(1,tempLen,Entry,attribute,key,rid);
                            if(result==1){
                                memcpy(&left,Entry,sizeof(unsigned));
                                free(buffer);
                                free(Entry);
                                return treeSearch(ixFileHandle,left,key,attribute,rid);
                            }
                        }
                        free(buffer);
                        free(Entry);
                        return treeSearch(ixFileHandle,left,key,attribute,rid);
                    }

                }

            }
            else{
                free(buffer);
                return -1;
            }
        }
    }
    // mine > given 大于返回 1; mine == given 等于返回 0 mine < given 返回 -1；
    //Label -> 0 leaf ; Label -> 1 nonleaf
    int IndexManager::compare(int Label,int EntryLen,const void *Entry, const Attribute &attribute, const void *key, const RID &rid) {
        int mineInt,givenInt;
        float mineReal,givenReal;
        std::string mineVar,givenVar;
        int mineVarLen=0,givenVarLen = 0;
        RID mineRid,givenRid=rid;
        switch(attribute.type){
            case TypeVarChar:
                memcpy(&givenVarLen,(char*)key,sizeof(unsigned));
                givenVar = std::string((char*)key + sizeof(int), givenVarLen);
                if(Label==1){//non-leaf
                    mineVarLen=EntryLen-sizeof(unsigned)*4;
                    mineVar=std::string((char*)Entry+2*sizeof(unsigned),mineVarLen);
                    memcpy(&mineRid.pageNum,(char*)Entry+mineVarLen+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+3*sizeof(unsigned)+mineVarLen,sizeof(unsigned));

                }
                else{
                    mineVarLen=EntryLen-sizeof(unsigned)*2;
                    mineVar=std::string((char*)Entry,mineVarLen);
                    memcpy(&mineRid.pageNum,(char*)Entry+mineVarLen,sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+sizeof(unsigned)+mineVarLen,sizeof(unsigned));
                }
                if(mineVar > givenVar)return 1;
                else if(mineVar < givenVar)return -1;
                else return compareRid(mineRid,givenRid);
                break;

            case TypeReal:
                memcpy(&givenReal,key,sizeof(unsigned));
                if(Label==1){//non-leaf
                    memcpy(&mineReal,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+4*sizeof(unsigned),sizeof(unsigned));
                }
                else{//leaf
                    memcpy(&mineReal,(char*)Entry,sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(mineReal > givenReal)return 1;
                else if(mineReal < givenReal)return -1;
                else return compareRid(mineRid,givenRid);
                break;
            case TypeInt:
                memcpy(&givenInt,key,sizeof(unsigned));
                if(Label==1){//non-leaf
                    memcpy(&mineInt,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+4*sizeof(unsigned),sizeof(unsigned));
                }
                else{//leaf
                    memcpy(&mineInt,(char*)Entry,sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(mineInt > givenInt)return 1;
                else if(mineInt < givenInt)return -1;
                else {
                    return compareRid(mineRid,givenRid);
                }
                break;
        }
    }

    int IndexManager::compareRid(const RID &mine, const RID &given) {
        if(mine.pageNum<given.pageNum)return -1;
        else if(mine.pageNum>given.pageNum)return 1;
        else{
            if(mine.slotNum<given.slotNum)return -1;
            else if(mine.slotNum>given.slotNum)return 1;
            else return 0;
        }
    }

    int IndexManager::CheckNewOrOriginal(int Label,void *buffer, void *newBuffer, const int EntryLen, const void *Entry,const Attribute& attribute) {
        int bufferFreeSpace,newBufferFreeSpace;
        memcpy(&bufferFreeSpace,(char*)buffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        memcpy(&newBufferFreeSpace,(char*)newBuffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        void * comparedEntry=malloc(PAGE_SIZE);//!!!
        int bufferSlotNum,newBufferSlotNum;
        memcpy(&bufferSlotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        memcpy(&newBufferSlotNum,(char*)newBuffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        if(bufferSlotNum>=1){
            int Off,Len;
            memcpy(&Off,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*bufferSlotNum*sizeof(unsigned),sizeof(unsigned));
            memcpy(&Len,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*bufferSlotNum-1)*sizeof(unsigned),sizeof(unsigned));
            memcpy(comparedEntry,(char*)buffer+Off,Len);
            int result= compareEntry(Label,EntryLen,Entry,Label,Len,comparedEntry,attribute);
            if(result==-1){
                free(comparedEntry);
                if(bufferFreeSpace-(2*sizeof(unsigned)+EntryLen)>2){
                    return 0;
                }
                else{
                    return -1;
                }
            }
            else{
                free(comparedEntry);
                if(newBufferFreeSpace-(2*sizeof(unsigned)+EntryLen)>2){
                    return 1;
                }
                else{
                    return -1;
                }
            }

        }
        free(comparedEntry);
        return 0;
    }

    int IndexManager::compareEntry(int EntryLabel, int EntryLen, const void *Entry, int EntryLabel2,int EntryLen2, const void *Entry2,
                                   const Attribute &attribute) {
        int mineInt,givenInt;
        float mineReal,givenReal;
        std::string mineVar,givenVar;
        int mineVarLen=0,givenVarLen = 0;
        RID mineRid,givenRid;
        switch(attribute.type){
            case TypeVarChar:
                if(EntryLabel == 1){
                    mineVarLen=EntryLen-sizeof(unsigned)*4;
                    mineVar=std::string((char*)Entry+2*sizeof(unsigned),mineVarLen);
                    memcpy(&mineRid.pageNum,(char*)Entry+mineVarLen+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+3*sizeof(unsigned)+mineVarLen,sizeof(unsigned));
                }
                else{
                    mineVarLen=EntryLen-sizeof(unsigned)*2;
                    mineVar=std::string((char*)Entry,mineVarLen);
                    memcpy(&mineRid.pageNum,(char*)Entry+mineVarLen,sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+sizeof(unsigned)+mineVarLen,sizeof(unsigned));

                }
                if(EntryLabel2 == 1){
                    givenVarLen=EntryLen2-sizeof(unsigned)*4;
                    givenVar=std::string((char*)Entry2+2*sizeof(unsigned),givenVarLen);
                    memcpy(&givenRid.pageNum,(char*)Entry2+givenVarLen+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+givenVarLen+3*sizeof(unsigned),sizeof(unsigned));
                }
                else{
                    givenVarLen=EntryLen2-sizeof(unsigned)*2;
                    givenVar=std::string((char*)Entry2,givenVarLen);
                    memcpy(&givenRid.pageNum,(char*)Entry2+givenVarLen,sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+sizeof(unsigned)+givenVarLen,sizeof(unsigned));
                }
                if(mineVar > givenVar)return 1;
                else if(mineVar < givenVar)return -1;
                else return compareRid(mineRid,givenRid);
                break;

            case TypeReal:
                if(EntryLabel == 1){
                    memcpy(&mineReal,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));\
                    memcpy(&mineRid.pageNum,(char*)Entry+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+4*sizeof(unsigned),sizeof(unsigned));
                }
                else{
                    memcpy(&mineReal,(char*)Entry,sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(EntryLabel2 == 1){
                    memcpy(&givenReal,(char*)Entry2+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.pageNum,(char*)Entry2+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+4*sizeof(unsigned),sizeof(unsigned));
                }
                else{
                    memcpy(&givenReal,(char*)Entry2,sizeof(unsigned));
                    memcpy(&givenRid.pageNum,(char*)Entry2+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(mineReal > givenReal)return 1;
                else if(mineReal < givenReal)return -1;
                else return compareRid(mineRid,givenRid);
                break;
            case TypeInt:
                if(EntryLabel == 1){
                    memcpy(&mineInt,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+4*sizeof(unsigned),sizeof(unsigned));
                }else{
                    memcpy(&mineInt,(char*)Entry,sizeof(unsigned));
                    memcpy(&mineRid.pageNum,(char*)Entry+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&mineRid.slotNum,(char*)Entry+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(EntryLabel2 == 1){
                    memcpy(&givenInt,(char*)Entry2+2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.pageNum,(char*)Entry2+3*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+4*sizeof(unsigned),sizeof(unsigned));
                }else{
                    memcpy(&givenInt,(char*)Entry2,sizeof(unsigned));
                    memcpy(&givenRid.pageNum,(char*)Entry2+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&givenRid.slotNum,(char*)Entry2+2*sizeof(unsigned),sizeof(unsigned));
                }
                if(mineInt > givenInt)return 1;
                else if(mineInt < givenInt)return -1;
                else {
                    return compareRid(mineRid,givenRid);
                }
                break;
        }

    }

    RC IndexManager::CompactEntry(IXFileHandle &ixFileHandle, int Label,void *buffer, void *Entry, int EntryLen,
                                  const Attribute &attribute, int & flag) {
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        int freeSpace;
        memcpy(&freeSpace,(char*)buffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        int find=0;
        for(int i=1;i<=slotNum;i++){
            int tempOff,tempLen;
            memcpy(&tempOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-2*i*sizeof(unsigned),sizeof(unsigned));
            memcpy(&tempLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
            void* tempEntry = malloc(PAGE_SIZE);
            memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
            int result= compareEntry(Label,tempLen,tempEntry,Label,EntryLen,Entry,attribute);
            free(tempEntry);
            if(result==0){
                find = i;
                break;
            }
        }
        if(find == 0)return -1;
        int deleteOff,deleteLen;
        memcpy(&deleteOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*find)*sizeof(unsigned),sizeof(unsigned));
        memcpy(&deleteLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*find-1)*sizeof(unsigned),sizeof(unsigned));
        int NewFreeSpace = freeSpace + deleteLen + 2*sizeof(unsigned);
        int NewSlotNum= slotNum-1;
        if(find != slotNum){
            int start, end,lastOff,lastLen;
            memcpy(&start,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(find+1)*sizeof(unsigned)),sizeof(unsigned));
            memcpy(&lastOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&lastLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*slotNum-1)*sizeof(unsigned),sizeof(unsigned));
            end=lastLen+lastOff;
            void * tempbuffer= malloc(PAGE_SIZE);
            memcpy(tempbuffer,(char*)buffer+start,end-start);
            memcpy((char*)buffer+deleteOff,tempbuffer,end-start);
            free(tempbuffer);
            for(int i= find+1;i<=slotNum;i++){
                int tempOff,tempLen;
                memcpy(&tempOff,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i)*sizeof(unsigned),sizeof(unsigned));
                memcpy(&tempLen,(char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
                int newTempOff=tempOff-deleteLen;
                memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(i-1))*sizeof(unsigned),&newTempOff,sizeof(unsigned));
                memcpy((char*)buffer+PAGE_SIZE-(Label==0?leafHeaderSize:nonleafHeaderSize)*sizeof(unsigned)-(2*(i-1)-1)*sizeof(unsigned),&tempLen,sizeof(unsigned));
            }

        }
        memcpy((char*)buffer+PAGE_SIZE-3*sizeof(unsigned),&NewSlotNum,sizeof(unsigned));
        memcpy((char*)buffer+PAGE_SIZE-sizeof(unsigned),&NewFreeSpace,sizeof(unsigned));
        if(NewSlotNum==0){
            flag=1;
        }
        return 0;
    }

    void IndexManager::debugBufferFind(void *buffer, void *Entry, int EntryLen, const Attribute &attribute) {
        int slotnum;
        memcpy(&slotnum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        if(slotnum==0)return;
        void * tempEntry =malloc(PAGE_SIZE);
        int StartOff,StartLen,LastOff,LastLen;
        memcpy(&StartOff,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-1*2*sizeof(unsigned),sizeof(unsigned));
        memcpy(&StartLen,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-(1*2-1)*sizeof(unsigned),sizeof(unsigned));
        memcpy(&LastOff,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-slotnum*2*sizeof(unsigned),sizeof(unsigned));
        memcpy(&LastLen,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-(slotnum*2-1)*sizeof(unsigned),sizeof(unsigned));
        if(attribute.type==TypeInt){
            int entry_key;
            memcpy(&entry_key,Entry,sizeof(unsigned));
            memcpy(tempEntry,(char*)buffer+StartOff,StartLen);
            int last_key,start_key;
            memcpy(&start_key,tempEntry,sizeof(unsigned));
            memcpy(tempEntry,(char*)buffer+LastOff,LastLen);
            memcpy(&last_key,tempEntry,sizeof(unsigned));
            std::cout<<"entry_key: "<<entry_key<<" start->last: "<<start_key<<" "<<last_key<<std::endl;
        }
        else if(attribute.type == TypeReal){
            float entry_key;
            memcpy(&entry_key,Entry,sizeof(unsigned));
            memcpy(tempEntry,(char*)buffer+StartOff,StartLen);
            float last_key,start_key;
            memcpy(&start_key,tempEntry,sizeof(unsigned));
            memcpy(tempEntry,(char*)buffer+LastOff,LastLen);
            memcpy(&last_key,tempEntry,sizeof(unsigned));
            std::cout<<"entry_key: "<<entry_key<<" start->last: "<<start_key<<" "<<last_key<<std::endl;
        }
        free(tempEntry);
    }

    template<typename T>
    int IndexManager::findFirstSlot(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key,
                                    int startNode,bool inclusive) {
        void * buffer= malloc(PAGE_SIZE);
        ixFileHandle.readPage(startNode,buffer);
        int slotNum;
        memcpy(&slotNum,(char*)buffer+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        for(int i=1;i<=slotNum;i++){
            int tempOff,tempLen;
            memcpy(&tempOff,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-2*i*sizeof(unsigned),sizeof(unsigned));
            memcpy(&tempLen,(char*)buffer+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-(2*i-1)*sizeof(unsigned),sizeof(unsigned));
            void * tempEntry= malloc (PAGE_SIZE);
            memcpy(tempEntry,(char*)buffer+tempOff,tempLen);
            T Real1,Real2;
            std::string Var1,Var2;
            switch (attribute.type){
                case TypeVarChar:
                    Var1=std::string((char*)tempEntry,tempLen-2*sizeof(unsigned));
                    int strLen;
                    memcpy(&strLen,(char*)key,sizeof(unsigned));
                    Var2=std::string((char*)key+sizeof(unsigned),strLen);
                    if(inclusive==true){
                        if(Var1==Var2){
                            free(tempEntry);
                            free(buffer);
                            return i;
                        }
                    }
                    else{
                        if(Var1>Var2){
                            free(tempEntry);
                            free(buffer);
                            return i;
                        }
                    }
                break;
                case TypeInt:
                case TypeReal:
                    memcpy(&Real1,(char*)tempEntry,sizeof(unsigned));
                    memcpy(&Real2,(char*)key,sizeof(unsigned));

                    if(inclusive==true){
                        if(Real1==Real2){
                            free(tempEntry);
                            free(buffer);
                            return i;
                        }
                    }
                    else{
                        if(Real1>Real2){
                            free(tempEntry);
                            free(buffer);
                            return i;
                        }
                    }
                break;
            }
            free(tempEntry);
        }
        free(buffer);
        return -1;
    }

    template<typename T>
    void IndexManager::collectRID(IXFileHandle &ixFileHandle, void *buffer, int id, void *Entry, int EntryLen,
                                   const Attribute &attribute, std::map<T, std::vector<RID>> &key,
                                   std::map<std::string, std::vector<RID>> &keyVar) {
        int flag=0;
        void * buffer2=malloc(PAGE_SIZE);
        memcpy(buffer2,buffer,PAGE_SIZE);
        int slotNum;
        memcpy(&slotNum,(char*)buffer2+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
        int pointer = id;
        while(1){
            if(flag==1)break;
            if(pointer>slotNum){
                int right;
                memcpy(&right,(char*)buffer2+PAGE_SIZE-6*sizeof(unsigned),sizeof(unsigned));
                if(right==-1)break;
                ixFileHandle.readPage(right,buffer2);
                memcpy(&slotNum,(char*)buffer2+PAGE_SIZE-3*sizeof(unsigned),sizeof(unsigned));
                pointer=1;
            }
            else{
                int tempOff,tempLen;
                memcpy(&tempOff,(char*)buffer2+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-2*pointer*sizeof(unsigned),sizeof(unsigned));
                memcpy(&tempLen,(char*)buffer2+PAGE_SIZE-leafHeaderSize*sizeof(unsigned)-(2*pointer-1)*sizeof(unsigned),sizeof(unsigned));
                void* tempEntry=malloc(PAGE_SIZE);
                memcpy(tempEntry,(char*)buffer2+tempOff,tempLen);
                T tempKey;
                std::string tempVar;
                RID tempRID;
                if(attribute.type!=TypeVarChar){
                    memcpy(&tempKey,(char*)tempEntry,sizeof(unsigned));
                    memcpy(&tempRID.pageNum,(char*)tempEntry+sizeof(unsigned),sizeof(unsigned));
                    memcpy(&tempRID.slotNum,(char*)tempEntry+2*sizeof(unsigned),sizeof(unsigned));
                    T EntryKey;
                    memcpy(&EntryKey,Entry,sizeof(unsigned));
                    if(EntryKey==tempKey){
                        key[tempKey].push_back(tempRID);
                    }
                    else{
                        flag=1;
                    }
                }
                else{
                    tempVar=std::string((char*)tempEntry,tempLen-2*sizeof(unsigned));
                    memcpy(&tempRID.pageNum,(char*)tempEntry+tempLen-2*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&tempRID.slotNum,(char*)tempEntry+tempLen-sizeof(unsigned),sizeof(unsigned));
                    std::string EntryVar=std::string((char*)Entry,EntryLen-2*sizeof(unsigned));
                    if(EntryVar==tempVar){
                        keyVar[tempVar].push_back(tempRID);
                    }
                    else{
                        flag=1;
                    }
                }
                pointer++;
                free(tempEntry);
            }
        }
        free(buffer2);
    }


} // namespace PeterDB