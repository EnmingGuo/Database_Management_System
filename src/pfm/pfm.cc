#include "src/include/pfm.h"
#include<cstdio>
namespace PeterDB {
    PagedFileManager *PagedFileManager::_pf_manager = nullptr;
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() {
        delete _pf_manager;
    }

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        //first, check whether the file is existed?
        FILE* fl=fopen(fileName.c_str(),"r"); //r+ means the file must exist!
        if(fl){            // the requested file is already existed!
            fclose(fl);
            return -1;           // return -1 means the opearation is failed.
        }
        fl = fopen(fileName.c_str(),"wb");  //wb means if the file is not existed, it will automatically create one binary file.
        //create two hidden page to store the counters.
        void* data = malloc(PAGE_SIZE); //create a block with PAGE_SIZE
        memset(data,0,sizeof(unsigned char)*PAGE_SIZE);
        unsigned occupyField=3;
        unsigned temp=PAGE_SIZE-occupyField*sizeof(unsigned);
        memcpy((char*)data+(PAGE_SIZE-sizeof(unsigned)),&temp,sizeof(unsigned));
        fseek(fl,0,SEEK_SET);
        fwrite(data, sizeof(unsigned char), PAGE_SIZE,fl);
        fclose(fl);
        free(data);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        //first, check whether the file is existed?
        FILE * fl=fopen(fileName.c_str(),"r");
        if(fl==NULL){ //file doesn't exist.
            return -1;
        }
        if(remove(fileName.c_str())==0){ //The remove function will return 0 when the file is removed successfully!
            return 0;
        }
        else return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        //first check the file is existed?
        FILE* fl=fopen(fileName.c_str(),"r+b");
        if(fl==NULL){ //the file is not existed!
            return -1;
        }
        //second check the fileHandle is NULL
        if(fileHandle.fl!=NULL){
            return -1;
        }
        fileHandle.fl=fl;
        //move the file pointer to the start of the file.
        fseek(fileHandle.fl,0,SEEK_SET);
        for(int i = 0; i < 4; i++){
            unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
            fseek(fileHandle.fl, i * 4, SEEK_SET);
            fread(buffer, sizeof(unsigned char), sizeof(unsigned), fileHandle.fl);
            if(i==0){
                //for readPageCounter
                memcpy(&fileHandle.readPageCounter,buffer, sizeof(unsigned));
                //std::cout<<"I have finished the readPageCounter "<<*buffer<<std::endl;
            }
            else if(i==1){
                //for writePageCounter
                memcpy(&fileHandle.writePageCounter,buffer, sizeof(unsigned));
                //std::cout<<"I have finished the writePageCounter "<<*buffer<<std::endl;
            }
            else if(i==2){
                //for appendPageCounter
                memcpy(&fileHandle.appendPageCounter,buffer, sizeof(unsigned));
                //std::cout<<"I have finished the appendPageCounter "<<*buffer<<std::endl;
            }
            else{
                //for pageNum
                memcpy(&fileHandle.pageNumber,buffer, sizeof(unsigned));
                //std::cout<<"I have finished the pageNumber "<<*buffer<<std::endl;
            }
            free(buffer);
        }
        fseek(fileHandle.fl,0,SEEK_SET);
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        //we need to check whether the fileHandle is proper!
        if(fileHandle.fl==NULL){
            return -1;
        }
        fseek(fileHandle.fl,0,SEEK_SET);
        for(int i = 0; i < 4; i++){
            unsigned char* buffer = (unsigned char*)malloc(sizeof(unsigned));
            if(i==0){
                //for readPageCounter
                memcpy(buffer, &fileHandle.readPageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back readPageCounter "<<*buffer<<std::endl;
            }
            else if(i==1){
                //for writePageCounter
                memcpy(buffer, &fileHandle.writePageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back writePageCounter "<<*buffer<<std::endl;
            }
            else if(i==2){
                //for appendPageCounter
                memcpy(buffer, &fileHandle.appendPageCounter,sizeof(unsigned));
                //std::cout<<"I have finished the back appendPageCounter "<<*buffer<<std::endl;
            }
            else{
                //for pageNum
                memcpy(buffer,&fileHandle.pageNumber, sizeof(unsigned));
                //std::cout<<"I have finished the back pageNumber "<<*buffer<<std::endl;
            }
            fseek(fileHandle.fl,4 * i, SEEK_SET);
            fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), fileHandle.fl);
            free(buffer);
        }
        fseek(fileHandle.fl,0,SEEK_SET);
        fclose(fileHandle.fl);
        fileHandle.fl=NULL;
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageNumber = 0;
    }

    FileHandle::~FileHandle() {
        if(fl)
            fclose(fl);
        fl = NULL;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        //first check the fl
        if(fl == NULL){
            return -1;
        }
        //second check whether the pageNum is proper.
        if(pageNum>=pageNumber){//pageNum start with 0; while pageNumber is a counter.
            return -1;
        }
        fseek(fl, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        unsigned bufferNum=fread(data,sizeof(unsigned char),PAGE_SIZE,fl);
        if(bufferNum != PAGE_SIZE){
            return -1;
        }
        readPageCounter=readPageCounter+1;
        fseek(fl,0,SEEK_SET);
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(fl==NULL){
            return -1;
        }
        if(pageNum>=pageNumber){
            return -1;
        }
        fseek(fl, (1 + pageNum) * PAGE_SIZE, SEEK_SET);
        unsigned bufferNum= fwrite(data,sizeof(unsigned char),PAGE_SIZE,fl);
        if(bufferNum!=PAGE_SIZE){
            return -1;
        }
        writePageCounter= writePageCounter+1;
        fseek(fl,0,SEEK_SET);
        return 0;
    }
    RC FileHandle::findAvailable(unsigned field){
        if(pageNumber==0)return -1;
        else{
            void* buffer=malloc(PAGE_SIZE);
            readPage(pageNumber-1,buffer);
            unsigned availField;
            memcpy(&availField,(char*)buffer+PAGE_SIZE-4,sizeof(unsigned));
            if(availField>=field){
                free(buffer);
                return pageNumber-1;
            }
            free(buffer);
            return -1;
        }
    }
    RC FileHandle::appendPage(const void *data) {
        if(fl==NULL){
            return -1;
        }
        if(fseek(fl,((pageNumber + 1) * PAGE_SIZE), SEEK_SET)!=0){//fseek error
            return -1;
        }
        unsigned bufferNum= fwrite(data,sizeof(unsigned char),PAGE_SIZE,fl);
        if(bufferNum!=PAGE_SIZE){
            return -1;
        }
        pageNumber=pageNumber+1;
        appendPageCounter= appendPageCounter+1;
        fseek(fl, 0, SEEK_SET);
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        if(fl==NULL)
            return -1;
        return pageNumber;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        if(fl == NULL){//fl is null
            return -1;
        }
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }
} // namespace PeterDB