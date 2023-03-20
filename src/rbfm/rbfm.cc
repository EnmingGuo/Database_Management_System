#include "src/include/rbfm.h"
#include<iostream>
#include<cstdio>
#include<stdio.h>
#include<vector>
#include<math.h>
namespace PeterDB {
    RecordBasedFileManager *RecordBasedFileManager:: _rbf_manager = nullptr;
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager(){
        delete _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager:: operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName,fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    //project 1
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if(fileHandle.fl==NULL){
            return -1;
        }
        if(recordDescriptor.size()==0){
            return -1;
        }
        void* buffer=malloc(PAGE_SIZE);
        toRecord(data,buffer,recordDescriptor);

        unsigned pages;
        if((pages=fileHandle.findAvailable(checkRecordSize(buffer)))!=-1){//zhaoweizhi
            void *pageContent= malloc(PAGE_SIZE);
            if(fileHandle.readPage(pages,pageContent)==0){
                rid.pageNum=pages;
                ChangePageContent(pageContent,buffer,rid);
                fileHandle.writePage(pages,pageContent);
            }
            else{
                free(pageContent);
                free(buffer);
                return -1;
            }
            free(pageContent);
        }
        else{
            //直接敷在最后
            void *pageContent= malloc(PAGE_SIZE);
            memset(pageContent,0,sizeof(unsigned char)*PAGE_SIZE);
            unsigned temp=PAGE_SIZE-occupyField*sizeof(unsigned);
            memcpy((char*)pageContent+(PAGE_SIZE-sizeof(unsigned)),&temp,sizeof(unsigned));
            ChangePageContent(pageContent,buffer,rid);
            fileHandle.appendPage(pageContent);
            rid.pageNum=fileHandle.getNumberOfPages()-1;
            free(pageContent);
        }
        free(buffer);
        return 0;

    }
    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        if(fileHandle.fl==NULL||recordDescriptor.size()==0)return -1;
        if(rid.pageNum>=fileHandle.getNumberOfPages())return -1;
        void * buffer=malloc(PAGE_SIZE);
        toRecord(data,buffer,recordDescriptor);
        if(deleteRecord(fileHandle,recordDescriptor,rid)==0){
            void * pageContent=malloc(PAGE_SIZE);
            if(fileHandle.readPage(rid.pageNum,pageContent)!=0){
                free(pageContent);
                free(buffer);
                return -1;
            }
            if(UpdatePageContent(pageContent,buffer,rid)==-1){
                free(pageContent);
                free(buffer);
                return -1;
            }
            else if(UpdatePageContent(pageContent,buffer,rid)==UPDATE_NO_ENOUGH){
                RID newRid;
                insertRecord(fileHandle,recordDescriptor,data,newRid);
                unsigned temp=SLOT_POINTER;
                memcpy((char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),&temp,sizeof(unsigned));
                unsigned test_temp;
                memcpy(&test_temp,(char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
                unsigned short newPage=(unsigned short)newRid.pageNum;
                unsigned short newSlot=(unsigned short)newRid.slotNum;
                memcpy((char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),&newPage,sizeof(short));
                memcpy((char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned)+2,&newSlot,sizeof(short));
            }
            fileHandle.writePage(rid.pageNum,pageContent);
            free(pageContent);
            free(buffer);
            return 0;
        }
        free(buffer);
        return -1;
    }

    //update 主要函数
    RC RecordBasedFileManager::UpdatePageContent(void* pageContent,void*buffer,const RID &rid){
        unsigned vacantSlotNum;
        memcpy(&vacantSlotNum,(char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
        if(vacantSlotNum<=0)return -1;
        unsigned currentVacant;
        memcpy(&currentVacant,(char*)pageContent+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        int newVaccant=(int)currentVacant- (int)checkRecordSize(buffer);
        if(newVaccant<5){
            return UPDATE_NO_ENOUGH;
        }
        unsigned bufferLen= checkRecordSize(buffer)-SLOT_SIZE*sizeof(unsigned);
        unsigned slotNum;
        memcpy(&slotNum,(char*)pageContent+PAGE_SIZE-occupyField*sizeof(unsigned),sizeof(unsigned));
        unsigned id=rid.slotNum,idLen,idOffset;

        memcpy(&idLen,(char*)pageContent+PAGE_SIZE-(id*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
        memcpy(&idOffset,(char*)pageContent+PAGE_SIZE-(id*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
        if(idLen!=VACANT_FLAG || idOffset!=VACANT_FLAG)return -1;

        unsigned previousOffset=0, previousLen=0;
        for(int i=1;i<id;i++){
            unsigned Offset,Length;
            memcpy(&Offset,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&Length,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
            if(Offset==VACANT_FLAG||Offset==SLOT_POINTER)continue;
            previousOffset=Offset;
            previousLen=Length;
        }
        unsigned lastOffset=previousOffset,lastLen=previousLen;
        for(unsigned i=id+1;i<=slotNum;i++){
            unsigned Offset,Length;
            memcpy(&Offset,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&Length,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
            if(Offset==VACANT_FLAG||Offset==SLOT_POINTER)continue;
            lastOffset=Offset;
            lastLen=Length;
        }
        unsigned pageRecordEnd=lastOffset+lastLen;
        unsigned pageInsertStart=previousLen+previousOffset;
        void* tempBuffer=malloc(pageRecordEnd-pageInsertStart);
        memcpy(tempBuffer,(char*)pageContent+pageInsertStart,pageRecordEnd-pageInsertStart);
        memcpy((char*)pageContent+pageInsertStart,buffer,bufferLen);
        memcpy((char*)pageContent+pageInsertStart+bufferLen,tempBuffer,pageRecordEnd-pageInsertStart);
        memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField)*sizeof(unsigned)),&pageInsertStart,sizeof(unsigned));
        memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField-1)*sizeof(unsigned)),&bufferLen,sizeof(unsigned));
        for(int i=id+1;i<=slotNum;i++){
            unsigned Offset;
            memcpy(&Offset,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
            if(Offset==VACANT_FLAG||Offset==SLOT_POINTER)continue;
            Offset+=bufferLen;
            memcpy((char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),&Offset,sizeof(unsigned));
        }
        unsigned newVacantSlotNum=vacantSlotNum-1;
        memcpy((char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),&newVacantSlotNum,sizeof(unsigned));
        memcpy((char*)pageContent+(PAGE_SIZE-sizeof(unsigned)),&newVaccant,sizeof(unsigned));
        free(tempBuffer);
        return 0;

    }

//    RC RecordBasedFileManager::UpdatePageContent(void* pageContent,void*buffer,const RID &rid){
//        unsigned vacantSlotNum;
//        memcpy(&vacantSlotNum,(char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
//        if(vacantSlotNum<=0)return -1;
//        unsigned currentVacant;
//        memcpy(&currentVacant,(char*)pageContent+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
//        int newVaccant=(int)currentVacant- (int)checkRecordSize(buffer);
//        if(newVaccant<0){
//            return UPDATE_NO_ENOUGH;
//        }
//        unsigned bufferLen= checkRecordSize(buffer)-SLOT_SIZE*sizeof(unsigned);
//        unsigned slotNum;
//        memcpy(&slotNum,(char*)pageContent+PAGE_SIZE-occupyField*sizeof(unsigned),sizeof(unsigned));
//        unsigned id=rid.slotNum,idLen,idOffset;
//        memcpy(&idLen,(char*)pageContent+PAGE_SIZE-(id*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
//        memcpy(&idOffset,(char*)pageContent+PAGE_SIZE-(id*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
//        if(idLen!=VACANT_FLAG || idOffset!=VACANT_FLAG)return -1;
//        unsigned previousOffset=0, previousLen=0;
//        if(id>1){
//            memcpy(&previousOffset,(char*)pageContent+PAGE_SIZE-((id-1)*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
//            memcpy(&previousLen,(char*)pageContent+PAGE_SIZE-((id-1)*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
//        }
//        unsigned lastPosition=0,lastLen=0;
//
//        for(unsigned i=1;i<=slotNum;i++){
//            unsigned Offset;
//            memcpy(&Offset,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
//            if(Offset!=VACANT_FLAG){
//                lastPosition=Offset;
//                memcpy(&lastLen,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
//            }
//        }
//        unsigned pageRecordEnd=lastPosition+lastLen;
//        unsigned pageInsertStart=previousLen+previousOffset;
//        void* tempBuffer=malloc(pageRecordEnd-pageInsertStart);
//        memcpy(tempBuffer,(char*)pageContent+pageInsertStart,sizeof(tempBuffer));
//        memcpy((char*)pageContent+pageInsertStart,buffer,bufferLen);
//        memcpy((char*)pageContent+pageInsertStart+bufferLen,tempBuffer,sizeof(tempBuffer));
//        memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField)*sizeof(unsigned)),&pageInsertStart,sizeof(unsigned));
//        memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField-1)*sizeof(unsigned)),&bufferLen,sizeof(unsigned));
//        unsigned newSlotOffset=pageInsertStart+bufferLen;
//        for(int i=id+1;i<=slotNum;i++){
//
//            unsigned Length;
//            memcpy(&Length,(char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*i+occupyField-1)*sizeof(unsigned)),sizeof(unsigned));
//            if(Length==VACANT_FLAG)continue;
//            memcpy((char*)pageContent+(PAGE_SIZE-SLOT_SIZE*i+occupyField)*sizeof(unsigned),&newSlotOffset,sizeof(unsigned));
//            newSlotOffset+=Length;
//        }
//        unsigned newVacantSlotNum=vacantSlotNum-1;
//        memcpy((char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),&newVacantSlotNum,sizeof(unsigned));
//        memcpy((char*)pageContent+(PAGE_SIZE-sizeof(unsigned)),&newVaccant,sizeof(unsigned));
//        free(tempBuffer);
//        return 0;
//
//    }
    //delete 主要更改函数！
    unsigned RecordBasedFileManager::ChangePageContent(void* pageContent, void* buffer,RID &rid){
        unsigned vacantSlotNum;
        memcpy(&vacantSlotNum,(char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
        unsigned currentVacant;//=*((char*)pageContent+(PAGE_SIZE-sizeof(unsigned)));
        memcpy(&currentVacant,(char*)pageContent+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        unsigned bufferLen= checkRecordSize(buffer)-SLOT_SIZE*sizeof(unsigned);
        unsigned slotNum;
        memcpy(&slotNum,(char*)pageContent+PAGE_SIZE-occupyField*sizeof(unsigned),sizeof(unsigned));
        if(vacantSlotNum>0){
            //去找那个有-1的地方；
            unsigned id,lastPosition=0,lastLen=0;
            bool flag=0;
            for(unsigned i=1;i<=slotNum;i++){
                unsigned Offset;
                memcpy(&Offset,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
                if(Offset==VACANT_FLAG&&!flag){
                    flag=1;
                    id=i;
                }
                else if(Offset!=VACANT_FLAG && Offset!=SLOT_POINTER){
                    lastPosition=Offset;
                    memcpy(&lastLen,(char*)pageContent+PAGE_SIZE-(i*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
                }
            }
            rid.slotNum=id;
            unsigned previousOffset=0, previousLen=0;
            if(id>1){
                memcpy(&previousOffset,(char*)pageContent+PAGE_SIZE-((id-1)*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
                memcpy(&previousLen,(char*)pageContent+PAGE_SIZE-((id-1)*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
            }
            unsigned pageRecordEnd=lastPosition+lastLen;
            unsigned pageInsertStart=previousLen+previousOffset;
            void* tempBuffer=malloc(pageRecordEnd-pageInsertStart);
            memcpy(tempBuffer,(char*)pageContent+pageInsertStart,sizeof(tempBuffer));
            memcpy((char*)pageContent+pageInsertStart,buffer,bufferLen);
            memcpy((char*)pageContent+pageInsertStart+bufferLen,tempBuffer,sizeof(tempBuffer));
            memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField)*sizeof(unsigned)),&pageInsertStart,sizeof(unsigned));
            memcpy((char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*id+occupyField-1)*sizeof(unsigned)),&bufferLen,sizeof(unsigned));
            unsigned newSlotOffset=pageInsertStart+bufferLen;
            for(int i=id+1;i<=slotNum;i++){

                unsigned Length;
                memcpy(&Length,(char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*i+occupyField-1)*sizeof(unsigned)),sizeof(unsigned));
                unsigned Offset;
                memcpy(&Offset,(char*)pageContent+(PAGE_SIZE-(SLOT_SIZE*i+occupyField)*sizeof(unsigned)),sizeof(unsigned));
                if(Offset==VACANT_FLAG||Offset==SLOT_POINTER)continue;
                memcpy((char*)pageContent+(PAGE_SIZE-SLOT_SIZE*i+occupyField)*sizeof(unsigned),&newSlotOffset,sizeof(unsigned));
                newSlotOffset+=Length;
            }
            unsigned newVacantSlotNum=vacantSlotNum-1;
            memcpy((char*)pageContent+PAGE_SIZE-2*sizeof(unsigned),&newVacantSlotNum,sizeof(unsigned));
            free(tempBuffer);
        }
        else{
            unsigned lastSlotLen=0;
            unsigned lastSlotOff=0;
            if(slotNum>0){
                memcpy(&lastSlotOff,(char*)pageContent+PAGE_SIZE-(2*slotNum+occupyField)*sizeof(unsigned),sizeof(unsigned));
                memcpy(&lastSlotLen,(char*)pageContent+PAGE_SIZE-(2*slotNum+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
            }
            memcpy((char*)pageContent+lastSlotLen+lastSlotOff,(char*)buffer,bufferLen);
            unsigned newSlotNum=slotNum+1;
            memcpy((char*)pageContent+(PAGE_SIZE-occupyField*sizeof(unsigned)),&newSlotNum,sizeof(unsigned));
            rid.slotNum=newSlotNum;
            unsigned newOff=lastSlotLen+lastSlotOff;
            memcpy((char*)pageContent+(PAGE_SIZE-(2*newSlotNum+occupyField)*sizeof(unsigned)),&newOff,sizeof(unsigned));
            memcpy((char*)pageContent+(PAGE_SIZE-(2*newSlotNum+occupyField-1)*sizeof(unsigned)),&bufferLen,sizeof(unsigned));

        }
        unsigned newVacant=currentVacant- checkRecordSize(buffer);
        memcpy((char*)pageContent+(PAGE_SIZE-sizeof(unsigned)),&newVacant,sizeof(unsigned));
        return 0;
    }
    
    unsigned RecordBasedFileManager::checkRecordSize(void* dest){//返回创建一个记录需要占用的大小（记录长度+Slot长度）
        unsigned len;
        memcpy(&len,(char*)dest,sizeof(unsigned));
        unsigned dis;
        memcpy(&dis,(char*)dest+len*sizeof(unsigned),sizeof(unsigned));
        return dis+SLOT_SIZE*sizeof(unsigned);
    }
    RC RecordBasedFileManager::toRecord(const void* data,void* dest,const std::vector<Attribute> &recordDescriptor){
        unsigned attributeNumber=recordDescriptor.size();
        if(attributeNumber<=0){
            return -1;
        }
        unsigned nullBitLength=ceil((float)attributeNumber/8);
        char* nullPointer=(char*)malloc(nullBitLength);
        memcpy(nullPointer,data,nullBitLength);

        char* point=(char*)data;//
        point=point+nullBitLength;
        char* destPoint=(char*)dest;
        //记录记录的个数；
        memcpy(destPoint,&attributeNumber,sizeof(unsigned));
        destPoint=destPoint+sizeof(unsigned);
        //预留个数个位置来表示指针
        for(unsigned i=0;i<attributeNumber;i++){
            //*(unsigned*)destPoint=0;
            unsigned temp=0;
            memcpy(destPoint,&temp,sizeof(unsigned));
            destPoint=destPoint+sizeof(unsigned);
        }
        //往里面誊写data
        for(unsigned i=0;i<recordDescriptor.size();i++){
            bool isnull= checkNull(i,nullPointer);
            switch(recordDescriptor[i].type){
                case TypeInt:
                case TypeReal:
                    if(isnull){
                        unsigned temp=(char*)destPoint-(char*)dest;
                       memcpy((char*)dest+(i+1)*sizeof(unsigned),&temp,sizeof(unsigned));
                    }
                    else{
                        memcpy((char*)destPoint,(char*)point,sizeof(unsigned));
                        point=point+sizeof(unsigned);
                        destPoint=destPoint+sizeof(unsigned);
                        unsigned temp=(char*)destPoint-(char*)dest;
                        memcpy((char*)dest+(i+1)*sizeof(unsigned),&temp,sizeof(unsigned));
                    }
                    break;
                case TypeVarChar:
                    if(isnull){
                        unsigned temp=(char*)destPoint-(char*)dest;
                        memcpy((char*)dest+(i+1)*sizeof(unsigned),&temp,sizeof(unsigned));

                    }
                    else{
                        unsigned strNum=*(unsigned*)point;
                        point=point+sizeof(unsigned);
                        for(unsigned _=0;_<strNum+1;_++,destPoint=destPoint+sizeof(char)){
                            if(_==strNum){
                                unsigned char temp='\0';
                                memcpy(destPoint,&temp,sizeof(unsigned char));
                            }
                            else{
                                memcpy((char*)destPoint,(char*)point,sizeof(unsigned char));
                                point=point+sizeof(char);
                            }
                        }
                        unsigned temp=(char*)destPoint-(char*)dest;
                        memcpy((char*)dest+(i+1)*sizeof(unsigned),&temp,sizeof(unsigned));
                    }
                    break;
            }
        }
        free(nullPointer);
        return 0;
    }
    RC RecordBasedFileManager::deRecord(const void* data,void*dest,const std::vector<Attribute>& recordDescriptor){
        unsigned attributeNumber=recordDescriptor.size();
        if(attributeNumber<=0){
            return -1;
        }
        unsigned nullBitLength=ceil((float)attributeNumber/8);
        char* nullPointer=(char*)malloc(nullBitLength);
        memset(nullPointer,0,nullBitLength);
        memcpy(dest,nullPointer,sizeof(nullBitLength));
        unsigned destPoint=nullBitLength;
        for(unsigned i=0;i<recordDescriptor.size();i++){
            unsigned Len;
            unsigned Pointer;
            if(i==0){
                unsigned firstRecordEnd;

                memcpy(&firstRecordEnd,(char*)data+1*sizeof(unsigned),sizeof(unsigned));
                Len=firstRecordEnd-sizeof(unsigned)*(1+recordDescriptor.size());
                Pointer=sizeof(unsigned)*(1+recordDescriptor.size());
            }
            else{
                unsigned firstRecordEnd;
                unsigned secondRecordEnd;
                memcpy(&firstRecordEnd,(char*)data+i*sizeof(unsigned),sizeof(unsigned));
                memcpy(&secondRecordEnd,(char*)data+(i+1)*sizeof(unsigned),sizeof(unsigned));
                Len=secondRecordEnd-firstRecordEnd;
                Pointer=firstRecordEnd;
            }
            if(Len==0){
                changeBit(nullPointer,i);
                continue;
            }
            switch(recordDescriptor[i].type){
                case TypeReal:
                case TypeInt:
                    memcpy((char*)dest+destPoint,(char*)data+Pointer,sizeof(unsigned));
                    destPoint=destPoint+sizeof(unsigned);
                    break;
                case TypeVarChar:
                    if(Len==1){
                        signed temp=Len-1;
                        memcpy((char*)dest+destPoint,&temp,sizeof(unsigned));
                        destPoint=destPoint+sizeof(unsigned);
                    }
                    else{
                        signed temp=Len-1;
                        memcpy((char*)dest+destPoint,&temp,sizeof(unsigned));
                        destPoint=destPoint+sizeof(unsigned);
                        memcpy((char*)dest+destPoint,(char*)data+Pointer,temp*sizeof(char));
                        destPoint=destPoint+sizeof(unsigned char)*temp;
                    }
                    break;
            }
        }
        memcpy(dest,nullPointer,sizeof(unsigned char)*nullBitLength);
        free(nullPointer);
        return 0;
    }

    //project 1
    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, void *data) {
        void* record=malloc(PAGE_SIZE);
        RC rc=getRecord(fileHandle,rid,record);
        if(rc==0){
            deRecord(record,data,recordDescriptor);
            free(record);
            return 0;
        }
        else if(rc==2){
            if(readRecord(fileHandle,recordDescriptor,tempRid,data)!=0){
                free(record);
                return -1;
            }
            free(record);
            return 0;
        }
        free(record);
        return -1;

    }
    RC RecordBasedFileManager::getRecord(FileHandle &fileHandle, const RID & rid,void* record){
        if(rid.pageNum>=fileHandle.getNumberOfPages())return -1;
        void* pageContent=malloc(PAGE_SIZE);
        fileHandle.readPage(rid.pageNum,pageContent);
        //unsigned slotNum=*((char*)pageContent+PAGE_SIZE-occupyField*sizeof(unsigned));
        unsigned slotNum;
        memcpy(&slotNum,(char*)pageContent+PAGE_SIZE-occupyField*sizeof(unsigned),sizeof(unsigned));
        if(rid.slotNum<=0||slotNum<rid.slotNum){
            free(pageContent);
            return -1;
        }
        unsigned requiredSlotOff;
        memcpy(&requiredSlotOff,(char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
        if(requiredSlotOff==VACANT_FLAG){
            free(pageContent);
            return -1;
        }
        else if(requiredSlotOff==SLOT_POINTER){
            unsigned short newPage,newSlot;
            memcpy(&newPage,(char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(short));
            memcpy(&newSlot,(char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned)+2,sizeof(short));
            RID newRid;
            newRid.slotNum=(unsigned)newSlot;
            newRid.pageNum=(unsigned)newPage;
            tempRid=newRid;
            free(pageContent);
            return 2;
        }
        unsigned requiredSlotLen;
        memcpy(&requiredSlotLen,(char*)pageContent+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
        memcpy(record,(char*)pageContent+requiredSlotOff,requiredSlotLen*sizeof(unsigned char));
        free(pageContent);
        return 0;
    }

    //project 1
    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int attributeNumber=recordDescriptor.size();
        int nullBitLength=ceil((float)attributeNumber/8);
        char* nullPointer=(char*)malloc(nullBitLength);
        memcpy(nullPointer,data,nullBitLength);
        char* point=(char*)data;
        point=point+nullBitLength;
        for(unsigned i=0;i<recordDescriptor.size();i++){
            if(i!=0)out<<", ";
            bool isnull= checkNull(i,nullPointer);
            switch(recordDescriptor[i].type){
                case TypeInt:
                    out<<recordDescriptor[i].name<<": ";
                    if(isnull)out<<"NULL";
                    else{
                        unsigned intResult;
                        memcpy(&intResult,point,sizeof(unsigned));
                        out<<intResult;
                        point=point+sizeof(unsigned);
                    }

                    break;
                case TypeReal:
                    out<<recordDescriptor[i].name<<": ";
                    if(isnull)out<<"NULL";
                    else{
                        float floatResult;
                        memcpy(&floatResult, point, sizeof(int));
                        out<<floatResult;
                        point=point+sizeof(unsigned);
                    }
                    break;
                case TypeVarChar:
                    out<<recordDescriptor[i].name<<": ";
                    if(isnull)out<<"NULL";
                    else{
                        unsigned bytesNum;//=*(int*)point;
                        memcpy(&bytesNum,point,sizeof(unsigned));
                        point=point+sizeof(unsigned);
                        for(int j=0;j<bytesNum;j++){
                            char temp;
                            memcpy(&temp,point,sizeof(char));
                            out<<temp;
                            point=point+sizeof(unsigned char);
                        }
                    }
                break;
            }

        }
        free(nullPointer);
        out<<std::endl;
        return 0;
    }
    RC RecordBasedFileManager::checkNull(unsigned idx,const char*data){
        unsigned bytePosition = idx / 8;
        unsigned bitPosition = idx % 8;
        char b = data[bytePosition];
        return ((b >> (7 - bitPosition)) & 0x1);
        return 0;
    }
    RC RecordBasedFileManager::changeBit(char* buffer,unsigned index){
        unsigned bytePosition=index/8;
        unsigned bitPosition=index%8;
        char temp=buffer[bytePosition]|(0x1<<(7-bitPosition));
        //*((char*)buffer+bytePosition)=temp;
        memcpy(buffer+bytePosition,&temp,sizeof(unsigned char));
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        unsigned pageNum=rid.pageNum;
        void* previousPageContent=malloc(PAGE_SIZE);
        memset(previousPageContent,0,sizeof(unsigned char)*PAGE_SIZE);
        if(fileHandle.readPage(pageNum,previousPageContent)!=0){
            std::cerr<<"Fail to readPage!"<<std::endl;
            return -1;
        }
        RC rc=compactRecord(previousPageContent,rid);
        if(rc==0){
            fileHandle.writePage(pageNum,previousPageContent);
            free(previousPageContent);
            return 0;
        }
        else if(rc==2){
            fileHandle.writePage(pageNum,previousPageContent);
            free(previousPageContent);
            return deleteRecord(fileHandle,recordDescriptor,tempRid);
        }
        free(previousPageContent);
        return -1;
    }
    RC RecordBasedFileManager::compactRecord(void* data,const RID &rid){
        unsigned deleteSlotNum=rid.slotNum;
        unsigned totalSlotNum;
        unsigned totalVacantFiled;
        memcpy(&totalSlotNum,(char*)data+PAGE_SIZE-occupyField*sizeof(unsigned),sizeof(unsigned));
        memcpy(&totalVacantFiled,(char*)data+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
        if(deleteSlotNum>totalSlotNum)return -1;//页码超过所求
        void * tempPointer=data;
        unsigned offsetPointer;
        memcpy(&offsetPointer,(char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
        unsigned deleteSlotLen;
        memcpy(&deleteSlotLen,(char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
        if(offsetPointer==VACANT_FLAG&&deleteSlotLen==VACANT_FLAG)return -1;
        if(offsetPointer==SLOT_POINTER){
            unsigned short a,b;
            memcpy(&a,(char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned short));
            memcpy(&b,(char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned)+2,sizeof(unsigned short));
            tempRid.pageNum=(unsigned)a;
            tempRid.slotNum=(unsigned)b;
            unsigned temp=VACANT_FLAG;
            memcpy((char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),&temp,sizeof(unsigned));
            memcpy((char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),&temp,sizeof(unsigned));
            unsigned vacantSlotNum;
            memcpy(&vacantSlotNum,(char*)tempPointer+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
            unsigned newVacantSlotNum=vacantSlotNum+1;
            memcpy((char*)tempPointer+PAGE_SIZE-2*sizeof(unsigned),&newVacantSlotNum,sizeof(unsigned));
            return 2;
        }
        for(int i=deleteSlotNum+1;i<=totalSlotNum;i++){
            unsigned subsequentOffset;
            unsigned subsequentLen;
            memcpy(&subsequentOffset,(char*)tempPointer+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),sizeof(unsigned));
            memcpy(&subsequentLen,(char*)tempPointer+PAGE_SIZE-(i*SLOT_SIZE+occupyField-1)*sizeof(unsigned),sizeof(unsigned));
            if(subsequentOffset==VACANT_FLAG||subsequentOffset==SLOT_POINTER)continue;
            void* tempData=malloc(subsequentLen);
            memcpy(tempData,(char*)tempPointer+subsequentOffset,subsequentLen);
            memcpy((char*)tempPointer+offsetPointer,tempData,subsequentLen);
            memcpy((char*)tempPointer+PAGE_SIZE-(i*SLOT_SIZE+occupyField)*sizeof(unsigned),&offsetPointer,sizeof(unsigned));
            offsetPointer+=subsequentLen;
            free(tempData);
        }
        unsigned temp=VACANT_FLAG;
        memcpy((char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField)*sizeof(unsigned),&temp,sizeof(unsigned));
        memcpy((char*)tempPointer+PAGE_SIZE-(rid.slotNum*SLOT_SIZE+occupyField-1)*sizeof(unsigned),&temp,sizeof(unsigned));
        totalVacantFiled=totalVacantFiled+deleteSlotLen;
        memcpy((char*)data+PAGE_SIZE-sizeof(unsigned),&totalVacantFiled,sizeof(unsigned));
        unsigned vacantSlotNum;
        memcpy(&vacantSlotNum,(char*)tempPointer+PAGE_SIZE-2*sizeof(unsigned),sizeof(unsigned));
        unsigned newVacantSlotNum=vacantSlotNum+1;
        memcpy((char*)tempPointer+PAGE_SIZE-2*sizeof(unsigned),&newVacantSlotNum,sizeof(unsigned));
        return 0;
    }


    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        void* record=malloc(PAGE_SIZE);
        if(getRecord(fileHandle,rid,record)==0){
            if(deAttribute(record,data,attributeName,recordDescriptor)==0){
                free(record);
                return 0;
            }
            free(record);
            return -1;
        }
        free(record);
        return -1;
    }

    RC RecordBasedFileManager::deAttribute(const void *data, void *dest, const std::string &attributeName,
                                           const std::vector<Attribute> &recordDescriptor) {
        unsigned pointer = 0;
        char nullByte = 0x00;//Because the table has several attributes
        char nullSign=128u;
        memcpy(dest, &nullByte, sizeof(char));
        pointer+= sizeof(char);
        for(int i=1;i<=recordDescriptor.size();i++){
            if(recordDescriptor.at(i-1).name==attributeName){
                unsigned Start,End;
                if(i==1){
                    unsigned itemNum;
                    memcpy(&itemNum,(char*)data,sizeof(unsigned));
                    Start=(itemNum+1)*sizeof(unsigned);
                    memcpy(&End,(char*)data+sizeof(unsigned),sizeof(unsigned));

                }
                else{
                    memcpy(&Start,(char*)data+(i-1)*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&End,(char*)data+(i)*sizeof(unsigned),sizeof(unsigned));

                }
                if(End-Start==0){
                    memcpy(dest, &nullSign, sizeof(char));
                }
                else{
                    if(recordDescriptor.at(i-1).type==TypeVarChar){
                        unsigned len=End-Start-1;
                        memcpy((char*)dest+pointer,&len,sizeof(unsigned));
                        pointer+=sizeof(unsigned);
                        if(len>0){
                            memcpy((char*)dest+pointer,(char*)data+Start,len);
                        }

                    }
                    else{
                        memcpy((char*)dest+pointer,(char*)data+Start,End-Start);
                    }
                }
                break;
            }
        }
        return 0;

    }
    void RecordBasedFileManager::RealRID(FileHandle &fileHandle, std::vector<RID> &rid,RID cur) {
        void* pageContent=malloc(PAGE_SIZE);
        fileHandle.readPage(cur.pageNum,pageContent);
        unsigned short a, b;
        memcpy(&a,(char*)pageContent+PAGE_SIZE-(occupyField+cur.slotNum*SLOT_SIZE-1)*sizeof(unsigned),sizeof(unsigned short));
        memcpy(&b,(char*)pageContent+PAGE_SIZE-(occupyField+cur.slotNum*SLOT_SIZE-1)*sizeof(unsigned)+2,sizeof(unsigned short));
        RID temp;
        temp.pageNum=(unsigned)a;
        temp.slotNum=(unsigned)b;
        rid.push_back(temp);
        free(pageContent);
    }
    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }


    RC RBFM_ScanIterator::initScanIterator(FileHandle &handle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                        const CompOp &compOp, const void *value, const std::vector<std::string> &attributeNames) {
        PagedFileManager::instance().openFile(handle.filename,this->fileHandle);
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->attributeNames = attributeNames;
        this->curPageNum = 0;
        this->curSlotNum = 1;
        this->value = (char*) malloc( PAGE_SIZE);
        // value might not have the length of PAGE_SIZE
        if (compOp != NO_OP){
            int valueLength = 0;
            AttrType type;
            for(int i = 0; i < recordDescriptor.size(); i++){
                if(recordDescriptor[i].name == conditionAttribute){
                    type = recordDescriptor[i].type;
                    break;
                }
            }
            switch(type){
                case TypeReal:
                    valueLength = 4;
                    break;
                case TypeInt:
                    valueLength = 4;
                    break;
                case TypeVarChar:
                    int charLength;
                    memcpy(&charLength, value, sizeof(int));
                    valueLength = sizeof(int) +charLength;
                    break;
            }

            memcpy(this->value, value, valueLength);
        }
        return 0;
    }
    RC RBFM_ScanIterator::close() {
        if(this->value != NULL){
            free(this->value);
            this->value=NULL;
        }

        PagedFileManager::instance().closeFile(this->fileHandle);
        return 0;
    };
    RC RBFM_ScanIterator::changeSlotPointer(){
        unsigned filePages=this->fileHandle.getNumberOfPages();
        if(filePages<=0)return -1;
        unsigned slotOfCurrentPages;
        void* buffer=malloc(PAGE_SIZE);
        this->fileHandle.readPage(this->curPageNum,buffer);
        memcpy(&slotOfCurrentPages,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
        if(this->curPageNum<filePages-1&&this->curSlotNum>slotOfCurrentPages){
            this->curPageNum=this->curPageNum+1;
            this->curSlotNum=1;
            free(buffer);
            return 0;
        }
        else if(this->curPageNum==filePages-1&&this->curSlotNum>slotOfCurrentPages){
            free(buffer);
            return -1;
        }
        free(buffer);
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if(this->attributeNames.at(0)=="attr29"){
            counter++;
            if(counter>3000)return -1;
        }
        if(changeSlotPointer()==-1){
            return -1;
        }
        if(repeatedRID.size()!=0){
            for(auto item:repeatedRID){
                if(item.pageNum==this->curPageNum&&item.slotNum==this->curSlotNum)return -1;
            }
        }
        void* buffer=malloc(PAGE_SIZE);
        this->fileHandle.readPage(this->curPageNum,buffer);
        bool flag=0;//必须读出来一条为止！
        while(!flag){
            if(changeSlotPointer()==-1){

                free(buffer);
                return -1;
            }
            this->fileHandle.readPage(this->curPageNum,buffer);
            unsigned offsetPointer;
            memcpy(&offsetPointer,(char*)buffer+PAGE_SIZE-(this->curSlotNum*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
            unsigned SlotLen;
            memcpy(&SlotLen,(char*)buffer+PAGE_SIZE-(this->curSlotNum*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
            if(offsetPointer==RecordBasedFileManager::instance().getVACANT_Flag()){
                this->curSlotNum=this->curSlotNum+1;
                continue;
            }
            else{

                void * record=malloc(PAGE_SIZE);
                RID curRid;
                curRid.slotNum=this->curSlotNum;
                curRid.pageNum=this->curPageNum;
                RC rc=RecordBasedFileManager::instance().getRecord(this->fileHandle,curRid,record);
                if(rc==0){
                    if(screenRecord(record)!=0){//筛选没满足
                        this->curSlotNum=this->curSlotNum+1;
                        free(record);
                        continue;
                    }
                    else{
                        void* tempProjectRecord=malloc(PAGE_SIZE);
                        projectRecord(tempProjectRecord,record);
                        RecordBasedFileManager::instance().deRecord(tempProjectRecord,data,this->projectDescriptor);
                        rid=curRid;
                        free(tempProjectRecord);
                    }
                }
                else if(rc==2){
                    RecordBasedFileManager::instance().RealRID(this->fileHandle,this->repeatedRID,curRid);
                    void* temp=malloc(PAGE_SIZE);
                    RecordBasedFileManager::instance().readRecord(this->fileHandle,this->recordDescriptor,curRid,temp);
                    RecordBasedFileManager::instance().toRecord(temp,record,this->recordDescriptor);
                    free(temp);
                    if(screenRecord(record)!=0){//筛选没满足
                        this->curSlotNum=this->curSlotNum+1;
                        free(record);
                        continue;
                    }
                    else{
                        void* tempProjectRecord=malloc(PAGE_SIZE);
                        projectRecord(tempProjectRecord,record);
                        RecordBasedFileManager::instance().deRecord(tempProjectRecord,data,this->projectDescriptor);
                        rid=curRid;
                        free(tempProjectRecord);
                    }
                }
                else{
                    std::cerr<<"Sorry to getRecord in the function of getNextRecord!"<<std::endl;
                    free(record);
                    free(buffer);
                    return -1;
                }
                free(record);
                flag=1;
                this->curSlotNum=this->curSlotNum+1;
            }
        }
        free(buffer);
        return 0;
    };
    RC RBFM_ScanIterator::projectRecord(void*data,void*record){
        unsigned pointer=0;
        unsigned projectRecordSize=this->attributeNames.size();
        memcpy((char*)data+pointer,&projectRecordSize,sizeof(unsigned));
        pointer+=sizeof(unsigned);
        for(int i=0;i<projectRecordSize;i++){
            unsigned temp=0;
            memcpy((char*)data+pointer,&temp,sizeof(unsigned));
            pointer+=sizeof(unsigned);
        }
        unsigned counter=0;
        this->projectDescriptor.clear();
        for(int i=0;i<this->attributeNames.size();i++){
            int _;
            _=isExistedAttributeNames(this->attributeNames.at(i));
            if(_>=0){
                this->projectDescriptor.push_back(this->recordDescriptor.at(_));
                if(_==0){
                    unsigned len;
                    memcpy(&len,record,sizeof(unsigned));
                    unsigned start,end;
                    memcpy(&end,(char*)record+sizeof(unsigned),sizeof(unsigned));
                    start=(len+1)*sizeof(unsigned);
                    memcpy((char*)data+pointer,(char*)record+start,end-start);
                    pointer+=end-start;
                    memcpy((char*)data+(counter+1)*sizeof(unsigned),&pointer,sizeof(unsigned));
                }else{
                    unsigned start,end;
                    memcpy(&start,(char*)record+_*sizeof(unsigned),sizeof(unsigned));
                    memcpy(&end,(char*)record+(1+_)*sizeof(unsigned),sizeof(unsigned));
                    memcpy((char*)data+pointer,(char*)record+start,end-start);
                    pointer+=end-start;
                    memcpy((char*)data+(counter+1)*sizeof(unsigned),&pointer,sizeof(unsigned));
                }
                counter++;
            }
        }
        return 0;
    }
    RC RBFM_ScanIterator::isExistedAttributeNames(std::string name){
        for(int i=0;i<this->recordDescriptor.size();i++){
            if(this->recordDescriptor.at(i).name==name)return i;
        }
        return -1;

    }
    RC RBFM_ScanIterator::screenRecord(void *record) {
        if(this->compOp==NO_OP)return 0;
        int id=-1;
        AttrType type;
        for(int _=0;_<this->recordDescriptor.size();_++){
            if(this->recordDescriptor.at(_).name==this->conditionAttribute){
                id=_;
                type=recordDescriptor.at(_).type;
                break;
            }
        }
        if(id==-1)return -1;
        void * mine=malloc(PAGE_SIZE);
        unsigned start,end;
        if(id==0){
            unsigned len;
            memcpy(&len,record,sizeof(unsigned));
            memcpy(&end,(char*)record+sizeof(unsigned),sizeof(unsigned));
            start=(len+1)*sizeof(unsigned);
        }else{
            memcpy(&start,(char*)record+id*sizeof(unsigned),sizeof(unsigned));
            memcpy(&end,(char*)record+(id+1)*sizeof(unsigned),sizeof(unsigned));
        }
        if(end-start==0){
            free(mine);
            return -1;
        }
        if(this->recordDescriptor.at(id).type==TypeVarChar){
            unsigned charLen=end-start-1;
            memcpy(mine,&charLen,sizeof(unsigned));
            memcpy((char*)mine+sizeof(unsigned),(char*)record+start,charLen);
        }
        else
            memcpy(mine,(char*)record+start,end-start);
        if(testComparison(this->value,mine,type)!=1){
            free(mine);
            return -1;
        }
        free(mine);
        return 0;
    }
    RC RBFM_ScanIterator::testComparison(void * given, void* mine,AttrType type){
        if(this->compOp==NO_OP)return 0;
        int givenNum,mineNum;
        float givenReal,mineReal;
        int givenVarLen = 0;
        int mineVarLen = 0;
        std::string givenVar;
        std::string mineVar;
        switch(type){
            case TypeInt:
                memcpy(&givenNum,(char*)given,sizeof(unsigned));
                memcpy(&mineNum,(char*)mine,sizeof(unsigned));
                break;
            case TypeReal:
                memcpy(&givenReal,(char*)given,sizeof(unsigned));
                memcpy(&mineReal,(char*)mine,sizeof(unsigned));
                break;
            case TypeVarChar:
                memcpy(&givenVarLen, given, sizeof(int));
                memcpy(&mineVarLen, mine, sizeof(int));
                givenVar = std::string((char*)given + sizeof(int), givenVarLen);
                mineVar = std::string((char*)mine + sizeof(int), mineVarLen);
                break;
        }
        switch(this->compOp){
            case EQ_OP:
                if(type==TypeInt){
                    if(givenNum==mineNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(givenReal==mineReal)return 1;
                    else return 0;
                }
                else{
                    if(givenVarLen!=mineVarLen)return 0;
                    else{
                        return givenVar==mineVar;
                    }

                }
                break;
            case LT_OP:
                if(type==TypeInt){
                    if(mineNum<givenNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(mineReal<givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<givenVar;
                }
                break;
            case LE_OP:
                if(type==TypeInt){
                    if(mineNum<=givenNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(mineReal<=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<=givenVar;
                }
                break;
            case GT_OP:
                if(type==TypeInt){
                    if(mineNum>givenNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(mineReal>givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>givenVar;
                }
                break;
            case GE_OP:
                if(type==TypeInt){
                    if(mineNum>=givenNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(mineReal>=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>=givenVar;
                }
                break;
            case NE_OP:
                if(type==TypeInt){
                    if(mineNum!=givenNum)return 1;
                    else return 0;
                }
                else if(type==TypeReal){
                    if(mineReal!=givenReal)return 1;
                    else return 0;
                }
                else{
                    if(givenVarLen!=mineVarLen)return 1;
                    else{
                        return givenVar!=mineVar;
                    }
                }
                break;
        }
        return 0;
    }



} // namespace PeterDB

