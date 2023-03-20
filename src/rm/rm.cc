#include "src/include/rm.h"
#include "src/include/rbfm.h"
#include "src/include/ix.h"
#include <functional>
namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }
    const std::string RelationManager::tableName = "Tables";
    const std::string RelationManager::columnName = "Columns";
    const std::string RelationManager::additionalTableName="Additional_Tables";
    const std::vector<Attribute> RelationManager::tableAttr = std::vector<Attribute>{
            Attribute{"table-id", TypeInt, 4},
            Attribute{"table-name", TypeVarChar, 50},
            Attribute{"file-name", TypeVarChar, 50},
    };
    const std::vector<Attribute> RelationManager::additionalTableAttr = std::vector<Attribute>{
            Attribute{"Attr1", TypeInt, 4},
            Attribute{"Attr2", TypeVarChar, 50},
            Attribute{"Attr3", TypeVarChar, 50},
            Attribute{"Attr4", TypeInt, 4}
    };
    const std::vector<Attribute> RelationManager::columnAttr =std::vector<Attribute>{
            Attribute{"table-id", TypeInt, 4},
            Attribute{"column-name", TypeVarChar, 50},
            Attribute{"column-type", TypeInt, 4},
            Attribute{"column-length", TypeInt, 4},
            Attribute{"column-position", TypeInt, 4}
    };

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        RC rc;
        rc=RecordBasedFileManager::instance().createFile(tableName);
        if (rc < 0 ) {
            std::cerr<<"Tables can not be created!"<<std::endl;
            return rc;
        }

        rc=RecordBasedFileManager::instance().createFile(columnName);
        if(rc<0){
            std::cerr<<"Columns can not be created!"<<std::endl;
            return rc;
        }
        rc=RecordBasedFileManager::instance().createFile(additionalTableName);
        if(rc<0){
            std::cerr<<"Additional Table can not be created!"<<std::endl;
            return rc;
        }
        insertMetaData(tableName,tableAttr);
        insertMetaData(columnName,columnAttr);
        insertMetaData(additionalTableName,additionalTableAttr);

        return 0;
    }
    RC RelationManager::openCatalog(){
        RC rc;
        rc = RecordBasedFileManager::instance().openFile(RelationManager::tableName, this->tableFileHandler);
        if (rc < 0 ) return rc;
        rc = RecordBasedFileManager::instance().openFile(RelationManager::columnName, this->columnFileHandler);
        if (rc < 0 ) return rc;
        fseek(tableFileHandler.fl, 5* sizeof(unsigned), SEEK_SET);
        void * buffer=malloc(sizeof(unsigned));
        fread(buffer, sizeof(unsigned char), sizeof(unsigned), tableFileHandler.fl);
        memcpy(&tableCounter,buffer,sizeof(unsigned));
        rc= readAllTableName();
        return rc;
    }
    RC RelationManager::readAllTableName(){
        for(int i=0;i<tableFileHandler.pageNumber;i++){
            char *buffer= (char*)malloc(PAGE_SIZE);
            if(tableFileHandler.readPage(i,buffer)!=0){
                free(buffer);
                return -1;
            }
            unsigned slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
            for(int j=1;j<=slotNum;j++){
                unsigned Offset;
                memcpy(&Offset,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
                if(Offset==RecordBasedFileManager::instance().getVACANT_Flag()){
                    continue;
                }
                unsigned Len;
                memcpy(&Len,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
                char * record=(char*)malloc(Len);
                memcpy(record,(char*)buffer+Offset,Len);
                unsigned recordLen;
                memcpy(&recordLen,record,sizeof(unsigned));
                unsigned filenameStart,filenameEnd;
                memcpy(&filenameStart,(char*)record+sizeof(unsigned),sizeof(unsigned));
                unsigned filenameLen;
                memcpy(&filenameEnd,(char*)record+sizeof(unsigned)*2,sizeof(unsigned));
                filenameLen=filenameEnd-filenameStart;
                char* filename=(char*)malloc(filenameLen);
                memcpy(filename,(char*)record+filenameStart,filenameLen);
                std::string fileName;
                for(int _=0;_<filenameLen-1;_++){
                    unsigned char temp;
                    memcpy(&temp,filename+_,sizeof(unsigned char));
                    fileName+=temp;

                }
                allTableName.push_back(fileName);
                free(filename);
                free(record);
            }
            free(buffer);
        }
        return 0;
    }
    RC RelationManager::closeCatalog(){
        RC rc;
        allTableName.clear();
        if(tableFileHandler.fl==NULL){
            return -1;
        }
        fseek(tableFileHandler.fl, 5* sizeof(unsigned), SEEK_SET);

        void * buffer=malloc(sizeof(unsigned));
        memcpy(buffer,&tableCounter,sizeof(unsigned));
        fwrite(buffer, sizeof(unsigned char), sizeof(unsigned), tableFileHandler.fl);
        fseek(tableFileHandler.fl,0,SEEK_SET);
        rc = RecordBasedFileManager::instance().closeFile(this->tableFileHandler);
        if (rc < 0 ) return rc;
        rc = RecordBasedFileManager::instance().closeFile(this->columnFileHandler);
        if (rc < 0 ) return rc;
        return 0;
    }

    RC RelationManager::insertMetaData(const std::string &tableName, const std::vector<Attribute> &attrs){
        RC rc;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        rc = openCatalog();
        //can't open the table and column tables.
        if (rc < 0 ) return rc;
        char* tableBuffer= (char*) malloc(PAGE_SIZE);
        memset(tableBuffer,0,PAGE_SIZE);
        int tableID = tableCounter++;
        std::cerr<<"当前的tableID"+tableID<<std::endl;
        RID rid;

        createTableRaw(tableBuffer, tableName, tableID);
        rc = rbfm.insertRecord(this->tableFileHandler, RelationManager::tableAttr, tableBuffer, rid);
        if (rc < 0) {
            free(tableBuffer);
            return rc;
        }

        //cout<<attrs.size()<<endl;
        //create each of the column raw data and insert into the column file
        char* columnBuffer = (char*)calloc(PAGE_SIZE, sizeof(char));
        for (int i = 0; i < attrs.size(); i++){
            createColumnRaw(columnBuffer, tableID, attrs.at(i), i+1);
            rc = rbfm.insertRecord(this->columnFileHandler, RelationManager::columnAttr, columnBuffer, rid);
            if (rc < 0) {
                free(tableBuffer);
                free(columnBuffer);
                return rc;
            }
        }
        free(tableBuffer);
        free(columnBuffer);

        rc = closeCatalog();
        return rc;
    }
    void RelationManager::createTableRaw(char *data, const std::string &tableName, const int ID) {
        unsigned pointer = 0;
        char nullByte = 0x00;//Because the table has several attributes
        memcpy(data, &nullByte, sizeof(char));
        pointer+= sizeof(char);
        //table-id
        memcpy(data + pointer, &ID, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        //table-name
        int tableNameLength = tableName.length();
        const char* name = tableName.c_str();
        memcpy(data + pointer, &tableNameLength, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        memcpy(data + pointer, name, tableNameLength);
        pointer+= tableNameLength;
        //file-name

       // std::string fileName =((tableName==RelationManager::tableName)||(tableName==RelationManager::columnName))?tableName:(tableName+".bin");
        std::string fileName=getFileName(tableName);
        int fileNameLength = fileName.size();
        const char* fileNameChar = fileName.c_str();
        memcpy(data + pointer, &fileNameLength, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        memcpy(data + pointer, fileNameChar, fileNameLength);
        pointer+= fileNameLength;
    }
    void RelationManager::createColumnRaw(char *data, const int ID, const Attribute &attr, const int position) {
        int pointer = 0;
        //nullByte
        char nullByte = 0x0;
        memcpy(data, &nullByte, sizeof(char));
        pointer += sizeof(char);
        //table-id
        memcpy(data + pointer, &ID, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        //column-name
        int columnNameLength = attr.name.size();
        const char* columnName = attr.name.c_str();
        memcpy(data + pointer, &columnNameLength, sizeof(unsigned));
        pointer += sizeof(unsigned);
        memcpy(data + pointer, columnName, columnNameLength);
        pointer += columnNameLength;
        //column-type
        int columnType = attr.type;
        memcpy(data + pointer, &columnType, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        //column-length
        int columnLength = attr.length;
        memcpy(data +pointer, &columnLength, sizeof(unsigned));
        pointer+= sizeof(unsigned);
        //column-position
        memcpy(data + pointer, &position, sizeof(unsigned));
        pointer+= sizeof(unsigned);
    }
    RC RelationManager::deleteCatalog() {

        RC rc=RecordBasedFileManager::instance().destroyFile(tableName);
        if(rc<0)return -1;
        rc=RecordBasedFileManager::instance().destroyFile(columnName);
        if(rc<0)return -1;
        rc=RecordBasedFileManager::instance().destroyFile(additionalTableName);
        if(rc<0)return -1;
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RC rc;
        if(tableName==RelationManager::tableName || tableName==RelationManager::columnName)return -1;
        rc=openCatalog();
        if(rc!=0)return rc;
        if(isExistedTableName(tableName)){
            closeCatalog();
            return -1;
        }
        rc=closeCatalog();
        const std :: string fileName=getFileName(tableName);
        rc = RecordBasedFileManager::instance().createFile(fileName);
        if (rc != 0 ) return rc;

        //Insert table meta data to table and column
        rc = insertMetaData(tableName, attrs);
        if(rc != 0) return rc;
        return rc;
    }
    std::string RelationManager::getFileName(const std::string tableName){
        if(tableName==RelationManager::tableName||tableName==RelationManager::columnName)return tableName;
        else return tableName;
    }
    bool RelationManager::isExistedTableName(const std::string & tableName){
        for(auto item:allTableName){
            if(item==tableName){
                std::cerr<<tableName<<std::endl;
                std::cerr<<"Table Name is repeated!"<<std::endl;
                return 1;
            }
        }
        return 0;
    }

    RC RelationManager::getTableRID(RID & rid,const std::string &tableName){
        for(int i=0;i<tableFileHandler.pageNumber;i++){
            char *buffer= (char*)malloc(PAGE_SIZE);
            if(tableFileHandler.readPage(i,buffer)!=0){
                free(buffer);
                return -1;
            }
            unsigned slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
            for(int j=1;j<=slotNum;j++){
                unsigned Offset;
                memcpy(&Offset,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
                if(Offset==RecordBasedFileManager::instance().getVACANT_Flag()){
                    continue;
                }
                unsigned Len;
                memcpy(&Len,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
                char * record=(char*)malloc(Len);
                memcpy(record,(char*)buffer+Offset,Len);
                unsigned filenameStart,filenameEnd;
                memcpy(&filenameStart,(char*)record+sizeof(unsigned),sizeof(unsigned));
                unsigned filenameLen;
                memcpy(&filenameEnd,(char*)record+sizeof(unsigned)*2,sizeof(unsigned));
                filenameLen=filenameEnd-filenameStart;
                char* filename=(char*)malloc(filenameLen);
                memcpy(filename,(char*)record+filenameStart,filenameLen);
                std::string fileName;
                for(int _=0;_<filenameLen-1;_++){
                    unsigned char temp;
                    memcpy(&temp,filename+_,sizeof(unsigned char));
                    fileName+=temp;
                }
                if(fileName==tableName){
                    free(filename);
                    free(record);
                    free(buffer);
                    rid.slotNum=j;
                    rid.pageNum=i;
                    return 0;

                }
                free(filename);
                free(record);
            }
            free(buffer);
        }
        return -1;

    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
            return -1;
        /*接下来我要把根据这个创建的全部索引文件全部删除哦*/
        RC rc= destroyAllIndex(tableName);
        if(rc!=0){
            std::cerr<<"delete all index failed!"<<std::endl;
            closeCatalog();
            return -1;
        }

        rc = openCatalog();
        if(rc != 0){
            closeCatalog();
            return -1;
        }

        std::string fileName = getFileName(tableName);
        rc = RecordBasedFileManager::instance().destroyFile(fileName);
        if(rc!=0){
            closeCatalog();
            return -1;
        }
        RC tid= getTableID(tableName);
        if(tid<0){
            closeCatalog();
            return -1;
        }
        RID tableRID;
        rc= getTableRID(tableRID,tableName);
        if(rc!=0){
            closeCatalog();
            return -1;
        }
        /*将全部记录再删除了*/
        std::vector<Attribute> recordDescriptor;
        rc=RecordBasedFileManager::instance().deleteRecord(tableFileHandler,recordDescriptor,tableRID);
        if(rc!=0){
            std::cerr<<"delete the record of tablename in the Table failed!"<<std::endl;
            closeCatalog();
            return -1;
        }
        std::vector<RID> columnRID;
        rc=getColumnRID(columnRID,tid);
        if(rc!=0){
            std::cerr<<"delete the record of columnname in the Column Table failed!"<<std::endl;
            closeCatalog();
            return -1;
        }
        for(int _=0;_<columnRID.size();_++){
            std::vector<Attribute> recordDescriptor;
            RecordBasedFileManager::instance().deleteRecord(columnFileHandler,recordDescriptor,columnRID.at(_));
        }
        rc=closeCatalog();
        if(rc!=0){
            std::cerr<<"closeCatalog failed!"<<std::endl;
            return -1;
        }
        return 0;
    }
    RC RelationManager::getTableID(const std::string &tableName){
        for(int i=0;i<tableFileHandler.pageNumber;i++){
            char *buffer= (char*)malloc(PAGE_SIZE);
            if(tableFileHandler.readPage(i,buffer)!=0){
                free(buffer);
                return -1;
            }
            unsigned slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
            for(int j=1;j<=slotNum;j++){
                unsigned Offset;
                memcpy(&Offset,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
                if(Offset==RecordBasedFileManager::instance().getVACANT_Flag()){
                    continue;
                }
                unsigned Len;
                memcpy(&Len,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
                char * record=(char*)malloc(Len);
                memcpy(record,(char*)buffer+Offset,Len);
                unsigned recordLen;
                memcpy(&recordLen,(char*)record,sizeof(unsigned));
                unsigned fileId;
                memcpy(&fileId,(char*)record+(recordLen+1)*sizeof(unsigned),sizeof(unsigned));
                unsigned filenameStart,filenameEnd;
                memcpy(&filenameStart,(char*)record+sizeof(unsigned),sizeof(unsigned));
                unsigned filenameLen;
                memcpy(&filenameEnd,(char*)record+sizeof(unsigned)*2,sizeof(unsigned));
                filenameLen=filenameEnd-filenameStart;
                char* filename=(char*)malloc(filenameLen);
                memcpy(filename,(char*)record+filenameStart,filenameLen);
                std::string fileName;
                for(int _=0;_<filenameLen-1;_++){
                    unsigned char temp;
                    memcpy(&temp,filename+_,sizeof(unsigned char));
                    fileName+=temp;

                }
                if(fileName==tableName){
                    free(filename);
                    free(record);
                    free(buffer);
                    return fileId;
                }
                free(filename);
                free(record);
            }
            free(buffer);
        }
        return -1;
    }
    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {

        RC rc;
        rc = openCatalog();
        if (rc != 0) {
            std::cerr << "Can't open the Catalog!" << std::endl;
            return -1;
        }
        if (!isExistedTableName(tableName)) {
            std::cerr << "This TableName is not existed!" << std::endl;
            closeCatalog();
            return -1;
        }
        RC tid = getTableID(tableName);

        if (tid < 0) {
            std::cerr << "Can't find the id of the TableName!" << std::endl;
            closeCatalog();
            return -1;
        }
        rc = combineColumnWithTableID(attrs, tid);
        if (rc < 0) {
            std::cerr << "combineColumnWithTableID failed!" << std::endl;
            closeCatalog();
            return -1;
        }
        rc = closeCatalog();
        if (rc != 0) {
            std::cerr << "Close Catalog failed!" << std::endl;
            return -1;
        }
        return 0;
    }
    RC RelationManager::getColumnRID(std::vector<RID>columnRID,int tableID){
        for(int i=0;i<columnFileHandler.pageNumber;i++){
            char *buffer= (char*)malloc(PAGE_SIZE);
            if(columnFileHandler.readPage(i,buffer)!=0){
                free(buffer);
                std::cerr<<"Read the file failed"<<std::endl;
                return -1;
            }
            unsigned slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
            for(int j=1;j<=slotNum;j++){
                unsigned Offset;
                memcpy(&Offset,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
                if(Offset==RecordBasedFileManager::instance().getVACANT_Flag()){
                    continue;
                }
                unsigned Len;
                memcpy(&Len,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
                char * record=(char*)malloc(Len);///
                memcpy(record,(char*)buffer+Offset,Len);
                unsigned recordLen;
                memcpy(&recordLen,(char*)record,sizeof(unsigned));
                unsigned Id;
                memcpy(&Id,(char*)record+(1+recordLen)*sizeof(unsigned),sizeof(unsigned));
                if(Id!=tableID){
                    free(record);
                    continue;
                }
                RID tempRID;
                tempRID.pageNum=i;
                tempRID.slotNum=j;
                columnRID.push_back(tempRID);
                free(record);
            }
            free(buffer);
        }
        return 0;
    }
    RC RelationManager::combineColumnWithTableID(std::vector<Attribute>&attrs,RC tableID){
        for(int i=0;i<columnFileHandler.pageNumber;i++){
            char *buffer= (char*)malloc(PAGE_SIZE);
            if(columnFileHandler.readPage(i,buffer)!=0){
                free(buffer);
                std::cerr<<"Read the file failed"<<std::endl;
                return -1;
            }
            unsigned slotNum;
            memcpy(&slotNum,(char*)buffer+PAGE_SIZE-RecordBasedFileManager::instance().getOccupyField()*sizeof(unsigned),sizeof(unsigned));
            for(int j=1;j<=slotNum;j++){
                unsigned Offset;
                memcpy(&Offset,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField())*sizeof(unsigned),sizeof(unsigned));
                if(Offset==RecordBasedFileManager::instance().getVACANT_Flag()){
                    continue;
                }
                unsigned Len;
                memcpy(&Len,(char*)buffer+PAGE_SIZE-(j*RecordBasedFileManager::instance().getSLOT_SIZE()+RecordBasedFileManager::instance().getOccupyField()-1)*sizeof(unsigned),sizeof(unsigned));
                char * record=(char*)malloc(Len);///
                memcpy(record,(char*)buffer+Offset,Len);
                unsigned recordLen;
                memcpy(&recordLen,(char*)record,sizeof(unsigned));
                unsigned Id;
                memcpy(&Id,(char*)record+(1+recordLen)*sizeof(unsigned),sizeof(unsigned));
                if(Id!=tableID){
                    free(record);
                    continue;
                }
                unsigned Start,End;
                memcpy(&Start,record+1*sizeof(unsigned),sizeof(unsigned));
                memcpy(&End,record+2*sizeof(unsigned),sizeof(unsigned));
                unsigned Length=End-Start;
                void * Name=malloc(Length);///
                memcpy(Name,(char*)record+Start,Length);
                std::string str((char*)Name);

                memcpy(&Start,record+2*sizeof(unsigned),sizeof(unsigned));
                memcpy(&End,record+3*sizeof(unsigned),sizeof(unsigned));
                Length=End-Start;
                AttrType Type;
                memcpy(&Type,(char*)record+Start,Length);

                memcpy(&Start,record+3*sizeof(unsigned),sizeof(unsigned));
                memcpy(&End,record+4*sizeof(unsigned),sizeof(unsigned));
                Length=End-Start;
                AttrLength LLen;
                memcpy(&LLen,(char*)record+Start,Length);
                attrs.push_back(Attribute{str,Type,LLen});
                free(Name);
                free(record);
            }
            free(buffer);
        }
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
            return -1;
        std::string fileName = getFileName(tableName);
        FileHandle fileHandle;
        RC rc = 0;
        std::vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
        if(rc != 0) {
            std::cerr<<"Fail to getAttributes!"<<std::endl;
            return -1;
        }
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if(rc != 0){
            std::cerr<<"Fail to openFile"<<" with name: "+fileName<<std::endl;
            return -1;
        }
        rc=RecordBasedFileManager::instance().insertRecord(fileHandle, attrs, data, rid);
        if(rc != 0){
            std::cerr<<"Fail to insert the tuple!"<<std::endl;
        }
        rc = RecordBasedFileManager::instance().closeFile(fileHandle);
        if(rc!=0){
            std::cerr<<"Fail to close File!"<<std::endl;
        }

        /*接下来就是索引的操作！*/
        std::vector<Attribute>indexAttrs;
        rc= getAttributes(tableName,indexAttrs);
        IndexManager& ixm = IndexManager::instance();
        for(Attribute attribute: indexAttrs){
            //先测试文件索引文件是否存在
            std::string indexFileName = getIndexTableName(tableName, attribute.name);
            FILE* fp = fopen(indexFileName.c_str(), "r");
            if(fp== NULL){
                continue;
            }
            fclose(fp);
            // With null byte at the beginning
            char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
            readAttribute(tableName, rid, attribute.name, rawAttribute);
            char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
            //skip null bit
            memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));
            IXFileHandle ixFileHandle;
            rc = ixm.openFile(indexFileName, ixFileHandle);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                free(key);
                free(rawAttribute);
                std::cerr<<"Can't open index file"<<std::endl;
                return -1;
            }
            rc = ixm.insertEntry(ixFileHandle, attribute, key, rid);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                free(key);
                free(rawAttribute);
                std::cerr<<"Can't insert index"<<std::endl;
                return -1;
            }
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
        }

        return rc;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
            return -1;
        std::string fileName = getFileName(tableName);
        FileHandle fileHandle;
        RC rc = 0;
        std::vector<Attribute> attrs;
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if(rc != 0)  {
            std::cerr<<"Fail to openFile!"<<std::endl;
            return -1;
        }
        /*接下来删除索引的操作*/
        std::vector<Attribute>indexAttrs;
        rc= getAttributes(tableName,indexAttrs);
        IndexManager& ixm = IndexManager::instance();
        for(Attribute attribute: indexAttrs){
            //先测试文件索引文件是否存在
            std::string indexFileName = getIndexTableName(tableName, attribute.name);
            FILE* fp = fopen(indexFileName.c_str(), "r");
            if(fp== NULL){
                continue;
            }
            fclose(fp);
            // With null byte at the beginning
            char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
            readAttribute(tableName, rid, attribute.name, rawAttribute);
            char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
            memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));
            IXFileHandle ixFileHandle;
            rc = ixm.openFile(indexFileName, ixFileHandle);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                std::cerr<<"Can't open index file"<<std::endl;
                free(key);
                free(rawAttribute);
                return -1;
            }
            rc = ixm.deleteEntry(ixFileHandle, attribute, key, rid);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                std::cerr<<"Can't delete index"<<std::endl;
                free(key);
                free(rawAttribute);
                return -1;
            }
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
        }
        rc = RecordBasedFileManager::instance().deleteRecord(fileHandle, attrs, rid);
        if(rc != 0) {
            std::cerr<<"Fail to deleteRecord!"<<std::endl;
            return -1;
        }
        rc = RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, RID &rid) {
        if(tableName == RelationManager::tableName || tableName == RelationManager::columnName)
            return -1;
        std::string fileName = getFileName(tableName);
        FileHandle fileHandle;
        RC rc = 0;
        std::vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
        if(rc != 0){
            std::cerr<<"Fail to getAttributes!"<<std::endl;
            return -1;
        }
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if(rc != 0){
            std::cerr<<"Fail to openFile!"<<std::endl;
            return -1;
        }
        /*接下来就是删除索引的操作了*/
        std::vector<Attribute>indexAttrs;
        rc= getAttributes(tableName,indexAttrs);
        IndexManager& ixm = IndexManager::instance();
        for(Attribute attribute: indexAttrs){
            //先测试文件索引文件是否存在
            std::string indexFileName = getIndexTableName(tableName, attribute.name);
            FILE* fp = fopen(indexFileName.c_str(), "r");
            if(fp== NULL){
                continue;
            }
            fclose(fp);
            // With null byte at the beginning
            char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
            readAttribute(tableName, rid, attribute.name, rawAttribute);
            char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
            memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));
            IXFileHandle ixFileHandle;
            rc = ixm.openFile(indexFileName, ixFileHandle);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                std::cerr<<"Can't open index file"<<std::endl;
                free(key);
                free(rawAttribute);
                return -1;
            }
            rc = ixm.deleteEntry(ixFileHandle, attribute, key, rid);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                std::cerr<<"Can't delete index"<<std::endl;
                free(key);
                free(rawAttribute);
                return -1;
            }
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
        }
        rc = RecordBasedFileManager::instance().updateRecord(fileHandle, attrs, data, rid);
        if(rc != 0){
            std::cerr<<"Fail to updateRecord!"<<std::endl;
            return -1;
        }
        /*接下来就是索引的操作！*/
        indexAttrs.clear();
        rc= getAttributes(tableName,indexAttrs);
        for(Attribute attribute: indexAttrs){
            //先测试文件索引文件是否存在
            std::string indexFileName = getIndexTableName(tableName, attribute.name);
            FILE* fp = fopen(indexFileName.c_str(), "r");
            if(fp== NULL){
                continue;
            }
            fclose(fp);
            // With null byte at the beginning
            char* rawAttribute = (char*)calloc(PAGE_SIZE, sizeof(char));
            readAttribute(tableName, rid, attribute.name, rawAttribute);
            char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
            //skip null bit
            memcpy(key, rawAttribute + sizeof(char), PAGE_SIZE - sizeof(char));
            IXFileHandle ixFileHandle;
            rc = ixm.openFile(indexFileName, ixFileHandle);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                free(key);
                free(rawAttribute);
                std::cerr<<"Can't open index file"<<std::endl;
                return -1;
            }
            rc = ixm.insertEntry(ixFileHandle, attribute, key, rid);
            if(rc != 0){
                ixm.closeFile(ixFileHandle);
                free(key);
                free(rawAttribute);
                std::cerr<<"Can't insert index"<<std::endl;
                return -1;
            }
            ixm.closeFile(ixFileHandle);
            free(key);
            free(rawAttribute);
        }
        rc=RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::string fileName = getFileName(tableName);
        FileHandle fileHandle;
        RC rc = 0;
        std::vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
        if(rc != 0){
            std::cerr<<"Fail to getAttributes!"<<std::endl;
            return -1;
        }
        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if(rc != 0){
            std::cerr<<"Fail to open the File!"<<std::endl;
            return -1;
        }
        rc = RecordBasedFileManager::instance().readRecord(fileHandle, attrs, rid, data);
        if(rc != 0){
            std::cerr<<"Fail to recordRecord!"<<std::endl;
            return -1;
        }

        rc=RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return RecordBasedFileManager::instance().printRecord(attrs,data,out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::string fileName = getFileName(tableName);
        FileHandle fileHandle;
        RC rc = 0;
        std::vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
        if(rc != 0){
            std::cerr<<"Fail to getAttributes!"<<std::endl;
            return -1;
        }

        rc = RecordBasedFileManager::instance().openFile(fileName, fileHandle);
        if(rc != 0){
            std::cerr<<"Fail to openFile!"<<std::endl;
            return -1;
        }

        rc = RecordBasedFileManager::instance().readAttribute(fileHandle, attrs, rid, attributeName, data);
        if(rc != 0){
            std::cerr<<"Fail to operate the readAttribute in RecordBasedFileManager!"<<std::endl;
            return -1;
        }

        rc=RecordBasedFileManager::instance().closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        std:: string fileName = getFileName(tableName);
        FileHandle fileHandle;
        fileHandle.filename=tableName;
        std::vector<Attribute> attrs;
        RC rc = getAttributes(tableName, attrs);
        if(rc < 0) return -1;
        return rm_ScanIterator.initScanIterator(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames);
    }


    RM_ScanIterator::RM_ScanIterator() = default;


    RC RM_ScanIterator::initScanIterator(FileHandle& fileHandle, const std::vector<Attribute>& recordDescriptor,
                                         const std::string& conditionAttribute, const CompOp& compOp, const void* value,
                                         const std::vector<std::string>& attributeNames){
        return this->rbfm_iter.initScanIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    }



    RM_ScanIterator::~RM_ScanIterator() = default;


    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return this->rbfm_iter.getNextRecord(rid,data);
    }

    RC RM_ScanIterator::close() {
        return this->rbfm_iter.close();
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        //先判断文件是否存在以及传入构造索引的attributename是否存在？
        if(isAttributeExist(tableName,attributeName)==false)return -1;

        std::string indexFileName = getIndexTableName(tableName,attributeName);
        IndexManager& indexManager = IndexManager::instance();
        RC rc = indexManager.createFile(indexFileName);
        if(rc != 0)return -1;
        IXFileHandle ixFileHandle;
        rc = indexManager.openFile(indexFileName, ixFileHandle);
        if(rc != 0) return -1;
        std::vector<Attribute> attributeNames;
        attributeNames.push_back(fetchAttribute(tableName,attributeName));
        insertMetaData(getIndexTableName(tableName,attributeName),attributeNames);

        /*往里面插入Entry数据*/
        RM_ScanIterator iter;
        std::vector<std::string>Names;
        for(auto item : attributeNames){
            Names.push_back(item.name);
        }
        rc = scan(tableName, "", NO_OP, NULL, Names, iter);
        if(rc != 0) return -1;
        RID rid;
        char* data = (char*)malloc(PAGE_SIZE);
        while(iter.getNextTuple(rid, data) != RM_EOF){
            // For there is null byte in the front of data
            char*key = (char*)malloc(PAGE_SIZE);
            memcpy(key, data + sizeof(char), PAGE_SIZE-sizeof(char));
            if(attributeNames.size()>0)
                indexManager.insertEntry(ixFileHandle, attributeNames.at(0), key, rid);
            free(key);
        }

        free(data);
        rc=indexManager.closeFile(ixFileHandle);
        if(rc != 0)return -1;
        iter.close();
        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        if(isAttributeExist(tableName,attributeName)==false)return -1;
        std::string indexFileName= getIndexTableName(tableName,attributeName);
        IndexManager & indexManager =IndexManager::instance();
        RC rc=indexManager.destroyFile(indexFileName);
        if(rc != 0 )return -1;
        IXFileHandle ixFileHandle;
        rc = deleteTable(indexFileName);
        if(rc != 0)return -1;
        return 0;
    }



    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return rm_IndexScanIterator.initScanIterator(tableName, attributeName, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    }

    bool RelationManager::isAttributeExist(const std::string &tableName, const std::string &attributeName) {
        std::vector<Attribute> attributes;
        RC rc = getAttributes(tableName, attributes);
        if(rc != 0) return false;
        for(Attribute attribute:attributes){
            if(attribute.name == attributeName)
                return true;
        }
        return false;
    }

    RC RelationManager::destroyAllIndex(const std::string &tableName) {
        std::vector <Attribute > attributes;
        RC rc = getAttributes(tableName,attributes);
        if(rc != 0 )return -1;
        for(Attribute attribute:attributes){
            std::string fileName = getIndexTableName(tableName,attribute.name);
            FILE* fp = fopen(fileName.c_str(), "r");
            if(fp!= NULL){
                destroyIndex(tableName,attribute.name);
                fclose(fp);
            }

        }
        return 0;
    }

    std::string RelationManager::getIndexTableName(const std::string tableName,const std::string attributeNames) {
        return tableName+"_"+attributeNames+".idx";
    }

    Attribute RelationManager::fetchAttribute(const std::string tableName,const std::string attributeName) {
        std::vector<Attribute> attributes;
        getAttributes(tableName,attributes);
        Attribute attribute;
        for(int i = 0; i < attributes.size(); i++){
            if(attributes[i].name == attributeName)
                attribute = attributes[i];
        }
        return attribute;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return this->ix_ScanIterator.getNextEntry(rid,key);
    }
    RC RM_IndexScanIterator::initScanIterator(const std::string &tableName,
                                              const std::string &attributeName,
                                              const void *lowKey,
                                              const void *highKey,
                                              bool lowKeyInclusive,
                                              bool highKeyInclusive
    ){

        std::string indexFileName = RelationManager::instance().getIndexTableName(tableName,attributeName);
        Attribute attribute;
        std::vector<Attribute> attributes;
        RC rc = RelationManager::instance().getAttributes(tableName, attributes);
        if(rc != 0){
            std::cerr<<"No such table!"<<std::endl;
            return rc;
        }
        // find the attribute with the specific name
        attribute = RelationManager::instance().fetchAttribute(tableName,attributeName);
        rc = IndexManager::instance().openFile(indexFileName, ixFileHandle);
        if(rc != 0){
            std::cerr<<"No such index table!"<<std::endl;
            return rc;
        }
        return IndexManager::instance().scan(ixFileHandle,attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, this->ix_ScanIterator);

    }
    RC RM_IndexScanIterator::close(){
        RC rc = IndexManager::instance().closeFile(ixFileHandle);
        if(rc != 0){
            std::cerr<<"No such index table!"<<std::endl;
            return rc;
        }
        return this->ix_ScanIterator.close();
    }

} // namespace PeterDB