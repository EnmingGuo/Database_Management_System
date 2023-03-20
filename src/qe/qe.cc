#include "src/include/qe.h"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->input = input;
        this->condition = condition;
        getAttributes(this->attributes);
    }


    RC Filter::getNextTuple(void *data) {
        while(input->getNextTuple(data) != QE_EOF){
            if(isSatisfy((char*)data,this->attributes,this->condition))
                return 0;
        }
        return QE_EOF;
    }
    bool Filter::isSatisfy(char *data, const std::vector<Attribute> &attrs, const Condition& cond) {
        int pivot = ceil((double)attrs.size() / 8);
        char* nullBytes = (char*)malloc(PAGE_SIZE);
        memset(nullBytes,0,PAGE_SIZE);
        memcpy(nullBytes, data, pivot);
        if(cond.bRhsIsAttr == false){
            for(int i = 0 ; i < attrs.size(); i++){
                if(RecordBasedFileManager::instance().checkNull(i, nullBytes))
                    continue;
                if(attrs[i].name == cond.lhsAttr) {
                    break;
                }
                if(attrs[i].type == TypeVarChar){
                    int strLength;
                    memcpy(&strLength, data + pivot, sizeof(int));
                    pivot += (sizeof(int) + strLength);
                }
                else{
                    pivot += sizeof(int);
                }
            }
            // save the key
            char* key = (char*)malloc(PAGE_SIZE);
            if(cond.rhsValue.type == TypeVarChar){
                int strLength;
                memcpy(&strLength, data + pivot, sizeof(int));
                memcpy(key, data + pivot, strLength + sizeof(int));
            }
            else{
                memcpy(key, data + pivot, sizeof(int));
            }
            bool satisfy = isCompareSatisfy(key);
            free(key);
            free(nullBytes);
            return satisfy;
        }
        free(nullBytes);
        return false;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        return input->getAttributes(attrs);
    }

    bool Filter::isCompareSatisfy(void *key) {
        if(this->condition.op==NO_OP)return 0;
        int givenNum,mineNum;
        float givenReal,mineReal;
        int givenVarLen = 0;
        int mineVarLen = 0;
        std::string givenVar;
        std::string mineVar;
        switch(this->condition.rhsValue.type){
            case TypeInt:
                memcpy(&mineNum,(char*)key,sizeof(unsigned));
                memcpy(&givenNum,(char*)this->condition.rhsValue.data,sizeof(unsigned));
                break;
            case TypeReal:
                memcpy(&mineReal,(char*)key,sizeof(unsigned));
                memcpy(&givenReal,(char*)this->condition.rhsValue.data,sizeof(unsigned));
                break;
            case TypeVarChar:
                memcpy(&mineVarLen, key, sizeof(int));
                memcpy(&givenVarLen, this->condition.rhsValue.data, sizeof(int));
                mineVar = std::string((char*)key + sizeof(int), givenVarLen);
                givenVar = std::string((char*)this->condition.rhsValue.data+ sizeof(int), mineVarLen);
                break;
        }
        switch(this->condition.op){
            case EQ_OP:
                if(this->condition.rhsValue.type==TypeInt){
                    if(givenNum==mineNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
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
                if(this->condition.rhsValue.type==TypeInt){
                    if(mineNum<givenNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
                    if(mineReal<givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<givenVar;
                }
                break;
            case LE_OP:
                if(this->condition.rhsValue.type==TypeInt){
                    if(mineNum<=givenNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
                    if(mineReal<=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<=givenVar;
                }
                break;
            case GT_OP:
                if(this->condition.rhsValue.type==TypeInt){
                    if(mineNum>givenNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
                    if(mineReal>givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>givenVar;
                }
                break;
            case GE_OP:
                if(this->condition.rhsValue.type==TypeInt){
                    if(mineNum>=givenNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
                    if(mineReal>=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>=givenVar;
                }
                break;
            case NE_OP:
                if(this->condition.rhsValue.type==TypeInt){
                    if(mineNum!=givenNum)return 1;
                    else return 0;
                }
                else if(this->condition.rhsValue.type==TypeReal){
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

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->attrNames = attrNames;
        this->input = input;
        input->getAttributes(this->attributes);
    }

    RC Project::getNextTuple(void *data) {

        char* rawTuple = (char*) malloc(PAGE_SIZE );
        RC rc = input->getNextTuple(rawTuple);
        if(rc != 0){
            free(rawTuple);
            return -1;
        }
        /*我先需要把这个东西转换成对应的Record模式*/
        char * rawRecord =(char*)malloc(PAGE_SIZE);
        RecordBasedFileManager::instance().toRecord(rawTuple,rawRecord,this->attributes);
        char * newRecord = (char*)malloc(PAGE_SIZE);
        int number = this->attrNames.size();
        memcpy(newRecord,&number,sizeof(unsigned));
        int newPivot=(1+number)*sizeof(unsigned);
        for(int i=0;i<this->attrNames.size();i++){
            int position= findAttributePosition(this->attrNames.at(i));
            int start,end;
            if(position==0){
                start=(1+this->attributes.size())*sizeof(unsigned);
            }else{
                memcpy(&start,(char*)rawRecord+position*sizeof(unsigned),sizeof(unsigned));
            }
            memcpy(&end,(char*)rawRecord+(position+1)*sizeof(unsigned),sizeof(unsigned));
            memcpy((char*)newRecord+newPivot,(char*)rawRecord+start,end-start);
            newPivot+=end-start;
            memcpy((char*)newRecord+(i+1)*sizeof(unsigned),&newPivot,sizeof(unsigned));
        }
        std::vector<Attribute> projectAttributes;
        getAttributes(projectAttributes);
        RecordBasedFileManager::instance().deRecord(newRecord,data,projectAttributes);
        free(newRecord);
        free(rawTuple);
        free(rawRecord);
        return 0;
    }

    int Project::findAttributePosition(std::string attrName){
        for(int i=0;i<this->attributes.size();i++){
            if(this->attributes.at(i).name==attrName)return i;
        }
        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        std::vector<Attribute> temp;
        input->getAttributes(temp);
        for (std::string s : this->attrNames){
            for(Attribute attr : temp){
                if (attr.name == s){
                    attrs.push_back(attr);
                    break;
                }
            }
        }
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->currentRightData=(char*)malloc(PAGE_SIZE);
        this->maxSize = numPages*PAGE_SIZE;
        this->singlePosibileSize = 0;
        this->leftFinished = false;
        for(auto item:this->leftAttrs){
            if(item.name==this->condition.lhsAttr){
                this->leftAttribute=item;
            }
        }
        for(auto item:this->rightAttrs){
            if(item.name==this->condition.rhsAttr){
                this->rightAttribue=item;
            }
        }
        for (int i = 0; i < this->leftAttrs.size(); i++){
            this->singlePosibileSize += this->leftAttrs.at(i).length;
        }
        fillLeftBuffer();
    }

    BNLJoin::~BNLJoin() {
        free(this->currentRightData);
    }

    RC BNLJoin::getNextTuple(void *data) {
        while(1){
            char* rightKey = (char*) malloc(PAGE_SIZE);
            MyJoin::findKey(rightKey, this->currentRightData, this->rightAttrs, this->condition.rhsAttr);
            int correspondingKeySize= correspondingVectorSize(rightKey,this->rightAttribue.type);
            if(this->pointer>=correspondingKeySize){
                int result = this->rightIn->getNextTuple(this->currentRightData);
                this->pointer=0;
                if(result == QE_EOF){
                    if(this->leftFinished==true){
                        free(rightKey);
                        return QE_EOF;
                    }
                    else{
                        free(rightKey);
                        fillLeftBuffer();
                    }
                }
            }
            else{
                BNLGiveResult(rightKey,data);
                this->pointer++;
                free(rightKey);
                return 0;
            }
        }
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.end(), this->leftAttrs.begin(), this->leftAttrs.end());
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }

    void BNLJoin::fillLeftBuffer() {
        this->rightIn->setIterator();
        this->rightIn->getNextTuple(this->currentRightData);
        this->pointer =0;
        this->intMap.clear();
        this->floatMap.clear();
        this->varcharMap.clear();
        this->remainSize=0;
        while(1){
            if(this->maxSize-(this->remainSize+ this->singlePosibileSize)<=10)break;
            void* tuple = malloc(PAGE_SIZE);
            void* record = malloc(PAGE_SIZE);
            void* key = malloc(PAGE_SIZE);
            int result = this->leftIn->getNextTuple(tuple);
            if(result==QE_EOF){
                this->leftFinished=true;
                free(tuple);
                free(record);
                free(key);
                break;
            }
            RecordBasedFileManager::instance().toRecord(tuple,record,this->leftAttrs);
            int singleSize=checkRecordSize(record);
            this->remainSize +=singleSize;
            MyJoin::findKey(key,tuple,this->leftAttrs,this->condition.lhsAttr);
            switch (this->leftAttribute.type){
                case TypeInt:
                    int intKey;
                    memcpy(&intKey, key, sizeof(int));
                    if (this->intMap.find(intKey) == this->intMap.end()){
                        this->intMap[intKey] = std::vector<std::vector<char>>{std::vector<char>((char*)record, (char*)record + singleSize)};
                    } else {
                        this->intMap.at(intKey).push_back(std::vector<char>((char*)record, (char*)record + singleSize));
                    }
                    break;
                case TypeReal:
                    float floatKey;
                    memcpy(&floatKey, key, sizeof(int));
                    if (this->floatMap.find(floatKey) == this->floatMap.end()){
                        this->floatMap[floatKey] = std::vector<std::vector<char>>{std::vector<char>((char*)record, (char*)record+ singleSize)};
                    } else {
                        this->floatMap.at(floatKey).push_back(std::vector<char>((char*)record, (char*)record + singleSize));
                    }
                    break;
                case TypeVarChar:
                    int strLength;
                    memcpy(&strLength, key, sizeof(int));
                    std::string strKey= std::string((char*)key+sizeof(unsigned),strLength);
                    if (this->varcharMap.find(std::string(strKey)) == this->varcharMap.end()){
                        this->varcharMap[std::string(strKey)] = std::vector<std::vector<char>>{std::vector<char>((char*)record, (char*)record + singleSize)};
                    } else {
                        this->varcharMap.at(std::string(strKey)).push_back(std::vector<char>((char*)record, (char*)record + singleSize));
                    }
                    break;
            }
            free(tuple);
            free(record);
            free(key);
        }

    }

    int BNLJoin::checkRecordSize(const void *record) {
        int slotNum;
        memcpy(&slotNum,record,sizeof(unsigned));
        int lastEnd;
        memcpy(&lastEnd,(char*)record+slotNum*sizeof(unsigned),sizeof(unsigned));
        return lastEnd;
    }

    int BNLJoin::correspondingVectorSize(const void *key, AttrType attrType) {
        int vectorSize = 0;
        int intKey = 0;
        int strLength = 0;
        float floatKey = 0;
        std::string strKey;
        //get the size of the match tuples in the map
        switch (attrType){
            case TypeInt:
                memcpy(&intKey, key, sizeof(int));
                vectorSize = (this->intMap.find(intKey) == this->intMap.end()) ? 0 : this->intMap.at(intKey).size();
                break;
            case TypeReal:
                memcpy(&floatKey, key, sizeof(int));
                vectorSize = (this->floatMap.find(floatKey) == this->floatMap.end()) ? 0 : this->floatMap.at(floatKey).size();
                break;
            case TypeVarChar:
                memcpy(&strLength, key, sizeof(int));
                strKey=std::string((char*)key+sizeof(unsigned), strLength);
                vectorSize = (this->varcharMap.find(std::string(strKey)) == this->varcharMap.end()) ? 0 : this->varcharMap.at(std::string(strKey)).size();
                break;
        }
        return vectorSize;
    }

    void BNLJoin::BNLGiveResult(const void* key,void *result) {
        char* leftCharPointer;
        switch (this->leftAttribute.type){
            case TypeInt:
                int intKey;
                memcpy(&intKey,key,sizeof(unsigned));
                leftCharPointer = &this->intMap.at(intKey).at(this->pointer)[0];
                break;
            case TypeReal:
                float floatKey;
                memcpy(&floatKey,key,sizeof(unsigned));
                leftCharPointer = &this->floatMap.at(floatKey).at(this->pointer)[0];
                break;
            case TypeVarChar:
                int strLength;
                memcpy(&strLength,key,sizeof(unsigned));
                std::string strKey = std::string((char*)key+sizeof(unsigned), strLength);
                leftCharPointer = &this->varcharMap.at(std::string(strKey)).at(this->pointer)[0];
                break;
        }
        char* LeftData= (char*)malloc(PAGE_SIZE);
        RecordBasedFileManager::instance().deRecord(leftCharPointer,LeftData,this->leftAttrs);
        // double check if it matches, maybe useless
        if (MyJoin::isSatisfy(LeftData, this->currentRightData, this->leftAttrs, this->rightAttrs, this->condition)){
            MyJoin::join((char*)result, LeftData, this->currentRightData, this->leftAttrs, this->rightAttrs);
        }
        free(LeftData);
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
        this->currentLeftData=(char*)malloc(PAGE_SIZE);
        this->leftIn = leftIn;
        this->rightIn = rightIn;
        this->condition = condition;
        this->leftIn->getAttributes(this->leftAttrs);
        this->rightIn->getAttributes(this->rightAttrs);
        this->currentLeftStatus = this->leftIn->getNextTuple(this->currentLeftData);
    }

    INLJoin::~INLJoin() {
        free(this->currentLeftData);
    }

    RC INLJoin::getNextTuple(void *data) {
        char* rightData = (char*) malloc(PAGE_SIZE);
        while (this->currentLeftStatus != QE_EOF){
            while(this->rightIn->getNextTuple(rightData) != QE_EOF){
                if(MyJoin::isSatisfy(this->currentLeftData, rightData, this->leftAttrs, this->rightAttrs, this->condition)){
                    MyJoin::join((char*) data, this->currentLeftData, rightData, this->leftAttrs, this->rightAttrs);
                    free(rightData);
                    return 0;
                }
            }
            this->currentLeftStatus = this->leftIn->getNextTuple(this->currentLeftData);
            if (this->currentLeftStatus != QE_EOF){
                char* rightKey = (char*) malloc(PAGE_SIZE);
                MyJoin::findKey(rightKey, this->currentLeftData, this->leftAttrs, this->condition.lhsAttr);
                this->rightIn->setIterator(rightKey, rightKey, true, true);
                free(rightKey);
            }
        }
        free(rightData);
        return QE_EOF;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs.insert(attrs.end(), this->leftAttrs.begin(), this->leftAttrs.end());
        attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
        return 0;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
        this->single = true;
        this->finished = false;
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        setSum(this->singleUnit,this->op);
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
        this->single = false;
        this->finished = false;
        this->groupCount = 0;
        this->input = input;
        this->aggAttr = aggAttr;
        this->groupAttr = groupAttr;
        this->op =op;
        this->floatUnitHash.clear();
        this->intUnitHash.clear();
        this->stringUnitHash.clear();
        groupScan();
    }

    RC Aggregate::getNextTuple(void *data) {
        if (this->finished) return QE_EOF;
        if(!this->single) {
            if (this->groupAttr.type == TypeInt) {
                if (this->groupCount >= this->intUnitHash.size())return QE_EOF;
                int count = 0;
                auto iter = this->intUnitHash.begin();
                for (; count <= this->groupCount; iter++, count++) {
                    if (count == this->groupCount) {
                        Value tempValue;
                        tempValue.type=TypeInt;
                        tempValue.data= malloc(PAGE_SIZE);
                        memcpy(tempValue.data,&iter->first,sizeof(unsigned));
                        giveResult(data, iter->second, tempValue,this->op, this->aggAttr.type);
                        free(tempValue.data);
                        this->groupCount++;
                        return 0;
                    }
                }

            } else if (this->groupAttr.type == TypeReal) {
                if (this->groupCount >= this->floatUnitHash.size())return QE_EOF;
                int count = 0;
                auto iter = this->floatUnitHash.begin();
                for (; count <= this->groupCount; iter++, count++) {
                    if (count == this->groupCount) {
                        Value  tempValue;
                        tempValue.type= TypeReal;
                        tempValue.data= malloc(PAGE_SIZE);
                        memcpy(tempValue.data,&iter->first,sizeof(unsigned));
                        giveResult(data, iter->second, tempValue,this->op, this->aggAttr.type);
                        free(tempValue.data);
                        this->groupCount++;
                        return 0;
                    }
                }

            } else {
                if (this->groupCount >= this->stringUnitHash.size())return QE_EOF;
                int count = 0;
                auto iter = this->stringUnitHash.begin();
                for (; count <= this->groupCount; iter++, count++) {
                    if (count == this->groupCount) {
                        Value tempValue;
                        tempValue.type=TypeVarChar;
                        tempValue.data= malloc(PAGE_SIZE);
                        int Size=iter->first.size();
                        memcpy(tempValue.data,&Size,sizeof(unsigned));
                        memcpy((char*)tempValue.data+sizeof(unsigned),iter->first.c_str(),Size);
                        giveResult(data, iter->second, tempValue, this->op, this->aggAttr.type);
                        free(tempValue.data);
                        this->groupCount++;
                        return 0;
                    }
                }
            }

        }
        std::vector<Attribute> tableAttrs;
        input->getAttributes(tableAttrs);
        char* tuple = (char*) malloc(PAGE_SIZE);
        char* key = (char*) malloc(PAGE_SIZE);
        while(input->getNextTuple(tuple) != QE_EOF){
            MyJoin::findKey(key, tuple, tableAttrs, this->aggAttr.name);
            if(this->single){
                aggreCal(key,this->aggAttr.type,this->singleUnit,this->op);
            }
        }
        if(this->single){
            giveResult(data,this->singleUnit,this->op,this->aggAttr.type);
        }
        free(tuple);
        free(key);
        this->finished = true;
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if(!this->single){
            attrs.push_back(this->groupAttr);
        }
        std::string name;
        Attribute attr;
        switch(this->op){
            case MIN:
                name = "MIN";
                attr.type = this->aggAttr.type;
                break;
            case MAX:
                name = "MAX";
                attr.type = this->aggAttr.type;
                break;
            case COUNT:
                name = "COUNT";
                attr.type = TypeInt;
                break;
            case SUM:
                name = "SUM";
                attr.type = this->aggAttr.type;
                break;
            case AVG:
                name = "AVG";
                attr.type = TypeReal;
                break;
        }
        std::string attrName = name + "(" + this->aggAttr.name + ")";
        attr.name = attrName;
        attr.length = this->aggAttr.length;
        attrs.push_back(attr);
        return 0;
    }

    void Aggregate::aggreCal(const char* key, AttrType attrType,Unit & unit, const AggregateOp op) {
        unit.count++;
        int intKey;
        float floatKey;
        switch(op){
            case MAX:
                if (attrType == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    unit.sum= std::max(unit.sum, (float)intKey);
                } else if (attrType== TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    unit.sum = std::max(unit.sum,floatKey);
                }
                break;
            case MIN:
                if (attrType == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    unit.sum= std::min(unit.sum, (float)intKey);
                } else if (attrType == TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    unit.sum = std::min(unit.sum, floatKey);
                }
                break;
            case COUNT:
                // does nothing here because counter is incremented anyway
                break;
            case AVG:
                //then continue with the sum
            case SUM:
                if (attrType == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    unit.sum+= (float)intKey;
                } else if (attrType == TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    unit.sum+= unit.sum, floatKey;
                }
                break;
        }

    }

    void Aggregate::giveResult(void* result,const Unit &unit, const AggregateOp op, AttrType attrType) {
        char nullByte[1] = {0};
        if (unit.count == 0){
            RecordBasedFileManager::instance().changeBit(nullByte,0);
        }
        memcpy(result, nullByte, sizeof(char));
        float floatTemp;
        int intTemp;
        switch(op){
            case MAX:
            case MIN:
            case SUM:
                if(attrType==TypeInt){
                    intTemp= (int)unit.sum;
                    memcpy((char*)result+ sizeof(char), &intTemp, sizeof(int));
                }
                else{
                    floatTemp=unit.sum;
                    memcpy((char*)result+sizeof(char),&floatTemp,sizeof(float));
                }
                break;
            case COUNT:
                memcpy((char*)result+ sizeof(char), &unit.count, sizeof(int));
                break;
            case AVG:
                floatTemp= (unit.sum == 0 ? 0 : unit.sum / (float)unit.count);
                memcpy((char*)result + sizeof(char), &floatTemp, sizeof(float));
                break;
        }
    }

    void Aggregate::groupScan() {
        std::vector<Attribute> tableAttrs;
        input->getAttributes(tableAttrs);
        char* tuple =(char*)malloc(PAGE_SIZE);
        char* aggKey = (char*)malloc(PAGE_SIZE);
        char* groupKey = (char*)malloc(PAGE_SIZE);
        while(input->getNextTuple(tuple)!=QE_EOF){
            MyJoin::findKey(aggKey,tuple,tableAttrs,this->aggAttr.name);
            MyJoin::findKey(groupKey,tuple,tableAttrs,this->groupAttr.name);
            if(!this->single){
                switch(this->groupAttr.type){
                    case TypeInt:
                        int Int;
                        prepareHash(groupKey,this->intUnitHash,this->stringUnitHash,this->groupAttr.type,this->op);
                        memcpy(&Int,groupKey,sizeof(unsigned));
                        aggreCal(aggKey,this->aggAttr.type,intUnitHash[Int],this->op);
                        break;
                    case TypeReal:
                        float Real;
                        prepareHash(groupKey,this->floatUnitHash,this->stringUnitHash,this->groupAttr.type,this->op);
                        memcpy(&Real,groupKey,sizeof(unsigned));
                        aggreCal(aggKey,this->aggAttr.type,floatUnitHash[Real],this->op);
                        break;
                    case TypeVarChar:
                        int VarLen;
                        std ::string Varchar;
                        prepareHash(groupKey,this->floatUnitHash,this->stringUnitHash,this->groupAttr.type,this->op);
                        memcpy(&VarLen,groupKey,sizeof(unsigned));
                        Varchar=std::string((char*)groupKey+sizeof(unsigned),VarLen);
                        aggreCal(aggKey,this->aggAttr.type,stringUnitHash[Varchar],this->op);
                        break;
                }
            }
        }
        free(tuple);
        free(aggKey);
        free(groupKey);
    }

    void Aggregate::giveResult(void *result, const Unit &unit, const Value value, const AggregateOp op, AttrType attrType) {
        char nullByte[1] = {0};
        memcpy(result, nullByte, sizeof(char));
        int pivot =sizeof(char);
        switch(value.type){
            case TypeInt:
            case TypeReal:
                memcpy((char*)result+pivot,value.data,sizeof(unsigned));
                pivot+=sizeof(unsigned);
                break;
            case TypeVarChar:
                int VarLen;
                memcpy(&VarLen,value.data,sizeof(unsigned));
                memcpy((char*)result+pivot,value.data,sizeof(unsigned)+VarLen);
                pivot+=(VarLen+sizeof(unsigned));
                break;
        }
        float floatTemp;
        int intTemp;
        switch(op){
            case MAX:
            case MIN:
            case SUM:
                if(attrType==TypeInt){
                    intTemp= (int)unit.sum;
                    memcpy((char*)result+ pivot, &intTemp, sizeof(int));
                }
                else{
                    floatTemp=unit.sum;
                    memcpy((char*)result+pivot,&floatTemp,sizeof(float));
                }
                break;
            case COUNT:
                memcpy((char*)result+ pivot, &unit.count, sizeof(int));
                break;
            case AVG:
                floatTemp= (unit.sum == 0 ? 0 : unit.sum / (float)unit.count);
                memcpy((char*)result +pivot, &floatTemp, sizeof(float));
                break;
        }
    }

    template<typename T>
    void Aggregate::prepareHash(const void* key,std::map<T, Unit> &TUnitHash, std::map<std::string, Unit> &stringUnitHash,
                                AttrType groupAttrType,AggregateOp op) {
        T Real;
        std::string Varchar;
        switch(groupAttrType){
            case TypeInt:
            case TypeReal:
                memcpy(&Real,key,sizeof(unsigned));
                if(TUnitHash.find(Real)==TUnitHash.end()){
                    TUnitHash[Real]=Unit();
                    setSum(TUnitHash[Real],op);
                }
                break;
            case TypeVarChar:
                int VarLen;
                memcpy(&VarLen,key,sizeof(unsigned));
                Varchar=std::string((char*)key+sizeof(unsigned),VarLen);
                if(stringUnitHash.find(Varchar)==stringUnitHash.end()){
                    stringUnitHash[Varchar]=Unit();
                    setSum(stringUnitHash[Varchar],op);
                }
                break;
        }
    }

    bool MyJoin::isSatisfy(void *leftData, void *rightData, const std::vector<Attribute> &leftAttrs,
                           const std::vector<Attribute> &rightAttrs, const Condition &cond) {
        char* leftKey = (char*) calloc(PAGE_SIZE, sizeof(char));
        char* rightKey = (char*) calloc(PAGE_SIZE, sizeof(char));
        if(cond.bRhsIsAttr == true){
            AttrType attrtype = MyJoin::findKey(leftKey, leftData, leftAttrs, cond.lhsAttr);
            MyJoin::findKey(rightKey, rightData, rightAttrs, cond.rhsAttr);
            bool result = MyJoin::isCompareSatisfy(leftKey, rightKey, cond.op, attrtype);
            free(leftKey);
            free(rightKey);
            return result;
        }
        free(leftKey);
        free(rightKey);
        return false;
    }

    AttrType MyJoin::findKey(void *key, void *data, const std::vector<Attribute> &attrs, const std::string name) {
        int pivot = ceil((double)attrs.size() / 8);
        char* nullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
        memcpy(nullBytes, data, pivot);
        //assuming always exists
        for(int i = 0 ; i < attrs.size(); i++){
            if(attrs[i].name == name) {
                if(!RecordBasedFileManager::instance().checkNull(i, nullBytes)){
                    if(attrs[i].type == TypeVarChar){
                        int strLength;
                        memcpy(&strLength, (char*)data + pivot, sizeof(int));
                        memcpy(key, (char*)data + pivot, strLength + sizeof(int));
                    }
                    else{
                        memcpy(key, (char*)data + pivot, sizeof(int));
                    }
                    free(nullBytes);
                    return attrs[i].type;

                }
            }
            if (RecordBasedFileManager::instance().checkNull(i, nullBytes)){
                continue;
            }
            if(attrs[i].type == TypeVarChar){
                int strLength;
                memcpy(&strLength, (char*)data + pivot, sizeof(int));
                pivot += (sizeof(int) + strLength);
            }
            else{
                pivot += sizeof(int);
            }
        }
        return TypeVacant;
    }

    bool MyJoin::isCompareSatisfy(void *leftData, void *rightData, CompOp op, AttrType attrType) {
        if(op==NO_OP)return 0;
        int givenNum,mineNum;
        float givenReal,mineReal;
        int givenVarLen = 0;
        int mineVarLen = 0;
        std::string givenVar;
        std::string mineVar;
        switch(attrType){
            case TypeInt:
                memcpy(&mineNum,(char*)leftData,sizeof(unsigned));
                memcpy(&givenNum,(char*)rightData,sizeof(unsigned));
                break;
            case TypeReal:
                memcpy(&mineReal,(char*)leftData,sizeof(unsigned));
                memcpy(&givenReal,(char*)rightData,sizeof(unsigned));
                break;
            case TypeVarChar:
                memcpy(&mineVarLen, leftData, sizeof(int));
                memcpy(&givenVarLen, rightData, sizeof(int));
                mineVar = std::string((char*) leftData + sizeof(int), givenVarLen);
                givenVar = std::string((char*)rightData+ sizeof(int), mineVarLen);
                break;
        }
        switch(op){
            case EQ_OP:
                if(attrType==TypeInt){
                    if(givenNum==mineNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
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
                if(attrType==TypeInt){
                    if(mineNum<givenNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
                    if(mineReal<givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<givenVar;
                }
                break;
            case LE_OP:
                if(attrType==TypeInt){
                    if(mineNum<=givenNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
                    if(mineReal<=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar<=givenVar;
                }
                break;
            case GT_OP:
                if(attrType==TypeInt){
                    if(mineNum>givenNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
                    if(mineReal>givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>givenVar;
                }
                break;
            case GE_OP:
                if(attrType==TypeInt){
                    if(mineNum>=givenNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
                    if(mineReal>=givenReal)return 1;
                    else return 0;
                }
                else{
                    return mineVar>=givenVar;
                }
                break;
            case NE_OP:
                if(attrType==TypeInt){
                    if(mineNum!=givenNum)return 1;
                    else return 0;
                }
                else if(attrType==TypeReal){
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

    void MyJoin::join(char *result, char *leftData, char *rightData, const std::vector<Attribute> &leftAttrs,
                      const std::vector<Attribute> &rightAttrs) {
        int leftAttrsNum = leftAttrs.size();
        int rightAttrsNum = rightAttrs.size();
        void* newRecord = malloc(PAGE_SIZE);
        void* leftRecord = malloc(PAGE_SIZE);
        void* rightRecord = malloc(PAGE_SIZE);
        RecordBasedFileManager::instance().toRecord(leftData,leftRecord,leftAttrs);
        RecordBasedFileManager::instance().toRecord(rightData,rightRecord,rightAttrs);
        int newAttrsNum = leftAttrsNum + rightAttrsNum;
        memcpy(newRecord,&newAttrsNum,sizeof(unsigned));
        int newPivot = (newAttrsNum + 1) * sizeof(unsigned);
        for( int i=0;i<leftAttrsNum;i++){
            int end,start;
            if(i==0){
                start = (leftAttrsNum + 1)*sizeof(unsigned);
            }
            else{
                memcpy(&start,(char*)leftRecord+i*sizeof(unsigned),sizeof(unsigned));
            }
            memcpy(&end,(char*)leftRecord+(i+1)*sizeof(unsigned),sizeof(unsigned));
            memcpy((char*)newRecord+newPivot,(char*)leftRecord+start,end-start);
            newPivot+=(end-start);
            memcpy((char*)newRecord+(i+1)*sizeof(unsigned),&newPivot,sizeof(unsigned));
        }
        for( int i=0;i<rightAttrsNum;i++){
            int end,start;
            if(i==0){
                start = (rightAttrsNum + 1)*sizeof(unsigned);
            }
            else{
                memcpy(&start,(char*)rightRecord+i*sizeof(unsigned),sizeof(unsigned));
            }
            memcpy(&end,(char*)rightRecord+(i+1)*sizeof(unsigned),sizeof(unsigned));
            memcpy((char*)newRecord+newPivot,(char*)rightRecord+start,end-start);
            newPivot+=(end-start);
            memcpy((char*)newRecord+(i+1+leftAttrsNum)*sizeof(unsigned),&newPivot,sizeof(unsigned));
        }
        std::vector<Attribute> newAttrs;
        newAttrs.insert(newAttrs.end(),leftAttrs.begin(),leftAttrs.end());
        newAttrs.insert(newAttrs.end(),rightAttrs.begin(),rightAttrs.end());

        RecordBasedFileManager::instance().deRecord(newRecord,result,newAttrs);
        free(newRecord);
        free(leftRecord);
        free(rightRecord);
    }
} // namespace PeterDB
