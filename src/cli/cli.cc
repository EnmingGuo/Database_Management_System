#include "src/include/cli.h"

#include <readline/readline.h>
#include <readline/history.h>
#include <fstream>

// Command parsing delimiters
// TODO: update delimiters later
#define DELIMITERS " ,()\""

// CVS file read delimiters
#define CVS_DELIMITERS ","
#define CLI_TABLES "cli_tables"
#define CLI_COLUMNS "cli_columns"
#define CLI_INDEXES "cli_indexes"
#define COLUMNS_TABLE_RECORD_MAX_LENGTH 150   // It is actually 112
#define DIVISOR "  |  "
#define DIVISOR_LENGTH 5
#define EXIT_CODE -99

#define DATABASE_FOLDER "./"
// DATABASE_FOLDER is given by makefile.inc file.
// If your compiler complains about DATABASE_FOLDER, explicitly define DATABASE_FOLDER here
// #define DATABASE_FOLDER "~/Users/yourpath..."

namespace PeterDB {
    static int getActualByteForNullsIndicator(int fieldCount) {

        return ceil((double) fieldCount / CHAR_BIT);
    }

    CLI *CLI::_cli = nullptr;

    CLI *CLI::Instance() {
        if (!_cli)
            _cli = new CLI();

        return _cli;
    }

    CLI::CLI() = default;

    CLI::~CLI() = default;

    RC CLI::start() {

        // what do we want from readline?
        using_history();
        // auto-complete = TAB
        rl_bind_key('\t', rl_complete);

        char *input, shell_prompt[100];
        std::cout << "************************" << std::endl;
        std::cout << "SecSQL CLI started" << std::endl;
        std::cout << "Enjoy!" << std::endl;
        for (;;) {

            // Create prompt string from user name and current working directory.
            //snprintf(shell_prompt, sizeof(shell_prompt), "%s >>> ", getenv("USER"));
            snprintf(shell_prompt, sizeof(shell_prompt), ">>> ");
            // Display prompt and read input (n.b. input must be freed after use)...
            input = readline(shell_prompt);

            // check for EOF
            if (!input)
                break;
            if ((this->process(std::string(input))) == EXIT_CODE) {
                free(input);
                break;
            }
            add_history(input);
            // Free Input
            free(input);
        }
        std::cout << "Goodbye :(" << std::endl;

        return 0;
    }

    RC CLI::process(const std::string& input) {
        // convert input to char *
        RC code = 0;
        char *a = new char[input.size() + 1];
        a[input.size()] = 0;
        memcpy(a, input.c_str(), input.size());

        // tokenize input
        char *tokenizer = strtok(a, DELIMITERS);
        if (tokenizer != NULL) {

            ////////////////////////////////////////////
            // create table <tableName> (col1=type1, col2=type2, ...)
            // create index <columnName> on <tableName>
            // create catalog
            ////////////////////////////////////////////
            if (expect(tokenizer, "create")) {
                tokenizer = next();
                if (tokenizer == NULL) {
                    code = error("I expect <table> or <index>");
                } else {
                    std::string type = std::string(tokenizer);

                    if (type.compare("table") == 0) // if type equals table, then create table
                        code = createTable();
                    else if (type.compare("index") == 0) // else if type equals index, then create index
                        code = createIndex();
                    else if (type.compare("catalog") == 0) // else if type equals catalog, then create the catalog
                        code = createCatalog();
                }
            }
                ////////////////////////////////////////////
                // add attribute <attributeName=type> to <tableName>
                ////////////////////////////////////////////
            else if (expect(tokenizer, "add")) {
                tokenizer = next();
                if (expect(tokenizer, "attribute"))
                    code = addAttribute();
                else
                    code = error("I expect <attribute>");
            }

                ////////////////////////////////////////////
                // drop table <tableName>
                // drop index <columnName> on <tableName>
                // drop attribute <attributeName> from <tableName>
                // drop catalog
                ////////////////////////////////////////////
            else if (expect(tokenizer, "drop")) {
                tokenizer = next();
                if (expect(tokenizer, "table")) {
                    code = dropTable();
                } else if (expect(tokenizer, "index")) {
                    code = dropIndex();
                } else if (expect(tokenizer, "attribute")) {
                    code = dropAttribute();
                } else if (expect(tokenizer, "catalog")) {
                    code = dropCatalog();
                } else
                    code = error("I expect <tableName>, <indexName>, <attribute>");
            }

                ////////////////////////////////////////////
                // load <tableName> <fileName>
                // drop index <indexName>
                // drop attribute <attributeName> from <tableName>
                ////////////////////////////////////////////
            else if (expect(tokenizer, "load")) {
                code = load();
            }

                ////////////////////////////////////////////
                // print <tableName>
                // print attributes <tableName>
                ////////////////////////////////////////////
            else if (expect(tokenizer, "print")) {
                tokenizer = next();
                if (expect(tokenizer, "body") || expect(tokenizer, "attributes"))
                    code = printAttributes();
                else if (expect(tokenizer, "index"))
                    code = printIndex();
                else if (tokenizer != NULL)
                    code = printTable(std::string(tokenizer));
                else
                    code = error("I expect <tableName>");
            }

                ///////////////////////////////////////////////////////////////
                // insert into <tableName> tuple(attr1=val1, attr2=value2, ...)
                ///////////////////////////////////////////////////////////////
            else if (expect(tokenizer, "insert")) {
                code = insertTuple();
            }

                ////////////////////////////////////////////
                // help
                // help <commandName>
                ////////////////////////////////////////////
            else if (expect(tokenizer, "help")) {
                tokenizer = next();
                if (tokenizer != NULL)
                    code = help(std::string(tokenizer));
                else
                    code = help("all");
            } else if (expect(tokenizer, "quit") || expect(tokenizer, "exit") ||
                       expect(tokenizer, "q") || expect(tokenizer, "e")) {
                code = EXIT_CODE;
            } else if (expect(tokenizer, "history") || expect(tokenizer, "h")) {
                code = history();
            }

                ////////////////////////////////////////////
                // select...
                ////////////////////////////////////////////
            else if (expect(tokenizer, "SELECT")) {
                Iterator *it = NULL;
                code = run(query(it));
                std::cout << std::endl;
            }

                ////////////////////////////////////////////
                // Utopia...
                ////////////////////////////////////////////
            else if (expect(tokenizer, "make")) {
                code = error("this is for you Sky...");
            } else {
                code = error("i have no idea about this command, sorry");
            }
        }
        delete[] a;
        return code;
    }

    // query
    Iterator *CLI::query(Iterator *previous, int code) {
        Iterator *it = NULL;
        if (code >= 0 || (code != -2 && isIterator(std::string(next()), code))) {
            switch (code) {
                case FILTER:
                    it = filter(previous);
                    break;

                case PROJECT:
                    it = projection(previous);
                    break;

                case AGG:
                    it = aggregate(previous);
                    break;

                case BNL_JOIN:
                    it = blockNestedLoopJoin(previous);
                    break;

                case INL_JOIN:
                    it = indexNestedLoopJoin(previous);
                    break;

                case GH_JOIN:
                    it = graceHashJoin(previous);
                    break;

                case IDX_SCAN:
                    it = createBaseScanner("IDXSCAN");
                    break;

                case TBL_SCAN:
                    it = createBaseScanner(std::string(next()));
                    break;

                case -1:
                    error("dude, be careful with what you are writing as a query");
                    break;
            }
        }
        return it;
    }

    // Create INLJoin
    Iterator *CLI::indexNestedLoopJoin(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code)) {
            input = query(input, code);
        }

        if (input == NULL) {
            input = createBaseScanner(std::string(token));
        }

        // get right table
        token = next();
        std::string rightTableName = std::string(token);
        token = next(); // eat WHERE

        // parse the join condition
        Condition cond;
        if (createCondition(getTableName(input), cond, true, rightTableName) != 0)
            error(__LINE__);

        auto *right = new IndexScan(rm, rightTableName, cond.rhsAttr);

        // Create Join
        auto *join = new INLJoin(input, right, cond);

        return join;
    }

    // Create Aggregate
    Iterator *CLI::aggregate(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code)) {
            input = query(input, code);
        }

        if (input == NULL) {
            input = createBaseScanner(std::string(token));
        }

        token = next();

        // check GROUPBY
        bool groupby = false;
        Attribute gAttr;
        if (std::string(token) == "GROUPBY") {
            groupby = true;
            if (createAttribute(input, gAttr) != 0)
                error("CLI: " + __LINE__);

            token = next(); // eat GET
        }

        std::string operation = std::string(next());

        AggregateOp op;
        if (createAggregateOp(operation, op) != 0)
            error("CLI: " + __LINE__);
        Attribute aggAttr;
        if (createAttribute(input, aggAttr) != 0)
            error("CLI: " + __LINE__);

        Aggregate *agg;
        if (groupby) {
            agg = new Aggregate(input, aggAttr, gAttr, op);
        } else {
            agg = new Aggregate(input, aggAttr, op);
        }
        return agg;
    }

    // Create BNLJoin
    Iterator *CLI::blockNestedLoopJoin(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code)) {
            input = query(input, code);
        }

        if (input == NULL) {
            input = createBaseScanner(std::string(token));
        }

        // get right table
        token = next();
        std::string rightTableName = std::string(token);
        auto *right = new TableScan(rm, rightTableName);

        token = next(); // eat WHERE

        // parse the join condition
        Condition cond;
        if (createCondition(getTableName(input), cond, true, rightTableName) != 0)
            error(__LINE__);

        token = next(); // eat RECORDS
        token = next(); // get the number of pages

        // Create Join
        auto *join = new BNLJoin(input, right, cond, (unsigned) atoi(std::string(token).c_str()));

        return join;
    }

    // Create GHJoin
    Iterator *CLI::graceHashJoin(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code)) {
            input = query(input, code);
        }

        if (input == NULL) {
            input = createBaseScanner(std::string(token));
        }

        // get right table
        token = next();
        std::string rightTableName = std::string(token);
        auto *right = new TableScan(rm, rightTableName);

        token = next(); // eat WHERE

        // parse the join condition
        Condition cond;
        if (createCondition(getTableName(input), cond, true, rightTableName) != 0)
            error(__LINE__);

        token = next(); // eat PARTITIONS
        token = next(); // get partitions number

        // Create Join
        auto *join = new GHJoin(input, right, cond, (unsigned) atoi(std::string(token).c_str()));

        return join;
    }

    // Create Filter
    Iterator *CLI::filter(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code)) {
            input = query(input, code);
        }

        if (input == NULL) {
            input = createBaseScanner(std::string(token));
        }

        token = next(); // eat WHERE

        // parse the filter condition
        Condition cond;
        if (createCondition(getTableName(input), cond) != 0)
            error(__LINE__);

        // Create Filter
        auto *filter = new Filter(input, cond);

        return filter;
    }

// Create Projector
    Iterator *CLI::projection(Iterator *input) {
        char *token = next();
        int code = -2;
        if (isIterator(std::string(token), code))
            input = query(input, code);

        if (input == NULL)
            input = createBaseScanner(std::string(token));

        token = next(); // eat GET
        token = next(); // eat [

        // parse the projection attributes
        std::vector <std::string> attrNames;
        while (true) {
            token = next();
            if (std::string(token) == "]")
                break;
            attrNames.emplace_back(token);
        }


        // if we have "*", convert it to all attributes
        if (attrNames.at(0) == "*") {
            std::vector <Attribute> attrs;
            input->getAttributes(attrs);
            attrNames.clear();
            for (auto & attr : attrs) {
                attrNames.push_back(attr.name);
            }
        } else {
            std::string tableName = getTableName(input);
            addTableNameToAttrs(tableName, attrNames);
        }

        auto *project = new Project(input, attrNames);
        return project;
    }

    Iterator *CLI::createBaseScanner(const std::string& token) {
        // if token is "IDXSCAN" (index scanner), create index scanner
        if (token == "IDXSCAN") {
            std::string tableName = std::string(next());
            Condition cond;
            if (createCondition(tableName, cond) != 0)
                error(__LINE__);

            auto *is = new IndexScan(rm, tableName, cond.lhsAttr);

            switch (cond.op) {
                case EQ_OP:
                    is->setIterator(cond.rhsValue.data, cond.rhsValue.data, true, true);
                    break;

                case LT_OP:
                    is->setIterator(NULL, cond.rhsValue.data, true, false);
                    break;

                case GT_OP:
                    is->setIterator(cond.rhsValue.data, NULL, false, true);
                    break;

                case LE_OP:
                    is->setIterator(NULL, cond.rhsValue.data, true, true);
                    break;

                case GE_OP:
                    is->setIterator(cond.rhsValue.data, NULL, true, true);
                    break;

                case NO_OP:
                    is->setIterator(NULL, NULL, true, true);
                    break;

                default:
                    break;
            }

            return is;
        }
        // otherwise, create create table scanner
        return new TableScan(rm, token);
    }

    bool CLI::isIterator(const std::string token, int &code) {
        if (expect(token, "FILTER"))
            code = FILTER;
        else if (expect(token, "PROJECT"))
            code = PROJECT;
        else if (expect(token, "BNLJOIN"))
            code = BNL_JOIN;
        else if (expect(token, "INLJOIN"))
            code = INL_JOIN;
        else if (expect(token, "GHJOIN"))
            code = GH_JOIN;
        else if (expect(token, "AGG"))
            code = AGG;
        else if (expect(token, "IDXSCAN"))
            code = IDX_SCAN;
        else if (expect(token, "TBLSCAN"))
            code = TBL_SCAN;
        else
            return false;

        return true;
    }

    RC CLI::run(Iterator *it) {
        void *data = malloc(PAGE_SIZE);
        std::vector <Attribute> attrs;
        std::vector <std::string> outputBuffer;
        it->getAttributes(attrs);

        for (auto & attr : attrs)
            outputBuffer.push_back(attr.name);

        while (it->getNextTuple(data) != QE_EOF) {
            if (updateOutputBuffer(outputBuffer, data, attrs) != 0)
                return error(__LINE__);
        }

        if (printOutputBuffer(outputBuffer, attrs.size()) != 0)
            return error(__LINE__);
        return 0;
    }

    RC CLI::createProjectAttributes(const std::string& tableName, std::vector <Attribute> &attrs) {
        char *token = next();
        Attribute attr;
        std::vector <Attribute> inputAttrs;
        getAttributesFromCatalog(tableName, inputAttrs);
        while (expect(token, "FROM")) {
            // get the attribute
            if (getAttribute(std::string(token), inputAttrs, attr) != 0)
                return error("given " + std::string(token) + " is not found in attributes");
            attrs.push_back(attr);
            token = next();
        }
        return 0;
    }

    RC CLI::createCondition(const std::string& tableName, Condition &condition, const bool join, const std::string joinTable) {
        // get left attribute
        char *token = next();

        std::string attribute = std::string(token);
        // concatenate left attribute with tableName
        condition.lhsAttr = fullyQualify(attribute, tableName);

        // get operation
        token = next();
        if (std::string(token) == "=")
            condition.op = EQ_OP;
        else if (std::string(token) == "<")
            condition.op = LT_OP;
        else if (std::string(token) == ">")
            condition.op = GT_OP;
        else if (std::string(token) == "<=")
            condition.op = LE_OP;
        else if (std::string(token) == ">=")
            condition.op = GE_OP;
        else if (std::string(token) == "!=")
            condition.op = NE_OP;
        else if (std::string(token) == "NOOP") {
            condition.op = NO_OP;
            return 0;
        }

        if (join) {
            condition.bRhsIsAttr = true;
            token = next();
            condition.rhsAttr = fullyQualify(std::string(token), joinTable);
            return 0;
        }

        condition.bRhsIsAttr = false;

        // get attribute from catalog
        Attribute attr;
        if (this->getAttribute(tableName, attribute, attr) != 0)
            return error(__LINE__);

        Value value;
        value.type = attr.type;
        value.data = malloc(PAGE_SIZE);
        token = next();
        attribute = std::string(token);

        int num;
        float floatNum;
        switch (attr.type) {
            case TypeVarChar:
                num = attribute.size();
                memcpy((char *) value.data, &num, sizeof(int));
                memcpy((char *) value.data + sizeof(int), attribute.c_str(), num);
                break;
            case TypeInt:
                num = std::atoi(std::string(token).c_str());
                memcpy((char *) value.data, &num, sizeof(int));
                break;
            case TypeReal:
                floatNum = std::atof(std::string(token).c_str());
                memcpy((char *) value.data, &floatNum, sizeof(float));
                break;
            default:
                return error("cli: " + __LINE__);
        }
        condition.rhsValue = value;
        return 0;
    }

    RC CLI::createAttribute(Iterator *input, Attribute &attr) {
        std::string tableName = getTableName(input);
        std::string attribute = std::string(next());
        attribute = fullyQualify(attribute, tableName);

        std::vector <Attribute> attrs;
        input->getAttributes(attrs);
        // get attribute from catalog
        if (getAttribute(attribute, attrs, attr) != 0)
            return error(__LINE__);
        return 0;
    }

    RC CLI::createAggregateOp(const std::string& operation, AggregateOp &op) {
        if (operation == "MAX")
            op = MAX;
        else if (operation == "MIN")
            op = MIN;
        else if (operation == "SUM")
            op = SUM;
        else if (operation == "AVG")
            op = AVG;
        else if (operation == "COUNT")
            op = COUNT;
        else {
            return error("create aggregate op: " + __LINE__);
        }
        return 0;
    }

    std::string CLI::getTableName(Iterator *it) {
        std::vector <Attribute> attrs;
        it->getAttributes(attrs);
        unsigned loc = attrs.at(0).name.find(".", 0);
        return attrs.at(0).name.substr(0, loc);
    }

    // input is like tableName.attributeName
    // returns attributeName
    std::string CLI::getAttribute(const std::string& input) {
        unsigned loc = input.find(".", 0);
        return input.substr(loc + 1, input.size() - loc);
    }

    std::string CLI::fullyQualify(const std::string& attribute, const std::string& tableName) {
        unsigned loc = attribute.find(".", 0);
        if (loc >= 0 && loc < attribute.size())
            return attribute;
        else
            return tableName + "." + attribute;
    }

///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
// END OF QUERY ENGINE ////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
///////////////////////////////////
//===============================//

    RC CLI::createTable() {
        char *tokenizer = next();
        if (tokenizer == NULL) {
            return error("I expect <name> to be created");
        }
        std::string name = std::string(tokenizer);

        // parse columnNames and types
        std::vector <Attribute> table_attrs;
        Attribute attr;
        while (tokenizer != NULL) {
            // get name if there is
            tokenizer = next();
            if (tokenizer == NULL) {
                break;
            }
            attr.name = std::string(tokenizer);

            tokenizer = next(); // eat =

            // get type
            tokenizer = next();
            if (tokenizer == NULL) {
                return error("expecting type");
            }
            if (expect(tokenizer, "int")) {
                attr.type = TypeInt;
                attr.length = 4;
            } else if (expect(tokenizer, "real")) {
                attr.type = TypeReal;
                attr.length = 4;
            } else if (expect(tokenizer, "varchar")) {
                attr.type = TypeVarChar;
                // read length
                tokenizer = next();
                attr.length = std::atoi(tokenizer);
            } else {
                return error("problem in attribute type in create table: " + std::string(tokenizer));
            }
            table_attrs.push_back(attr);
        }
        // cout << "create table for <" << name << "> and attributes:" << endl;
        // for (std::vector<Attribute>::iterator it = table_attrs.begin() ; it != table_attrs.end(); ++it)
        //    std::cout << ' ' << it->length;
        //  cout << endl;

        RC ret = rm.createTable(name, table_attrs);
        if (ret != 0)
            return ret;

        // add table to cli catalogs
        std::string file_url = std::string(DATABASE_FOLDER) + '/' + name;
        ret = this->addTableToCatalog(name, file_url, "heap");
        if (ret != 0)
            return ret;

        // add attributes to cli columns table
        for (uint i = 0; i < table_attrs.size(); i++) {
            this->addAttributeToCatalog(table_attrs.at(i), name, i);
        }

        return 0;
    }

    // create index <columnName> on <tableName>
    RC CLI::createIndex() {
        char *tokenizer = next();
        std::string columnName = std::string(tokenizer);

        tokenizer = next();
        if (!expect(tokenizer, "on")) {
            return error("syntax error: expecting \"on\"");
        }

        tokenizer = next();
        std::string tableName = std::string(tokenizer);

        // check if columnName, tableName is valid
        RID rid;
        if (this->checkAttribute(tableName, columnName, rid) == false)
            return error("Given tableName-columnName does not exist");

        if (rm.createIndex(tableName, columnName) != 0) {
            return error("cannot create index on column(" + columnName + ") , ixManager error");
        }

        // add index to cli_indexes table
        if (this->addIndexToCatalog(tableName, columnName) != 0)
            return error("error in adding index to cli_indexes table");

        return 0;
    }

    // create the system catalog
    RC CLI::createCatalog() {

        if (rm.createCatalog() != 0) {
            return error("cannot create the system catalog.");
        }

        Attribute attr;

        // create cli columns table
        std::vector <Attribute> column_attrs;
        attr.name = "column_name";
        attr.type = TypeVarChar;
        attr.length = 30;
        column_attrs.push_back(attr);

        attr.name = "table_name";
        attr.type = TypeVarChar;
        attr.length = 50;
        column_attrs.push_back(attr);

        attr.name = "position";
        attr.type = TypeInt;
        attr.length = 4;
        column_attrs.push_back(attr);

        attr.name = "type";
        attr.type = TypeInt;
        attr.length = 4;
        column_attrs.push_back(attr);

        attr.name = "length";
        attr.type = TypeInt;
        attr.length = 4;
        column_attrs.push_back(attr);

        if (rm.createTable(CLI_COLUMNS, column_attrs) != 0)
            return error("cannot create " + std::string(CLI_COLUMNS) + " table.");

        // add cli catalog attributes to CLI_COLUMNS table
        for (uint i = 0; i < column_attrs.size(); i++) {
            this->addAttributeToCatalog(column_attrs.at(i), CLI_COLUMNS, i);
        }

        // create CLI_TABLES table
        std::vector <Attribute> table_attrs;
        attr.name = "table_name";
        attr.type = TypeVarChar;
        attr.length = 50;
        table_attrs.push_back(attr);

        attr.name = "file_location";
        attr.type = TypeVarChar;
        attr.length = 100;
        table_attrs.push_back(attr);

        attr.name = "type";
        attr.type = TypeVarChar;
        attr.length = 20;
        table_attrs.push_back(attr);

        if (rm.createTable(CLI_TABLES, table_attrs) != 0)
            return error("cannot create " + std::string(CLI_TABLES) + " table.");

        // add cli catalog attributes to cli columns table
        for (uint i = 0; i < table_attrs.size(); i++) {
            this->addAttributeToCatalog(table_attrs.at(i), CLI_TABLES, i);
        }

        // add cli catalog information to itself
        std::string file_url = std::string(DATABASE_FOLDER) + '/' + CLI_COLUMNS;
        if (this->addTableToCatalog(CLI_COLUMNS, file_url, "heap") != 0)
            return error("cannot add " + std::string(CLI_COLUMNS) + " to catalog.");

        file_url = std::string(DATABASE_FOLDER) + '/' + CLI_TABLES;
        if (this->addTableToCatalog(CLI_TABLES, file_url, "heap") != 0)
            return error("cannot add " + std::string(CLI_TABLES) + " to catalog.");

        // Adding the index table attributes to the columns table
        std::vector <Attribute> index_attr;
        attr.name = "table_name";
        attr.type = TypeVarChar;
        attr.length = 50;
        index_attr.push_back(attr);

        attr.name = "column_name";
        attr.length = 30;
        attr.type = TypeVarChar;
        index_attr.push_back(attr);

        attr.name = "max_key_length";
        attr.length = 4;
        attr.type = TypeInt;
        index_attr.push_back(attr);

        attr.name = "is_variable_length";
        attr.length = 4;
        attr.type = TypeInt;
        index_attr.push_back(attr);

        if (rm.createTable(CLI_INDEXES, index_attr) != 0)
            return error("cannot create " + std::string(CLI_INDEXES) + " table.");

        // add cli index attributes to cli columns table
        for (uint i = 0; i < index_attr.size(); i++) {
            this->addAttributeToCatalog(index_attr.at(i), CLI_INDEXES, i);
        }

        // add cli catalog information to itself
        file_url = std::string(DATABASE_FOLDER) + '/' + CLI_INDEXES;
        if (this->addTableToCatalog(CLI_INDEXES, file_url, "heap") != 0)
            return error("cannot add " + std::string(CLI_INDEXES) + " to catalog.");

        return 0;
    }

    RC CLI::addAttribute() {
        Attribute attr;
        char *tokenizer = next(); // attributeName
        attr.name = std::string(tokenizer);
        if (tokenizer == NULL)
            return error("I expect type for attribute");

        tokenizer = next(); // eat =

        tokenizer = next(); // type

        if (expect(tokenizer, "int")) {
            attr.type = TypeInt;
            attr.length = 4;
        } else if (expect(tokenizer, "real")) {
            attr.type = TypeReal;
            attr.length = 4;
        } else if (expect(tokenizer, "varchar")) {
            attr.type = TypeVarChar;
            // read length
            if (tokenizer == NULL)
                return error("I expect length for varchar");
            tokenizer = next();
            attr.length = atoi(tokenizer);
        }

        tokenizer = next();
        if (expect(tokenizer, "to") == false) {
            return error("expect to");
        }
        tokenizer = next(); //tableName
        std::string tableName = std::string(tokenizer);

        // add entry to CLI_COLUMNS
        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(CLI_COLUMNS, attributes);

        // Set up the iterator
        RM_ScanIterator rmsi;
        RID rid;
        void *data_returned = malloc(PAGE_SIZE);

        // convert attributes to vector<string>
        std::vector <std::string> stringAttributes;
        stringAttributes.emplace_back("position");

        // Delete columns
        int stringSize = tableName.size();
        void *value = malloc(stringSize + sizeof(unsigned));
        memcpy((char *) value, &stringSize, sizeof(unsigned));
        memcpy((char *) value + sizeof(unsigned), tableName.c_str(), stringSize);

        if (rm.scan(CLI_COLUMNS, "table_name", EQ_OP, value, stringAttributes, rmsi) != 0)
            return -1;

        int biggestPosition = 0, position = 0;
        while (rmsi.getNextTuple(rid, data_returned) != RM_EOF) {
            // adding +1 because of nulls-indicator
            memcpy(&position, (char *) data_returned + 1, sizeof(int));
            if (biggestPosition < (int) position)
                biggestPosition = position;
        }
        if (this->addAttributeToCatalog(attr, tableName, biggestPosition + 1) != 0)
            return -1;
        rmsi.close();
        free(value);
        free(data_returned);
        return rm.addAttribute(tableName, attr);
    }

    RC CLI::dropTable() {
        char *tokenizer = next();
        if (tokenizer == NULL)
            return error("I expect <tableName> to be dropped");

        std::string tableName = std::string(tokenizer);

        // delete indexes from cli_indexes table if there are
        RID rid;
        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(tableName, attributes);
        for (auto & attribute : attributes) {
            if (this->checkAttribute(tableName, attribute.name, rid, false)) {
                // delete the index from cli_indexes table
                if (rm.deleteTuple(CLI_INDEXES, rid) != 0)
                    return -1;
            }
        }



        // Set up the iterator
        Attribute attr;
        RM_ScanIterator rmsi;
        void *data_returned = malloc(PAGE_SIZE);

        // convert attributes to vector<string>
        std::vector <std::string> stringAttributes;
        stringAttributes.emplace_back("table_name");

        int stringSize = tableName.size();
        void *value = malloc(stringSize + sizeof(unsigned));
        memcpy((char *) value, &stringSize, sizeof(unsigned));
        memcpy((char *) value + sizeof(unsigned), tableName.c_str(), stringSize);

        if (rm.scan(CLI_TABLES, "table_name", EQ_OP, value, stringAttributes, rmsi) != 0)
            return -1;

        // delete tableName from CLI_TABLES
        while (rmsi.getNextTuple(rid, data_returned) != RM_EOF) {
            if (rm.deleteTuple(CLI_TABLES, rid) != 0)
                return -1;
        }
        rmsi.close();
        free(value);

        // Delete columns from CLI_COLUMNS

        stringSize = tableName.size();
        value = malloc(stringSize + sizeof(unsigned));
        memcpy((char *) value, &stringSize, sizeof(unsigned));
        memcpy((char *) value + sizeof(unsigned), tableName.c_str(), stringSize);

        if (rm.scan(CLI_COLUMNS, "table_name", EQ_OP, value, stringAttributes, rmsi) != 0)
            return -1;


        // We rely on the fact that RM_EOF is not 0.
        // we want to return -1 when getNext tuple errors
        RC ret = -10;
        while ((ret = rmsi.getNextTuple(rid, data_returned)) == 0) {
            if (rm.deleteTuple(CLI_COLUMNS, rid) != 0)
                return -1;
        }
        rmsi.close();
        free(value);

        if (ret != RM_EOF)
            return -1;

        free(data_returned);

        // and finally DeleteTable
        ret = rm.deleteTable(tableName);
        if (ret != 0)
            return error("error in deleting table in recordManager");

        return 0;
    }

    // drop index <columnName> on <tableName>
    RC CLI::dropIndex(const std::string& tableName, const std::string& columnName, bool fromCommand) {
        std::string realTable;
        std::string realColumn;
        if (fromCommand == false) {
            realTable = tableName;
            realColumn = columnName;
        } else {
            // parse willDelete from command line
            char *tokenizer = next();
            realColumn = std::string(tokenizer);

            tokenizer = next();
            if (!expect(tokenizer, "on")) {
                return error("syntax error: expecting \"on\"");
            }

            tokenizer = next();
            realTable = std::string(tokenizer);
        }
        RC rc;
        // check if index is there or not
        RID rid;
        if (!this->checkAttribute(realTable, realColumn, rid, false)) {
            if (fromCommand)
                return error("given " + realTable + ":" + realColumn + " index does not exist in cli_indexes");
            else // return error but print nothing
                return -1;
        }

        // drop the index
        rc = rm.destroyIndex(realTable, realColumn);
        if (rc != 0)
            return error("error while destroying index in ixManager");

        // delete the index from cli_indexes table
        rc = rm.deleteTuple(CLI_INDEXES, rid) != 0;

        return rc;
    }

    // drop the system catalog
    RC CLI::dropCatalog() {
        if (rm.deleteCatalog() != 0) {
            return error("error while deleting the system catalog.");
        }

        return 0;
    }

    RC CLI::dropAttribute() {
        char *tokenizer = next(); // attributeName
        std::string attrName = std::string(tokenizer);
        tokenizer = next();
        if (expect(tokenizer, "from") == false) {
            return error("expect from");
        }
        tokenizer = next(); //tableName
        std::string tableName = std::string(tokenizer);

        RID rid;
        if (!this->checkAttribute(tableName, attrName, rid)) {
            return error("given tableName-attrName does not exist");
        }
        // delete entry from CLI_COLUMNS
        RC rc = rm.deleteTuple(CLI_COLUMNS, rid) != 0;
        if (rc != 0)
            return rc;

        // drop attribute
        rc = rm.dropAttribute(tableName, attrName);
        if (rc != 0)
            return rc;

        // if there is an index on dropped attribute
        //    delete the index from cli_indexes table
        bool hasIndex = this->checkAttribute(tableName, attrName, rid, false);

        if (hasIndex) {
            // delete the index from cli_indexes table
            rc = rm.deleteTuple(CLI_INDEXES, rid) != 0;
            if (rc != 0)
                return rc;
        }

        return 0;
    }

    // CSV reader without escaping commas
    // should be fixed
    // reads files in data folder
    RC CLI::load() {
        char *commandTokenizer = next();
        if (commandTokenizer == NULL)
            return error("I expect <tableName>");

        std::string tableName = std::string(commandTokenizer);
        commandTokenizer = next();
        if (commandTokenizer == NULL) {
            return error("I expect <fileName> to be loaded");
        }
        std::string fileName = std::string(commandTokenizer);

        // get attributes from catalog
        Attribute attr;
        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(tableName, attributes);
        uint offset = 0, index = 0, keyIndex = 0;
        uint length;
        void *buffer = malloc(PAGE_SIZE);
        void *key = malloc(PAGE_SIZE);
        RID rid;

        // find out if there is any index for tableName
        std::unordered_map<int, void *> indexMap;
        for (uint i = 0; i < attributes.size(); i++) {
            if (this->checkAttribute(tableName, attributes.at(i).name, rid, false))
                // add index to index-map
                indexMap[i] = malloc(PAGE_SIZE);
        }

        // read file
        std::ifstream ifs;
        std::string
        file_url = DATABASE_FOLDER
        "../data/" + fileName;
        ifs.open(file_url, std::ifstream::in);

        if (!ifs.is_open())
            return error("could not open file: " + file_url);

        // Assume that we don't have any NULL values when loading data.
        // Null-indicators
        int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributes.size());
        auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
        memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

        std::string line, token;
        char *tokenizer;
        while (ifs.good()) {
            getline(ifs, line);
            if (line == "")
                continue;
            char *a = new char[line.size() + 1];
            a[line.size()] = 0;
            memcpy(a, line.c_str(), line.size());
            index = 0, offset = 0;

            // Null-indicator for the fields
            memcpy((char *) buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
            offset += nullAttributesIndicatorActualSize;

            // tokenize input
            tokenizer = strtok(a, CVS_DELIMITERS);
            while (tokenizer != NULL) {
                attr = attributes.at(index++);
                token = std::string(tokenizer);
                if (attr.type == TypeVarChar) {
                    length = token.size();
                    memcpy((char *) buffer + offset, &length, sizeof(int));
                    offset += sizeof(int);
                    memcpy((char *) buffer + offset, token.c_str(), length);
                    offset += length;

                    auto got = indexMap.find(keyIndex);
                    if (got != indexMap.end())
                        memcpy((char *) indexMap[keyIndex], (char *) buffer + offset - sizeof(int) - length,
                               sizeof(int) + length);
                } else if (attr.type == TypeInt) {
                    int num = atoi(tokenizer);
                    memcpy((char *) buffer + offset, &num, sizeof(num));
                    offset += sizeof(num);

                    auto got = indexMap.find(keyIndex);
                    if (got != indexMap.end())
                        memcpy((char *) indexMap[keyIndex], (char *) buffer + offset - sizeof(int), sizeof(int));
                } else if (attr.type == TypeReal) {
                    float num = atof(tokenizer);
                    memcpy((char *) buffer + offset, &num, sizeof(num));
                    offset += sizeof(num);

                    auto got = indexMap.find(keyIndex);
                    if (got != indexMap.end())
                        memcpy((char *) indexMap[keyIndex], (char *) buffer + offset - sizeof(float), sizeof(int));
                }

                tokenizer = strtok(NULL, CVS_DELIMITERS);
                keyIndex += 1;
                if (keyIndex == attributes.size())
                    keyIndex = 0;
            }
            if (this->insertTupleToDB(tableName, attributes, buffer, indexMap) != 0) {
                return error("error while inserting tuple");
            }

            delete[] a;
            // prepare tuple for addition
            // for (std::vector<Attribute>::iterator it = attrs.begin() ; it != attrs.end(); ++it)
            // totalLength += it->length;
        }
        // clear up indexMap
        for (auto & it : indexMap) {
            free(it.second);
        }

        free(buffer);
        free(key);
        ifs.close();
        return 0;
    }

    RC CLI::insertTuple() {
        char *token = next();
        if (!expect(token, "into"))
            return error("expecting into" + __LINE__);

        std::string tableName = next();

        token = next(); // tuple
        if (!expect(token, "tuple"))
            return error("expecting tuple" + __LINE__);

        // get attributes from catalog
        Attribute attr;
        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(tableName, attributes);
        int offset = 0, index = 0;
        int length;
        void *buffer = malloc(PAGE_SIZE);
        memset(buffer, 0, PAGE_SIZE);
        void *key = malloc(PAGE_SIZE);
        RID rid;

        // find out if there is any index for tableName
        std::unordered_map<int, void *> indexMap;
        for (uint i = 0; i < attributes.size(); i++) {
            if (this->checkAttribute(tableName, attributes.at(i).name, rid, false))
                // add index to index-map
                indexMap[i] = malloc(PAGE_SIZE);
        }

        // Assume that we don't have any NULL values when inserting data.
        // Null-indicators
        int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attributes.size());
        auto *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);

        memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullsIndicator, nullAttributesIndicatorActualSize);
        offset += nullAttributesIndicatorActualSize;

        // tokenize input
        token = next();
        while (token != NULL) {
            attr = attributes[index];
            if (attributes.at(index).name != std::string(token))
                return error("this table does not have this attribute!");
            token = next(); // eat =
            token = next();
            if (attr.type == TypeVarChar) {
                std::string varChar = std::string(token);
                length = varChar.size();
                memcpy((char *) buffer + offset, &length, sizeof(int));
                offset += sizeof(int);
                memcpy((char *) buffer + offset, varChar.c_str(), length);
                offset += length;

                auto got = indexMap.find(index);
                if (got != indexMap.end()) {
                    memcpy((char *) indexMap[index], (char *) buffer + offset - sizeof(int) - length,
                           sizeof(int) + length);
                }
            } else if (attr.type == TypeInt) {
                int num = std::atoi(token);
                memcpy((char *) buffer + offset, &num, sizeof(num));
                offset += sizeof(num);

                auto got = indexMap.find(index);
                if (got != indexMap.end())
                    memcpy((char *) indexMap[index], (char *) buffer + offset - sizeof(int), sizeof(int));
            } else if (attr.type == TypeReal) {
                float num = std::atof(token);
                memcpy((char *) buffer + offset, &num, sizeof(num));
                offset += sizeof(num);

                auto got = indexMap.find(index);
                if (got != indexMap.end())
                    memcpy((char *) indexMap[index], (char *) buffer + offset - sizeof(float), sizeof(int));
            }
//            else if (attr.type == TypeBoolean || attr.type == TypeShort) {
//                return error("I do not wanna add this type of variable");
//            }
            token = next();
            index += 1;
        }
        if (this->insertTupleToDB(tableName, attributes, buffer, indexMap) != 0) {
            return error("error while inserting tuple");
        }

        // clear up indexMap
        for (auto it = indexMap.begin(); it != indexMap.end(); ++it) {
            free(it->second);
        }

        free(buffer);
        free(key);
        return 0;
    }

    RC CLI::insertTupleToDB(const std::string& tableName, const std::vector <Attribute>& attributes, const void *data,
                            const std::unordered_map<int, void *>& indexMap) {
        RID rid;

        // insert data to given table
        if (rm.insertTuple(tableName, data, rid) != 0)
            return error("error CLI::insertTuple in rm.insertTuple");

        return 0;
    }

    RC CLI::printAttributes() {
        char *tokenizer = next();
        if (tokenizer == NULL) {
            error("I expect tableName to print its attributes/columns");
            return -1;
        }

        std::string tableName = std::string(tokenizer);

        // get attributes of tableName
        Attribute attr;
        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(tableName, attributes);

        // update attributes
        std::vector <std::string> outputBuffer;
        outputBuffer.emplace_back("name");
        outputBuffer.emplace_back("type");
        outputBuffer.emplace_back("length");

        for (auto & attribute : attributes) {
            outputBuffer.push_back(attribute.name);
            outputBuffer.push_back(std::to_string(attribute.type));
            outputBuffer.push_back(std::to_string(attribute.length));
        }

        return this->printOutputBuffer(outputBuffer, 3);
    }

    RC CLI::printIndex() {
        char *tokenizer = next();
        std::string columnName = std::string(tokenizer);

        tokenizer = next();
        if (tokenizer == NULL || !expect(tokenizer, "on")) {
            return error("syntax error: expecting \"on\"");
        }

        tokenizer = next();
        std::string tableName = std::string(tokenizer);

        RM_IndexScanIterator rmisi;
        if (rm.indexScan(tableName, columnName, NULL, NULL, false, false, rmisi) != 0)
            return error("error in indexScan::printIndex");

        std::vector <std::string> outputBuffer;
        RID rid;
        char key[PAGE_SIZE];

        outputBuffer.emplace_back("PageNum");
        outputBuffer.emplace_back("SlotNum");
        while (rmisi.getNextEntry(rid, key) == 0) {
            outputBuffer.push_back(std::to_string(rid.pageNum));
            outputBuffer.push_back(std::to_string(rid.slotNum));
        }

        return this->printOutputBuffer(outputBuffer, 2);
    }

// print every tuples in given tableName
    RC CLI::printTable(const std::string& tableName) {
        std::vector <Attribute> attributes;
        RC rc = this->getAttributesFromCatalog(tableName, attributes);
        if (rc != 0)
            return error("table: " + tableName + " does not exist");

        // Set up the iterator
        RM_ScanIterator rmsi;
        RID rid;
        void *data_returned = malloc(4096);


        // convert attributes to vector<string>
        std::vector <std::string> stringAttributes;
        for (auto & attribute : attributes)
            stringAttributes.push_back(attribute.name);

        rc = rm.scan(tableName, "", NO_OP, NULL, stringAttributes, rmsi);
        if (rc != 0)
            return rc;

        // print
        std::vector <std::string> outputBuffer;
        for (auto & attribute : attributes) {
            outputBuffer.push_back(attribute.name);
        }

        while ((rc = rmsi.getNextTuple(rid, data_returned)) != RM_EOF) {
            if (rc != 0) {
                std::cout << "fata" << std::endl;
                exit(1);
            }

            if (this->updateOutputBuffer(outputBuffer, data_returned, attributes) != 0) {
                free(data_returned);
                return error("problem in updateOutputBuffer");
            }
        }
        rmsi.close();
        free(data_returned);

        return this->printOutputBuffer(outputBuffer, attributes.size());
    }

    RC CLI::help(const std::string& input) {
        if (input == "create") {
            std::cout << "\tcreate table <tableName> (col1 = type1, col2 = type2, ...): creates table with given properties"
                 << std::endl;
            std::cout << "\tcreate index <columnName> on <tableName>: creates index for <columnName> in table <tableName>"
                 << std::endl;
            std::cout << "\tcreate catalog" << std::endl;
        } else if (input == "add") {
            std::cout << "\tadd attribute \"attributeName=type\" to \"tableName\": drops given table" << std::endl;
        } else if (input == "drop") {
            std::cout << "\tdrop table <tableName>: drops given table" << std::endl;
            std::cout << "\tdrop index <attributeName> on <tableName>: drops given index" << std::endl;
            std::cout << "\tdrop attribute <attributeName> from <tableName>: drops attributeName from tableName" << std::endl;
            std::cout << "\tdrop catalog" << std::endl;
        } else if (input == "insert") {
            std::cout << "\tinsert into <tableName> tuple(attr1 = val1, attr2 = value2, ...)";
            std::cout << ": inserts given tuple to given tableName" << std::endl;
        } else if (input == "print") {
            std::cout << "\tprint <tableName>: print every record in tableName" << std::endl;
            std::cout << "\tprint attributes <tableName>: print columns of given tableName" << std::endl;
            std::cout << "\tprint index <attributeName> on <tableName>: print columns of given tableName" << std::endl;
        } else if (input == "load") {
            std::cout << "\tload <tableName> \"fileName\"";
            std::cout << ": loads given filName to given table" << std::endl;
        } else if (input == "help") {
            std::cout << "\thelp <commandName>: print help for given command" << std::endl;
            std::cout << "\thelp: show help for all commands" << std::endl;
        } else if (input == "quit") {
            std::cout << "\tquit or exit: quit SecSQL. But remember, love never ends!" << std::endl;
        } else if (input == "query") {
            std::cout << std::endl;
            std::cout << "\tAll queries start with \"SELECT\"" << std::endl;

            std::cout << "\t\t<query> = " << std::endl;
            std::cout << "\t\t\tPROJECT <query> GET \"[\" <attrs> \"]\"" << std::endl;
            std::cout << "\t\t\tFILTER <query> WHERE <attr> <op> <value>" << std::endl;
            std::cout << "\t\t\tBNLJOIN <query>, <query> WHERE <attr> <op> <attr> PAGES(<numPages>)" << std::endl;
            std::cout << "\t\t\tINLJOIN <query>, <query> WHERE <attr> <op> <attr>" << std::endl;
            std::cout << "\t\t\tGHJOIN <query>, <query> WHERE <attr> <op> <attr> PARTITIONS(<numPartitions>)" << std::endl;
            std::cout << "\t\t\tAGG <query> [ GROUPBY(<attr>) ] GET <agg-op>(<attr>)" << std::endl;
            std::cout << "\t\t\tIDXSCAN <query> <attr> <op> <value>" << std::endl;
            std::cout << "\t\t\tTBLSCAN <query>" << std::endl;
            std::cout << "\t\t\t<tableName>" << std::endl;

            std::cout << "\t\t<agg-op> = MIN | MAX | SUM | AVG | COUNT" << std::endl;
            std::cout << "\t\t<op> = < | > | = | != | >= | <= | NOOP" << std::endl;
            std::cout << "\t\t<attrs> = <attr> { \",\" <attr> }" << std::endl;
            std::cout << "\t\t<numPages> = is a number bigger than 0" << std::endl;
            std::cout << "\t\t<numPartitions> = is a number bigger than 0" << std::endl;
            std::cout << std::endl;
        } else if (input == "all") {
            help("create");
            help("drop");
            help("print");
            help("insert");
            help("load");
            help("help");
            help("query");
            help("quit");
        } else {
            std::cout << "I dont know how to help you with <" << input << ">" << std::endl;
            return -1;
        }
        return 0;
    }

    RC CLI::getAttributesFromCatalog(const std::string& tableName, std::vector <Attribute> &columns) {
        return rm.getAttributes(tableName, columns);
    }

    // Add given table to CLI_TABLES
    RC CLI::addTableToCatalog(const std::string& tableName, const std::string& file_url, const std::string& type) {
        int offset = 0;
        int length;
        void *buffer = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH);

        // Null-indicators
        auto *nullsIndicator = (unsigned char *) malloc(1);

        memset(buffer, 0, COLUMNS_TABLE_RECORD_MAX_LENGTH);
        memset(nullsIndicator, 0, 1);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullsIndicator, 1);
        offset += 1;

        length = tableName.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, tableName.c_str(), tableName.size());
        offset += tableName.size();

        length = file_url.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, file_url.c_str(), file_url.size());
        offset += file_url.size();

        length = type.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, type.c_str(), type.size());
        offset += type.size();

        RID rid;
        RC ret = rm.insertTuple(CLI_TABLES, buffer, rid);

        free(buffer);
        return ret;
    }

// adds given attribute to CLI_COLUMNS
    RC CLI::addAttributeToCatalog(const Attribute &attr, const std::string& tableName, const int position) {
        int offset = 0;
        int length;
        void *buffer = malloc(COLUMNS_TABLE_RECORD_MAX_LENGTH + 1);

        // Null-indicators
        auto *nullsIndicator = (unsigned char *) malloc(1);

        memset(buffer, 0, COLUMNS_TABLE_RECORD_MAX_LENGTH + 1);
        memset(nullsIndicator, 0, 1);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullsIndicator, 1);
        offset += 1;

        length = attr.name.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, attr.name.c_str(), attr.name.size());
        offset += attr.name.size();

        length = tableName.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, tableName.c_str(), tableName.size());
        offset += tableName.size();

        memcpy((char *) buffer + offset, &position, sizeof(position));
        offset += sizeof(position);

        memcpy((char *) buffer + offset, &attr.type, sizeof(attr.type));
        offset += sizeof(attr.type);

        memcpy((char *) buffer + offset, &attr.length, sizeof(attr.length));
        offset += sizeof(attr.length);

        RID rid;
        RC ret = rm.insertTuple(CLI_COLUMNS, buffer, rid);

        free(buffer);
        return ret;
    }

// Add given index to CLI_INDEXES
    RC CLI::addIndexToCatalog(const std::string& tableName, const std::string& columnName) {
        // Collect information from the catalog for the columnName
        std::vector <Attribute> columns;
        if (this->getAttributesFromCatalog(tableName, columns) != 0)
            return -1;

        int max_size = -1;
        bool is_variable = false;
        for (auto & column : columns) {
            if (column.name == columnName) {
                if (column.type == TypeVarChar) {
                    max_size = column.length + 2;
                    is_variable = true;
                } else {
                    max_size = column.length;
                }
                break;
            }
        }

        if (max_size == -1)
            return error("max-size returns -1");

        int offset = 0;
        int length;
        void *buffer = malloc(tableName.size() + columnName.size() + 8 + 4 + 1 + 1);

        // Null-indicators
        auto *nullsIndicator = (unsigned char *) malloc(1);

        memset(buffer, 0, tableName.size() + columnName.size() + 8 + 4 + 1 + 1);
        memset(nullsIndicator, 0, 1);

        // Null-indicator for the fields
        memcpy((char *) buffer + offset, nullsIndicator, 1);
        offset += 1;

        length = tableName.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, tableName.c_str(), tableName.size());
        offset += tableName.size();

        length = columnName.size();
        memcpy((char *) buffer + offset, &length, sizeof(int));
        offset += sizeof(int);
        memcpy((char *) buffer + offset, columnName.c_str(), columnName.size());
        offset += length;

        memcpy((char *) buffer + offset, &max_size, sizeof(max_size));
        offset += sizeof(max_size);

        memcpy((char *) buffer + offset, &is_variable, sizeof(is_variable));
        offset += sizeof(is_variable);

        RID rid;
        RC rc = rm.insertTuple(CLI_INDEXES, buffer, rid);

        free(buffer);

        return rc;
    }

    RC CLI::history() {
#ifndef NO_HISTORY_LIST
        HIST_ENTRY **the_list;
      int ii;
      the_list = history_list();
      if (the_list)
      for (ii = 0; the_list[ii]; ii++)
         printf ("%d: %s\n", ii + history_base, the_list[ii]->line);
#else
        HIST_ENTRY *the_list;
        the_list = current_history();
        std::vector <std::string> list;
        while (the_list) {
            list.emplace_back(the_list->line);
            the_list = next_history();
        }
        int tot = list.size();
        for (int i = tot - 1; i >= 0; i--) {
            std::cout << (tot - i) << ": " << list.at(i) << std::endl;
        }
#endif
        return 0;
    }

    // checks whether given tableName-columnName exists or not in cli_columns or cli_indexes
    bool CLI::checkAttribute(const std::string& tableName, const std::string& columnName, RID &rid, bool searchColumns) {
        std::string searchTable = CLI_COLUMNS;
        if (searchColumns == false)
            searchTable = CLI_INDEXES;

        std::vector <Attribute> attributes;
        this->getAttributesFromCatalog(CLI_COLUMNS, attributes);

        // Set up the iterator
        RM_ScanIterator rmsi;
        void *data_returned = malloc(PAGE_SIZE);

        // convert attributes to vector<string>
        std::vector <std::string> stringAttributes;
        //stringAttributes.push_back("column_name");
        stringAttributes.emplace_back("table_name");

        int stringSize = columnName.size();
        void *value = malloc(stringSize + sizeof(unsigned));
        memcpy((char *) value, &stringSize, sizeof(unsigned));
        memcpy((char *) value + sizeof(unsigned), columnName.c_str(), stringSize);

        // Find records whose column is columnName
        if (rm.scan(searchTable, "column_name", EQ_OP, value, stringAttributes, rmsi) != 0)
            return -1;

        // check if tableName is what we want
        while (rmsi.getNextTuple(rid, data_returned) != RM_EOF) {
            int length = 0, offset = 0;

            // adding +1 because of nulls-indicator
            offset++;

            memcpy(&length, (char *) data_returned + offset, sizeof(int));
            offset += sizeof(int);

            char *str = (char *) malloc(length + 1);
            str[length] = '\0';

            memcpy(str, (char *) data_returned + offset, length);
            offset += length;

            if (tableName == std::string(str)) {
                free(data_returned);
                free(str);
                return true;
            }
            free(str);
        }
        rmsi.close();
        free(value);
        free(data_returned);
        return false;
    }

    RC CLI::updateOutputBuffer(std::vector <std::string> &buffer, void *data, std::vector <Attribute> &attrs) {
        int length, offset = 0, number;
        float fNumber;
        char *str;
        std::string record;

        // Assume that we don't have any NULL values when loading data.
        // Null-indicators
        int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());

        // Pass Null-indicator for the fields
        offset += nullAttributesIndicatorActualSize;

        for (auto & attr : attrs) {
            switch (attr.type) {
                case TypeInt:
                    number = 0;
                    memcpy(&number, (char *) data + offset, sizeof(int));
                    offset += sizeof(int);
                    buffer.push_back(std::to_string(number));
                    break;
                case TypeReal:
                    fNumber = 0;
                    memcpy(&fNumber, (char *) data + offset, sizeof(float));
                    offset += sizeof(float);
                    buffer.push_back(std::to_string(fNumber));
                    break;
                case TypeVarChar:
                    length = 0;
                    memcpy(&length, (char *) data + offset, sizeof(int));

                    if (length == 0) {
                        buffer.emplace_back("--");
                        break;
                    }

                    offset += sizeof(int);

                    str = (char *) malloc(length + 1);
                    memcpy(str, (char *) data + offset, length);
                    str[length] = '\0';
                    offset += length;

                    buffer.emplace_back(str);
                    free(str);
                    break;
            }
        }
        return 0;
    }

    // 2-pass output function
    RC CLI::printOutputBuffer(std::vector <std::string> &buffer, uint mod) {
        // find max for each column
        uint *maxLengths = new uint[mod];
        for (uint i = 0; i < mod; i++)
            maxLengths[i] = 0;

        int index;
        for (uint i = 0; i < buffer.size(); i++) {
            index = i % mod;
            maxLengths[index] = fmax(maxLengths[index], buffer.at(i).size());
        }

        uint startIndex = 0;
        int totalLength = 0;

        for (uint i = 0; i < mod; i++) {
            std::cout << std::setw(maxLengths[i]) << std::left << buffer[i] << DIVISOR;
            totalLength += maxLengths[i] + DIVISOR_LENGTH;
        }
        std::cout << std::endl;

        // totalLength - 2 because I do not want to count for extra spaces after last column
        for (int i = 0; i < totalLength - 2; i++)
            std::cout << "=";
        startIndex = mod;

        // output columns
        for (uint i = startIndex; i < buffer.size(); i++) {
            if (i % mod == 0)
                std::cout << std::endl;
            std::cout << std::setw(maxLengths[i % mod]) << std::left << buffer[i] << DIVISOR;
        }
        std::cout << std::endl;
        delete[] maxLengths;
        return 0;
    }

    // advance tokenizer to next token
    char *CLI::next() {
        return strtok(NULL, DELIMITERS);
    }

    bool CLI::expect(const std::string& token, const std::string& expected) {
        return expect(token.c_str(), expected);
    }

// return 0 if tokenizer is equal to expected string
    bool CLI::expect(const char *token, const std::string& expected) {
        if (token == NULL) {
            error("tokenizer is null, expecting: " + expected);
            return -1;
        }
        std::string s1 = toLower(std::string(token));
        std::string s2 = toLower(expected);
        return s1 == s2;
    }

    std::string CLI::toLower(std::string data) {
        std::transform(data.begin(), data.end(), data.begin(), ::tolower);
        return data;
    }

    RC CLI::error(const std::string& errorMessage) {
        std::cout << errorMessage << std::endl;
        return -2;
    }

    RC CLI::error(uint errorCode) {
        std::cout << errorCode << std::endl;
        return -3;
    }

    RC CLI::getAttribute(const std::string& name, const std::vector <Attribute>& pool, Attribute &attr) {
        for (const auto & i : pool) {
            if (name == i.name) {
                attr = i;
                return 0;
            }
        }
        return error("attribute cannot be found");
    }

    RC CLI::getAttribute(const std::string& tableName, const std::string& attrName, Attribute &attr) {
        std::vector <Attribute> attrs;
        if (this->getAttributesFromCatalog(tableName, attrs) != 0)
            return error(__LINE__);
        return getAttribute(attrName, attrs, attr);
    }

    void CLI::addTableNameToAttrs(const std::string& tableName, std::vector <std::string> &attrs) {
        for (auto & attr : attrs)
            attr = fullyQualify(attr, tableName);
    }
}