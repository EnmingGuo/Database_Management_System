#include <string>

#include "src/include/cli.h"

using namespace std;

bool DEMO = false;

PeterDB::CLI *cli;

void exec(const string& command) {
    cout << ">>> " << command << endl;
    cli->process(command);
}

int main() {

    cli = PeterDB::CLI::Instance();
    if (DEMO) {
        exec("create table employee EmpName = varchar(30), Age = int, Height = real, Salary = int");
        exec("create table ages Age = int, Explanation = varchar(50)");
        exec("create table salary Salary = int, Explanation = varchar(50)");
        exec("create table company CompName = varchar(50), Age = int");

        exec("load employee employee_5");
        exec("load ages ages_90");
        exec("load salary salary_5");
        exec("load company company_7");

        exec("create index Age on employee");
        exec("create index Age on ages");
        exec("create index Salary on employee");
        exec("create index Salary on salary");
        exec("create index Age on company");
    }

    cli->start();

    return 0;
}
