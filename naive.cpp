#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <cassert>

#include <sstream>
#include <typeinfo>

#include <vector>
#include <list>
#include <utility>
#include <functional>
#include <algorithm>
#include <bitset>
#include <time.h>

using namespace std;

#define LINE_BUF_SIZE 512

char *word;
char *tmp;

struct Record
{
    vector<string> tuple;
};

struct CSVDATA
{
    list<Record> data;
    char *name;
};

CSVDATA csvData_1;
CSVDATA csvData_2;
vector<int> record;

// // prototype

string StrToBitStr(string str);

// //

void print_csv(CSVDATA csvdata)
{
    for (auto rec = csvdata.data.begin(); rec != csvdata.data.end(); rec++)
    {
        for (auto col = rec->tuple.begin(); col != rec->tuple.end(); col++)
        {
            string str = *col;
            printf("%s ", str.c_str());
            // cout << str.c_str(); << endl;
        }
    }
}

// insert csv data into struct
CSVDATA insert_structure(CSVDATA csvdata, const char *path)
{
    ifstream ifs(path, ios::in);
    string _line;

    while (getline(ifs, _line))
    {
        Record rec;
        Record hrec;

        stringstream ss(_line);
        string _sub;

        while (getline(ss, _sub, ','))
        {
            rec.tuple.push_back(_sub);
        }

        csvdata.data.push_back(rec);
    }
    return csvdata;
}

CSVDATA get_record(const char *path, CSVDATA csvdata)
{
    csvdata = insert_structure(csvdata, path);

    return csvdata;
}

void compare(CSVDATA csvData_1, CSVDATA csvData_2)
{
    int line = 0;
    auto rec1 = csvData_1.data.begin();

    for (; rec1 != csvData_1.data.end();
         rec1++, line++)
    {
        if (line % 100 == 0)
        {
            printf("line: %d\n", line);
        }
        auto rec2 = csvData_2.data.begin();
        for (; rec2 != csvData_2.data.end(); rec2++)
        {
            if (rec1->tuple == rec2->tuple)
            {
                record.push_back(line);
            }
        }
    }
    sort(record.begin(), record.end());
    record.erase(unique(record.begin(), record.end()), record.end());
    cout << record.size() << endl;
}

int main(void)
{
    // clock_t begin = clock();
    string filepath1_1 = "cdl_16w_less.csv";
    string filepath2_1 = "cdl_1w_1_less.csv";
    const char *filepath1 = filepath1_1.c_str();
    const char *filepath2 = filepath2_1.c_str();
    csvData_1 = get_record(filepath1, csvData_1);

    csvData_2 = get_record(filepath2, csvData_2);

    struct timeval begin, end;
    gettimeofday(&begin, NULL);

    compare(csvData_1, csvData_2);
    gettimeofday(&end, NULL);
    // long dif_usec = end.tv_usec - begin.tv_usec;
    // long dif_msec = dif_usec / 1000;
    // cout << record.size() << endl;
    long diff = (end.tv_sec - begin.tv_sec) * 1000 * 1000 + (end.tv_usec - begin.tv_usec);
    long mdiff = diff / 1000;
    cout << "Running time: " << mdiff << "ms" << endl;
    // clock_t end = clock();
    // cout << "Running time: " << (double)(end - begin) / CLOCKS_PER_SEC * 1000 << "ms" << endl;
}
