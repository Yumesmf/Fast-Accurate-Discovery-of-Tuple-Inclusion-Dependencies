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
#include <sys/time.h>
#include <thread>

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

CSVDATA csvData_1, csvData_1_hash;
CSVDATA csvData_2, csvData_2_hash;
list<string> hash_hll_1;
list<string> hash_hll_2;
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
        }
    }
}

// insert csv data into struct
pair<CSVDATA, CSVDATA> insert_structure(CSVDATA csvdata, CSVDATA csvdata_hash, const char *path)
{
    ifstream ifs(path, ios::in);
    string _line;

    while (getline(ifs, _line))
    {
        Record rec;
        Record hrec;
        hash<string> hash_obj;

        stringstream ss(_line);
        string _sub;

        while (getline(ss, _sub, ','))
        {
            rec.tuple.push_back(_sub);
            stringstream hash_data;
            hash_data << hash_obj(_sub);
            hrec.tuple.push_back(string(hash_data.str()));
        }

        csvdata.data.push_back(rec);
        csvdata_hash.data.push_back(hrec);
    }
    return make_pair(csvdata, csvdata_hash);
}

pair<CSVDATA, CSVDATA> get_record(const char *path, CSVDATA csvdata, CSVDATA csvdata_hash)
{
    csvdata = insert_structure(csvdata, csvdata_hash, path).first;
    csvdata_hash = insert_structure(csvdata, csvdata_hash, path).second;

    return make_pair(csvdata, csvdata_hash);
}

// binary string, inverted index, generate ind
void HLL(CSVDATA csvData_1_hash, CSVDATA csvData_2_hash)
{
    // hashvalue->binary->first four bits
    for (auto &hrec : csvData_1_hash.data)
    {
        string hll = "";
        for (auto &hcol : hrec.tuple)
        {
            hll += StrToBitStr(hcol);
        }
        //     cout << hll << endl;
        //    cout << "###########" << endl;
        hash_hll_1.push_back(hll);
    }

    for (auto &hrec : csvData_2_hash.data)
    {
        string hll = "";
        for (auto &hcol : hrec.tuple)
        {
            hll += StrToBitStr(hcol);
        }
        hash_hll_2.push_back(hll);
    }
}

// get binary string
string StrToBitStr(string str)
{
    str = str.substr(0, 19);
    long binary;
    binary = atol(str.c_str());

    string r;
    while (binary != 0)
    {
        r += (binary % 2 == 0 ? "0" : "1");
        binary /= 2;
    }
    reverse(begin(r), end(r));

    string s = r.substr(0, 4);

    return s;
}

// Nested Loops Join
void IND_check()
{
    int line = 0;
    auto rec1 = csvData_1_hash.data.begin();
    auto h1 = hash_hll_1.begin();
    for (; h1 != hash_hll_1.end();
         h1++, rec1++, line++)
    {
        if (line % 100 == 0)
            printf("line: %d\n", line);
        auto rec2 = csvData_2_hash.data.begin();
        auto h2 = hash_hll_2.begin();
        for (; h2 != hash_hll_2.end(); h2++, rec2++)
        {
            if (*h1 == *h2)
            { // hash is same, false positive check.
                if (rec1->tuple == rec2->tuple)
                {
                    record.push_back(line);
                    // for (int i = 0; i < rec1->tuple.size(); i++)
                    // {
                    //     cout << rec1->tuple[i] << endl;
                    // }
                }
            }
        }
    }
    sort(record.begin(), record.end());
    record.erase(unique(record.begin(), record.end()), record.end());
    cout << record.size() << endl;
}

int main(void)
{
    string filepath1_1 = "cdl_4w.csv";
    string filepath2_1 = "cdl_4w_1.csv";
    const char *filepath1 = filepath1_1.c_str();
    const char *filepath2 = filepath2_1.c_str();
    csvData_1 = get_record(filepath1, csvData_1, csvData_1_hash).first;
    csvData_1_hash = get_record(filepath1, csvData_1, csvData_1_hash).second;
    // print_csv(csvData_1);
    csvData_2 = get_record(filepath2, csvData_2, csvData_2_hash).first;
    csvData_2_hash = get_record(filepath2, csvData_2, csvData_2_hash).second;
    HLL(csvData_1_hash, csvData_2_hash);

    struct timeval begin, end;
    gettimeofday(&begin, NULL);
    std::vector<std::thread> threads;
    IND_check();

    gettimeofday(&end, NULL);
    long diff = (end.tv_sec - begin.tv_sec) * 1000 * 1000 + (end.tv_usec - begin.tv_usec);
    long mdiff = diff / 1000;
    cout << "Running time: " << mdiff << "ms" << endl;
}
