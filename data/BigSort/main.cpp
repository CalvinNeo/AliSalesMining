#include <iostream>
#include <cstdio>
#include <cstring>
#include <string>
#include <algorithm>
#include <functional>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <ctime>
#include <cctype>
#include <vector>
#include <process.h>
#include <io.h>
#include <map>
#include <queue>

using namespace std;

struct Trans{
	int ts;
	int sid;
	int uid;
};

const int MAXSHARD = 1000000;
char buf[1024];
Trans tdata[5000010];
int shard_count = 0;

void do_psort(){
    FILE * fout = fopen("./shard/partition.bind", "rb");
    fread(&shard_count, sizeof shard_count, 1, fout);
    fclose(fout);
    for(int shard_index = 0; shard_index <= shard_count; shard_index++){

        sprintf(buf, "./shard/s%05d.bind", shard_index);
        FILE * fshard = fopen(buf, "rb");
        sprintf(buf, "./shard/s%05d.sorted", shard_index);
        FILE * fsort = fopen(buf, "wb+");
        memset(tdata, sizeof tdata, 0);
        printf("sort shard %d into %s\n", shard_index, buf);

        int index = 0;
        while(true){
            if(fread(&(tdata[index]), sizeof(Trans), 1, fshard) != 1){
                break;
            }
            index++;
        }
        printf("shard %d total is %d\n", shard_index, index);
        std::sort(tdata, tdata+index+1, [&](const Trans & a, const Trans & b){
            return a.ts < b.ts;
        });
        for(int i = 0; i < index; i++){
            fwrite(&(tdata[i]), sizeof(tdata[i]), 1, fsort);
        }
        fclose(fshard);
        fclose(fsort);

    }
    fout = fopen("./shard/psort.sorted", "wb+");
    fwrite(&shard_count, sizeof shard_count, 1, fout);
    fclose(fout);
}

void do_partition(){
    int  debug_i = 0;
	FILE * ori_f = fopen("../user_pay.txt", "r");
	int u, s, yy, mm, dd, H, M, S;
	FILE * fout = nullptr;
	int item_id = 0;
	while(~fscanf(ori_f, "%d,%d,%d-%d-%d %d:%d:%d\n", &u, &s, &yy, &mm, &dd, &H, &M, &S)){
        tm _tm;
        _tm.tm_year = yy - 1900;
        _tm.tm_mon = mm - 1;
        _tm.tm_mday = dd;
        _tm.tm_hour = H;
        _tm.tm_min = M;
        _tm.tm_sec = S;
        time_t _ts = mktime(&_tm);
        Trans tr{_ts, s, u};
        if(fout == nullptr || item_id >= MAXSHARD){
            if(item_id >= MAXSHARD){
                shard_count ++;
                item_id = 0;
                fclose(fout);
            }
            memset(buf, 0, sizeof buf);
            sprintf(buf, "./shard/s%05d.bind", shard_count);
            fout = fopen(buf, "wb+");
            printf("make shard %d\n", shard_count);
        }
        int cc = fwrite(&tr, sizeof(Trans), 1, fout);
        item_id++;
	}
    fclose(fout);
    fout = fopen("./shard/partition.bind", "wb+");
    fwrite(&shard_count, sizeof shard_count, 1, fout);
    fclose(fout);
}

struct ST{
    Trans tr;
    int sh;
    ST(const Trans & _tr, int _sh) : tr(_tr), sh(_sh){

    }
    bool operator< (const ST & b) const{
        return this->tr.ts < b.tr.ts;
    }
};

bool get_tr(Trans & t_ptr, FILE * fs){
    int cc = fread(&t_ptr, sizeof(Trans), 1, fs);
    if(cc != 1){
        return false;
    }else{
        return true;
    }
}

void do_pmerge(){
    FILE * fout = fopen("./shard/psort.sorted", "rb");
    fread(&shard_count, sizeof shard_count, 1, fout);
    fclose(fout);
    FILE* fs[256];
    for(int shard_index = 0; shard_index <= shard_count; shard_index++){
        sprintf(buf, "./shard/s%05d.sorted", shard_index);
        FILE * fsort = fopen(buf, "rb");
        fs[shard_index] = fsort;
    }

    int acc = 0;
    priority_queue<ST> pq;
    Trans tr;
    for(int shard_index = 0; shard_index <= shard_count; shard_index++){
        if(get_tr(tr, fs[shard_index])){
            pq.push(ST{tr, shard_index});
        }
    }
    fout = fopen("./shard/final.bin", "wb+");

    while(!pq.empty()){
        ST st = pq.top();
        Trans tt = st.tr;

        fwrite(&tt, sizeof (tt), 1, fout);
        bool can = get_tr(tr, fs[st.sh]);
        pq.pop();
        if(can){
            pq.push(ST{tr, st.sh});
        }
        acc++;
        if(acc % 1000000 == 0){
            printf("completeness is %d Million \n", acc / 1000000);
        }
    }
    fclose(fout);
    fout = fopen("./shard/pmerge.merged", "wb+");
    fwrite(&acc, sizeof acc, 1, fout);
    fclose(fout);
}

int main(){
    mkdir("./shard");
    FILE * ftest;
    ftest = fopen("./shard/partition.bind", "rb");
    if(ftest == 0){
        puts("begin partitioning");
        do_partition();
    }
    ftest = fopen("./shard/psort.sorted", "rb");
    if(ftest == 0){
        puts("begin sorting");
        do_psort();
    }
    ftest = fopen("./shard/pmerge.merged", "rb");
    if(ftest == 0){
        puts("begin merging");
        do_pmerge();
    }
    // s00000.sorted
    // final.bin
    ftest = fopen("./shard/final.bin", "rb");
    if(ftest != 0){
        puts("showing results");
        Trans tr;
        while(true){
            for(int i = 0; i < 100; i++){
                if(fread(&tr, sizeof(tr), 1, ftest) != 1){
                    break;
                }
                printf("uid: %d, sid: %d, ts: %d\n", tr.uid, tr.sid, tr.ts);
            }
            system("pause");
        }
    }


//	printf("%d,%d,%d-%d-%d %d:%d:%d\n", u, s, yy, mm, dd, H, M, S);
}

