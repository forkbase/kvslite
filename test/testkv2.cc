#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <fstream>
#include <list>
#include "key_value.h"
#include "kv.h"

class KT:public Key{
public:
    KT(){}

    KT(const std::string& str){
        content_ = str;
    }
    
    uint32_t Size(){
        //add 1 endline character
        return content_.size()+1;
    }

    void Serialize(void * buf){
        memcpy(buf, content_.c_str(), Size());
    }

    void Deserialize(void * buf){
        //for c++ string this content_ does not point to buf.
        //a new memory space is allocated for the content and content_ points to it.
        content_.assign(reinterpret_cast<char const*>(buf));
    }

    std::string Represent_str(){
        return content_;
    }

    bool IsEqual(void * key_seri){
        return !content_.compare(reinterpret_cast<char const*>(key_seri));
    }

private:
    std::string content_;
};

class VT:public Value{
public:
    VT(){}

    VT(const std::string& str){
        content_ = str;
    }
    
    uint32_t Size(){
        //add 1 endline character
        return content_.size()+1;
    }

    void Serialize(void * buf){
        memcpy(buf, content_.c_str(), Size());
    }

    void Deserialize(void * buf){
        //for c++ string this content_ does not point to buf.
        //a new memory space is allocated for the content and content_ points to it.
        content_.assign(reinterpret_cast<char const*>(buf));
    }

    std::string Represent_str(){
        return content_;
    }

private:
    std::string content_;
};

int main(int argc, char **argv){
    if (argc!=4){
        printf("Usage: input_data_path, output_path, kv_path");
        exit(0);
    }

    std::string key_str;
    std::string value_str;
    std::list<std::string> keylist;


    std::ifstream in;
    in.open(argv[1], std::ios::in);
    while(getline(in, key_str, ' ')){
        getline(in, value_str);
        keylist.push_back(key_str);
    }
    in.close();

    KT key;
    VT value;
    VT ret;

    // ///////////////////test reload/////////////////////// //
    std::ofstream out2;
    out2.open(argv[2], std::ios::out | std::ios::trunc);
    KV kv_test2;
    
    kv_test2.Initialize(argv[3], 0);
    kv_test2.SetDefaultHash();

    int count = 0;
    for (auto itr = keylist.begin(); itr != keylist.end(); itr++){
        key = KT(*itr);
        kv_test2.Get(key, ret);
        out2 << key.Represent_str() << ' ' << ret.Represent_str() << "\n";
    }

    // //////////////////test repeated put and delete /////////////// //
    // key = KT("1234567890");
    // value = VT("99887766554433221100");

    
    kv_test2.Checkpoint();
    kv_test2.Close();
    out2.close();


    return 0;
}
