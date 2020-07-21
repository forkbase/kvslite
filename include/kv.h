#ifndef KV_KV_H_
#define KV_KV_H_

// #include <sched.h>
#include <thread>
#include "status.h"
#include "system_catelog.h"
#include "hash_index.h"
#include "log_storage.h"
#include "flexible_log.h"
#include "key_value.h"

class KV {
public: 
    typedef uint64_t (*ID64) (Key &key); //conversion from key to a 64 bit id

    KV() = default;
    KV(const KV&) = delete;
    KV& operator=(const KV&) = delete;
    KV(KV&&) = delete;
    KV& operator=(KV&&) = delete;
    
    KV* Initialize(const std::string&& system_catelog_path, const int newbit);

    //a default hash function uses the string representation of the key to generate a 64-bit hash value
    void SetDefaultHash(){
        //default hash function takes the first 64 bit. user can replace it with other things.
        GetHash64 = [](Key &k){
                std::string s = k.Represent_str();
                s.resize(7, '0');
                return *reinterpret_cast<const uint64_t*>(s.c_str());
            };
    }

    //user shall not use it unless they have good reason
    void SetCustomizeHash(ID64 hash_function){
        GetHash64 = hash_function;
    }
    
    Status Get(Key &key, Value &value);

    Status Put(Key &key, Value &value);
    
    // TODO
    Status Delete(Key &key, Value &value);

    void BackgroundRoutine();

    // TODO, currently only flush all changes to original file. used before close
    void Checkpoint();

    void Close();

    ~KV() = default;
private:
    ID64 GetHash64;
    SystemCatelog* catelog_;
    File_linux fd_idx;
    File_linux fd_log;
    HashIndex* index_;
    FlexibleLog* store_;
    bool  background_stop_=false;
    std::thread back_thread_;
};

#endif 