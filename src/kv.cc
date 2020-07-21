#include <sched.h>
#include <thread>
#include "kv.h"
#include "status.h"
#include "system_catelog.h"
#include "hash_index.h"
#include "log_storage.h"
#include "flexible_log.h"
#include "key_value.h"

// for debug
#include <iostream>

KV* KV::Initialize(const std::string&& system_catelog_path, const int newbit){
    catelog_ = new SystemCatelog(system_catelog_path, newbit);
    fd_idx.Open(catelog_->GetIndexFilePath().c_str());
    fd_log.Open(catelog_->GetdataFilePath().c_str());
    index_ = new HashIndex(&fd_idx);
    (!newbit) ? index_->LoadFromFile(&fd_idx) : index_->Initialize();
    store_= new FlexibleLog(&fd_log);
    (!newbit) ? store_->LoadFromFile(&fd_log) : store_->Initialize();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc!= 0) std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    std::cout << "main thread on CPU: " << sched_getcpu() << "\n";
    CPU_ZERO(&cpuset);
    CPU_SET(3, &cpuset);
    back_thread_ = std::thread(&KV::BackgroundRoutine, this);
    rc = pthread_setaffinity_np(back_thread_.native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc!= 0) std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    return this;
}

Status KV::Get(Key &key, Value &value){
    uint64_t hash_val = GetHash64(key);
    uint64_t log_address;
    // { // logging time version
    //     auto op_start = std::chrono::high_resolution_clock::now();
    //     if (index_->Get(hash_val, log_address) == Status::FAILED) return Status::FAILED;
    //     auto index_end = std::chrono::high_resolution_clock::now();
    //     if (store_->Get(log_address, key, value) == Status::FAILED) return Status::FAILED;
    //     auto op_end = std::chrono::high_resolution_clock::now();
    //     std::cout << "read: " << std::chrono::duration_cast<std::chrono::nanoseconds>(index_end-op_start).count()<<"(index)"<<std::chrono::duration_cast<std::chrono::nanoseconds>(op_end-op_start).count() << "(op)" << "\n";
    // }
    if (index_->Get(hash_val, log_address) == Status::FAILED) return Status::FAILED;
    if (store_->Get(log_address, key, value) == Status::FAILED) return Status::FAILED;
    return Status::SUCCESS;
}

Status KV::Put(Key &key, Value &value){
    uint64_t hash_val = GetHash64(key);
    uint64_t address;
    uint64_t prev_address;
    // { // logging time version
    //     auto op_start = std::chrono::high_resolution_clock::now();
    //     if (index_->Get(hash_val, prev_address)==Status::FAILED) prev_address=0;
    //     auto index_end = std::chrono::high_resolution_clock::now();
    //     if (store_->Put(address, key, value, prev_address)==Status::FAILED) return Status::FAILED;
    //     auto log_end = std::chrono::high_resolution_clock::now();
    //     if (index_->Upsert(hash_val, address)==Status::FAILED) return Status::FAILED;
    //     auto op_end = std::chrono::high_resolution_clock::now();
    //     std::cout << "put: " << std::chrono::duration_cast<std::chrono::nanoseconds>(index_end-op_start).count()<<"(index)"
    //     <<std::chrono::duration_cast<std::chrono::nanoseconds>(log_end-op_start).count() << "(log)" 
    //     <<std::chrono::duration_cast<std::chrono::nanoseconds>(op_end-op_start).count() << "(op)" << "\n";
    // }
    //TODO: might provide a third state for retry
    if (index_->Get(hash_val, prev_address)==Status::FAILED) prev_address=0;
    if (store_->Put(prev_address, key, value, address)==Status::FAILED) return Status::FAILED;
    if (index_->Upsert(hash_val, address)==Status::FAILED) return Status::FAILED;
    return Status::SUCCESS;        
}

Status KV::Delete(Key &key, Value &value){
    uint64_t hash_val = GetHash64(key);
    uint64_t address;
    uint64_t prev_address;
    if (index_->Get(hash_val, prev_address)==Status::FAILED) return Status::FAILED; //nothing to delete
    if (store_->Delete(address, key, prev_address)==Status::FAILED) return Status::FAILED;
    index_->Delete(hash_val, address); //will never fail, since Get returned Success.
    return Status::SUCCESS;
}

void KV::BackgroundRoutine(){
    std::this_thread::sleep_for(std::chrono::microseconds(6));
    std::cout << "Background thread at pid " << std::this_thread::get_id() << "started" << std::endl;
    std::cout << "background thread on CPU: " << sched_getcpu() << "\n";
    int flush_batch_trial = 10;
    while(!background_stop_){
        // while (index_->BackgroundFlush()==Status::SUCCESS);
        while ((flush_batch_trial-->0)&&(store_->BackgroundFlush()==Status::SUCCESS));
        flush_batch_trial = 10;
        std::this_thread::sleep_for(std::chrono::nanoseconds(60));
    }
    std::cout << "Background thread at pid " << std::this_thread::get_id() << "stopped" << std::endl;
}

void KV::Checkpoint(){
    //checkpoint index and log
    store_->Checkpoint(); 
    index_->Checkpoint();
    //update system catelog
}

void KV::Close(){
    background_stop_ = true;
    if (back_thread_.joinable()) back_thread_.join();
    index_->Close();
    store_->Close();
    fd_idx.Close();
    fd_log.Close();
    delete catelog_;
    delete index_;
    delete store_;
}
