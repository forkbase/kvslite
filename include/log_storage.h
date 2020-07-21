#ifndef KV_LOGSTORAGE_H_
#define KV_LOGSTORAGE_H_

// log storage syncrhonize with the background persistence thread only. 
// it is not mean for concurrent access.

#include <cstdint>
#include <mutex>
#include <condition_variable>
#include "file_linux.h"
#include "key_value.h"
#include "mapped_pages_ro.h"

//a log storage consists of a circular in memory buffer and disk component tied to a file
class LogStorage{

public:
    LogStorage() = delete;
    LogStorage(const LogStorage&) = delete;
    LogStorage& operator=(const LogStorage&) = delete;

    LogStorage(File * file){
        page_size_ = sysconf(_SC_PAGE_SIZE);
        file_=file;
        pages_.Initialize(file_, kNumSwapPage);
        buffer_ = nullptr;
    }

    LogStorage* Initialize();

    LogStorage* LoadFromFile(File* file_in);

    virtual Status Get(const uint64_t address, Key &key_in, Value &value_return) = 0; // read bytes from certain address
    
    virtual Status Put(const uint64_t &prev_address, Key &key_in, Value &value_in, uint64_t &address_return) = 0;

    // virtual void Compact()=0; // TODO

    Status Checkpoint(){
        return Status::SUCCESS;
        //override this function to perform other update and writting metadata.
    }; 

    void Close(){
        EvictPage(kNumBufPage);
        free(buffer_);
        buffer_=nullptr;
    }
    virtual ~LogStorage(){}

protected:
    // calculate the next page boundary
    inline uint64_t NextBoundary(const uint64_t address){
        return ((address-1) | (page_size_-1))+1;
    }
    
    // get page pointer of the in memory log component based on the index.
    // need to ensure idx is in range in the caller function
    inline void * GetPage(const unsigned int idx){
        //byte traversal to locate the idx page
        return (reinterpret_cast<unsigned char*> (buffer_)) + idx * page_size_;
    }

    // get the index of in memory component based on the log address.
    // assume addr > log_head_addr_
    // return kNumBufPage if address is not in beyond log.
    inline unsigned int BufPageIdx(const uint32_t addr){
        if (addr > log_end_addr_) {
            printf ("address beyond log.");
            return kNumBufPage;
        } else return ((addr-log_head_addr_)/page_size_ + log_head_page_)%kNumBufPage;
    }

    // flush one page. called by background thread or by the main thread when it is forced to use it to make space.
    Status FlushPages(std::unique_lock<std::mutex>&& latch);
    
    // evict num_page from the log head.
    // the evicted pages are assumed to have been flushed
    unsigned int EvictPage(unsigned int num_page);

    
    bool flush_in_progress = false;
    unsigned int flush_pending_count = 0;
    unsigned int log_persist_page_=0; // the next page to be flushed. newly added to replace log_persisit_addr_ 0522
    uint64_t last_flush_request_until_addr_=0;
    std::mutex flush_loc;
    std::condition_variable cv_flush_loc;

    //note that log_head_addr and log_persist_addr only stop at page boundary
    unsigned int log_head_page_=0;// the idx in the circular buffer of earliest page [0, kNumBufPage-1]
    uint64_t log_head_addr_=0; //head of in memory log.
    uint64_t log_end_addr_=0; //the front to insert (address of the byte after the last element)
    uint64_t span_record_addr_=0;
    void * span_record_=nullptr;
    
    uint64_t page_size_=4096;
    const unsigned int kNumBufPage = 1024;
    const unsigned int kNumSwapPage = 2048;

    void * buffer_=nullptr;
    MappedPagesRO pages_;
    File * file_; 
};

#endif