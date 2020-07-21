#ifndef KV_FLEXIBLELOG_H_
#define KV_FLEXIBLELOG_H_

#include <cstdint>
#include <cstring>
#include "log_storage.h"
#include "file_linux.h"
#include "key_value.h"

class FlexibleLog : public LogStorage{
public:
    FlexibleLog() = delete;
    FlexibleLog(const FlexibleLog&) = delete;
    FlexibleLog& operator=(const FlexibleLog&) = delete;

    FlexibleLog(File * file) : LogStorage(file){}
    
    Status Get(const uint64_t address, Key &key_in, Value &value_return);

    Status Put(const uint64_t &prev_address, Key &key_in, Value &value_in, uint64_t &address_return); 

    //TODO: not tested.
    //tombstone record has only record header and serialized key.
    Status Delete(uint64_t &address_return, Key &key_in, const uint64_t &prev_address);

    Status BackgroundFlush();

    Status Checkpoint();

    void Compact(){
        //TODO
    }

    void Close();

    ~FlexibleLog() = default;

private:

    // adjust put location
    // side effects: update num_extra_page_load, update log_end_addr_
    inline void AdjustPutAddr(const uint64_t &record_size, uint64_t &num_extra_page_load);

    // TODO
    inline Status PutLargerThanBufferRecord(const uint64_t &record_size
        , const uint64_t &address_return, Key &key_in, Value &value_in, const uint64_t &prev_address){
        
        // TODO
        
        return Status::FAILED; // to be modified
    }

    // extend the log. old pages are un-mapped and new pages are mapped in
    Status ExtendLog(const uint64_t record_size, const uint64_t &num_extra_page_load, const uint64_t &insert_addr);
};

#endif