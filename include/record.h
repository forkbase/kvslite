#ifndef KV_RECORD_H_
#define KV_RECORD_H_

#include<cstdint>

//Record class shall only be used inside log_storage.h
//hold the info of a serialized record in log.
class Record{
private:
    //record header includes: pointer to previous header, size of the record, 
    //size 16ULL
    struct RecordHeader{
        union {
            struct{
                uint64_t prev_addr_:48ULL; //address of the previous record
                uint64_t reserved_:64ULL-48ULL-1ULL;
                uint64_t tombstone:1ULL; //1 if the record is tombstone, only info inserted
            };
            uint64_t info_=0;
        };
        union {
            struct{
                uint64_t keysize_:32ULL; // the length of serialized key
                uint64_t valuesize_:32ULL; // the length of serialized value
            };
            uint64_t kv_sizes=0;
        };
    };
public:
    Record(){}
    
    void Fill(uint32_t keysize, uint32_t valuesize, bool tombstone, uint64_t prev_address){
        header_.prev_addr_ = prev_address;
        header_.tombstone = tombstone;
        header_.keysize_ = keysize;
        header_.valuesize_ = valuesize;
    }
    inline void* Key(){
        return reinterpret_cast<uint8_t*>(this)+16ULL;
    }

    inline void* Value(){
        return reinterpret_cast<uint8_t*>(this)+16ULL+header_.keysize_;
    }

    //return the size of the record in log.
    inline uint64_t Size(){
        return 16ULL+header_.keysize_+header_.valuesize_;
    }

public:
    RecordHeader header_;
};

#endif