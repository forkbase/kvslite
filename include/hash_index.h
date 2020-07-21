#ifndef KV_HASHINDEX_H_
#define KV_HASHINDEX_H_

//implementation of extendible hash table

#include <cstdint>
#include <cstring>
#include <list>
#include <mutex>
#include "status.h"
#include "file_linux.h"

// for debug
#include <bitset>


//HashIndex takes a Hash value in the format of uint64_t
//it is tied with the system page size, user can make some configuration later. default values are used for now.
//return the address of the head of a chained record with like hash value in log storage
class HashIndex{
public:
    static constexpr uint64_t kCachelineSize = 64;
    // number of entries in each bucket
    static constexpr uint64_t kNumEntryPerBucket = 4; // 64B cacheline/16B per entry
    // initial number of pages allocated to hash table. 
    static constexpr uint64_t kMaxIndexPage = 1280;
    // number of max in-mem pages. 4 pages allocated for directory.
    static constexpr uint64_t kNumInMemPage = 1024;
private:
    
    //each page in Mapped is an array of HashEntry
    class HashEntry{
    public:
        // return the validation bit wrt 
        // para: the number of buckets in a page (default 64 in 4KB page)
        //          (in calculation only the first half are hashable buckets so divided in 2 inside functions)
        //       local depth of the page
        //       init size of the table (guaranteed to be power of 2)
        // during extend, local depth increment 1, the last validation bit of each bucket will be used for splitting
        // usage: to populate a hash entry call SetTaken(), SetAddress(), SetValidation().
        //        for the last entry of a bucket, SetOverflownBucket() also needed.
        inline uint64_t Validation(const uint64_t k_num_bucket, const uint64_t local_depth){
            return hash_val_ / (k_num_bucket/2) >> local_depth;
        }

        inline void SetValidation(const uint64_t hash_val, const uint64_t k_num_bucket, const uint64_t local_depth){
            {
                // std::cout << "\n found index entry and setting hash value: " 
                //     << ((hash_val & ~((k_num_bucket/2 << local_depth) -1)) | this->GetOverflownBucket(k_num_bucket))
                //     << "\n validation: "
                //     << (hash_val & ~((k_num_bucket/2 << local_depth) -1)) << " in binary "
                //     << std::bitset<64>((hash_val & ~((k_num_bucket/2 << local_depth) -1)))
                //     << "\n overflown bucket: " 
                //     << this->GetOverflownBucket(k_num_bucket) << "\n";
            }
            hash_val_ = ((hash_val & ~((k_num_bucket/2 << local_depth) -1)) //this part set the validation
                        | this->GetOverflownBucket(k_num_bucket)); //this part preserve the previous overflown bucket value.
        }
        
        //values should fall in [bucket_in_page_/2, bucket_in_page_)
        //in this case one addition bit used but reduced one calculation and 0 can be used to represent empty 
        inline void SetOverflownBucket(const uint64_t overflow_bucket_addr){
            hash_val_ = hash_val_ | overflow_bucket_addr;
        }

        // currently not used
        inline void ResetOverflownBucket(const uint64_t k_num_bucket){
            hash_val_ = hash_val_ & ~(k_num_bucket-1);
        }

        inline uint64_t GetOverflownBucket(const uint64_t k_num_bucket){
            return hash_val_ & (k_num_bucket-1);
        }

        //used to take the last bit in old validation value to redistribute into extended page
        inline uint64_t GetHashVal(){
            return hash_val_;
        }

        // inline void ClearHashVal(){
        //     hash_val_=0;
        // }

        inline uint64_t GetAddress(){
            return log_address_;
        }

        inline uint64_t SetAddress(const uint64_t addr){
            return log_address_ = addr;
        }

        inline void SetTaken(){
            empty_bit_ = 1;
        }
        inline uint64_t CheckTaken(){
            return empty_bit_;
        }
        inline void UnsetTaken(){
            empty_bit_ = 0;
        }

        inline void SetDeleted(){
            deleted_ = 1;
        }
        inline uint64_t CheckDeleted(){
            return deleted_;
        }
        inline void UnsetDeleted(){
            deleted_= 0;
        }

        inline bool FullChain(){return chain_length_ == ((1ULL<<(64ULL-48ULL-1ULL-1ULL))-1);}
        inline bool EmptyChain(){return chain_length_==0;}
        inline void IncChainLength(){if(!FullChain()) chain_length_++;}
        inline void DecChainLength(){if(!FullChain()) chain_length_--;}
        
        inline void ClearAll(){
            hash_val_=0;
            log_content_=0;
        }

        //unused
        // inline uint64_t GetContent(){
        //     return log_content_;
        // }
        uint64_t hash_val_;
        union {
            struct{
                uint64_t log_address_:48ULL;
                uint64_t chain_length_:64ULL-48ULL-1ULL-1ULL; //14 bits left
                uint64_t deleted_:1ULL;
                uint64_t empty_bit_:1ULL;
            };
            uint64_t log_content_;
        };
    };

    struct HashBucket{
        HashEntry elements_[kNumEntryPerBucket];
    };

    struct MappedPage{
        bool dirtybit = 0; 
        uint32_t idx = 0;
        uint32_t dir_idx;
        std::list<unsigned int>::iterator lru_idx; //points back to its location in lru_seq.
        std::recursive_mutex page_mtx;
        void* page = nullptr;
    };



    struct DirEntry{
        // int membit=0;
        uint32_t local_depth=0;
        // uint64_t occupancy = 0;//not used for now
        uint32_t overflown_bucket_cur=32; //index at half of page size by default
        uint64_t offset=UINT64_MAX;//logical index of the hash pages in file
        MappedPage * page_ptr = nullptr;
    };

    // load the nth page from file.
    MappedPage * LoadPage(uint64_t offset, uint32_t directory_idx);
    
    // invoked by index function whenever page accessed before modification.
    inline void ResetPageLru(MappedPage & mp){
        if(mp.idx == lru_seq_.back()) std::unique_lock<std::mutex> latch(lru_seq_tail_mtx_);
        lru_seq_.erase(mp.lru_idx);
        // if (!lru_seq_.empty()) latch.unlock();
        lru_seq_.push_front(mp.idx);
        mp.lru_idx = lru_seq_.begin(); //not locked: flush thread can only touch the end of tail. checkpoint only take tail when occupancy full. 
    }
    
    // set dirty shall happen after modification. 
    inline void SetDirty(MappedPage & mp){
        mp.dirtybit = 1;
    }

    // flush the least recently used page by background thread. not enabled
    Status FlushLRUPage();

    //the last few bits (usually 5 for 32) are used for bucket assignment.
    //so diridx shall not use bits before them.
    inline uint64_t DirIdx(const uint64_t hash_val){
        return hash_val/(bucket_per_page_/2) & ((1 << global_depth_) -1);
    }

    //return the the actual entry refering at wrt local depth.
    inline uint64_t RefDirIdx(const uint64_t hash_val){
        uint64_t ld = entries_[DirIdx(hash_val)].local_depth;
        return hash_val/(bucket_per_page_/2) & ((1<<ld)-1);
    }

    inline uint64_t BucketIdx(const uint64_t hash_val){
        return hash_val & (bucket_per_page_/2-1);
    }

    inline HashBucket * GetBucket(const uint64_t bucket_idx, void * page){
        return reinterpret_cast<HashBucket*>(page) + bucket_idx;
    }

    inline uint64_t GetRecordValidate(const uint64_t hash_val, const uint64_t local_depth){
        return hash_val / (bucket_per_page_/2) >> local_depth;
    }

    //the last entry in page store the current local depth for recovery purpose
    inline void UpdatePageLocalDepth(void * page, const uint64_t depth){
        reinterpret_cast<HashEntry*>(page)[entry_per_page_-1].SetAddress(depth);
    }

public:
    //the size input shall be a power of 2 and lt 0;
    HashIndex() = delete;
    HashIndex(const HashIndex&) = delete;
    HashIndex& operator=(const HashIndex&) = delete;

    HashIndex(File* file)
    : file_(file){
        page_size_ = sysconf(_SC_PAGE_SIZE);
        bucket_per_page_ = page_size_/kCachelineSize;
        entry_per_page_ = page_size_/sizeof(HashEntry);
    }

    HashIndex(const int size, File* file)
    : file_(file){
        page_size_ = sysconf(_SC_PAGE_SIZE);
        bucket_per_page_ = page_size_/kCachelineSize;
        entry_per_page_ = page_size_/sizeof(HashEntry);
        global_depth_ = ffs(size)-1;
    }

    HashIndex* Initialize();

    HashIndex* LoadFromFile(File* file_in);

    // return the physical address in log of the last recrod with the same hash value.
    Status Get(uint64_t record, uint64_t &log_addr);

    // return the address of the last element sharing the same
    // para record is the 64 bit hash representation of the record.
    // para log_addr is the address of this record in log storage.
    // if the record has been inserted before, put failed and log_addr is changed to refer to previous record.
    // note that since uint64_t might not be unique for a record, Put shall not be called in kv.
    // it is only called by Upsert, when it is confirmed that record corresponding hash_val has not been inserted to the index yet.
    Status Put(const uint64_t record, uint64_t &log_addr);

    // only difference from put is that if record exist and not deleted, it is updated with success.
    Status Upsert(const uint64_t record, uint64_t &log_addr);

    // if the Deleted record found, log_addr is updated for the kvs to mark or insert tombstone as needed.
    // hash entry will be marked deleted
    // if not found, return FAILED.
    Status Delete(const uint64_t record, uint64_t &log_addr);

    // record is the seed. Extend only happen when an insertion failed due to not enough space.
    // extend will not be reversed, as deletion is rare if it ever happens. 
    // note that local_depths and global_depth_ can only be updated by this function.
    Status Extend(const uint64_t record);

    // background thread flushing disabled
    Status BackgroundFlush(){
        return FlushLRUPage();
    }

    // checkpoint should write into a new file.
    Status Checkpoint();

    void Close();

    void PrintDirectoryInfo();

    ~HashIndex(){
    }

private:
    uint16_t bucket_per_page_= 64;
    uint16_t entry_per_page_ = 256; //4096/16
    uint64_t page_size_ = 4096; //_SC_PAGE_SIZE
    uint64_t global_depth_ = 6; //extendible hash global depth
    //number of index page used, first few pages reserved for directory.
    uint64_t num_page_alloc_ed_ = 256;
    // data structure belong to in_mem_pages
    uint64_t in_mem_page_count_ = 0; // count of pages allocated so far. meaningful at the beginning. but once full, main thread evict to get space

    //one file to store both the directory and hash index.
    File * file_=nullptr; 
    
    //for checkpointing, need to use File->Write. first few pages reserved for it.
    //directory occupies a contiguous space and is supposed be in memory at all times
    //size can be calculated from global depth.
    DirEntry* entries_=nullptr; 

    std::mutex lru_seq_tail_mtx_;
    std::list<unsigned int> lru_seq_; //points to the corresponding entry in MappedPage entry
    MappedPage im_pages_[kNumInMemPage];
    
};

#endif