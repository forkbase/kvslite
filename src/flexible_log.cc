#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>
#include "flexible_log.h"
#include "file_linux.h"
#include "key_value.h"
#include "mapped_pages_ro.h"
#include "record.h"

Status FlexibleLog::Get(const uint64_t address, Key &key_in, Value &value_return){

    if (address < page_size_) return Status::FAILED;
    Record * record;
    void * page;
    if (address == span_record_addr_){
        record = reinterpret_cast<Record*>(span_record_);
    } else if (address >= log_head_addr_){
        if (address >= log_end_addr_) return Status::FAILED; // record not inserted yet. shall never happen
        //in circular buffer.
        page = GetPage(BufPageIdx(address));
        record = reinterpret_cast<Record*>(reinterpret_cast<uint8_t*>(page)+address%page_size_);
    } else {
        
        //at least load the record header first
        page = pages_.GetPage(address, sizeof(Record));
        record = reinterpret_cast<Record*>(reinterpret_cast<uint8_t*>(page)+address%page_size_);
        // std::cout << record->Size() << std::endl; 
        // check if need to load extra page (actually can return the PgEntry then can determine if the mapped page is enough)
        if (address%page_size_+record->Size()>page_size_){
            page = pages_.GetPage(address, record->Size());//still need to modify the lru list tho, may modify later. now just make the logic here simple
            record = reinterpret_cast<Record*>(reinterpret_cast<uint8_t*>(page)+address%page_size_);
        }
        // std::cout << record->Size() << std::endl; 
    }
    if (key_in.IsEqual(record->Key())){
        value_return.Deserialize(record->Value());
        return Status::SUCCESS;
    } else return Get(address-record->header_.prev_addr_, key_in, value_return); //return Status::FAILED;
    //later i think better remove the recursion let kv to handle this. it would interfere with concurrent access (but may choose shared-nothing haven't decided.)
}

Status FlexibleLog::Put(const uint64_t &prev_address, Key &key_in, Value &value_in, uint64_t &address_return){

    // auto log_start = std::chrono::high_resolution_clock::now();

    uint64_t record_size = sizeof(Record)+key_in.Size()+value_in.Size(); 
    uint64_t num_extra_page_load = 0;

    // std::cout << "\n\nkey: " << key_in.Represent_str() << " val: " << value_in.Size() << "\n";
    AdjustPutAddr(record_size, num_extra_page_load);
    address_return = log_end_addr_;//the final return value no modification later.
    
    {
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "PUT1: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-log_start).count() << " ";
    }
    if (record_size > kNumBufPage*page_size_){
        return PutLargerThanBufferRecord(record_size, address_return, key_in, value_in, prev_address);
    } else {
        ExtendLog(record_size, num_extra_page_load, address_return);// if no need to exten then this function will not exten

        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "PUT2: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-log_start).count() << " ";
        }
        uint64_t start_insert_page = BufPageIdx(address_return);
        void * page = GetPage(start_insert_page);
        Record * to_insert = reinterpret_cast<Record*>(reinterpret_cast<uint8_t*>(page)+(address_return%page_size_));
        to_insert->Fill(key_in.Size(), value_in.Size(),0, prev_address);

        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "PUT2: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-log_start).count() << " ";
        }
        if (start_insert_page > BufPageIdx(address_return+record_size)){
            //record span the circular end;
            span_record_addr_ = address_return;
            span_record_ = malloc(record_size);
            if (!span_record_){perror("malloc failed"); return Status::ERROR;}
            //fill span record
            memcpy(span_record_, to_insert, sizeof(Record));
            key_in.Serialize(reinterpret_cast<Record*>(span_record_)->Key());
            value_in.Serialize(reinterpret_cast<Record*>(span_record_)->Value());
            //fill in circular buffer
            uint64_t bytes_till_buf_end = (address_return&(page_size_-1))
            ? (page_size_-(address_return&(page_size_-1))+(kNumBufPage-start_insert_page-1)*page_size_)
            : ((kNumBufPage-start_insert_page)*page_size_);
            memcpy(to_insert, span_record_, bytes_till_buf_end);
            memcpy(buffer_, reinterpret_cast<uint8_t*>(span_record_)+bytes_till_buf_end
                , record_size-bytes_till_buf_end);            
            {
                // auto bp_time = std::chrono::high_resolution_clock::now();
                // std::cout << "PUT31: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-log_start).count() << " ";
            }
        } else {
            //record fits in circular buffer
            key_in.Serialize(to_insert->Key());
            value_in.Serialize(to_insert->Value());

            {
                // auto bp_time = std::chrono::high_resolution_clock::now();
                // std::cout << "PUT32: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-log_start).count() << " ";
            }
        }
        if (log_end_addr_ - last_flush_request_until_addr_ > page_size_){
            unsigned int count = (log_end_addr_ - last_flush_request_until_addr_)/page_size_;
                {
                    std::lock_guard<std::mutex> latch(flush_loc);
                    flush_pending_count += count;
                    // std::cout << "\nincrement flush pending count to " << flush_pending_count << "\n"; 
                }                
            cv_flush_loc.notify_one();
            last_flush_request_until_addr_ += count*page_size_;
        } 
        {
            // auto log_end = std::chrono::high_resolution_clock::now();
            // std::cout << "PUT4: " << std::chrono::duration_cast<std::chrono::nanoseconds>(log_end-log_start).count() << " ";
        }
        return Status::SUCCESS;
    }
}

// TODO: not tested
Status FlexibleLog::Delete(uint64_t &address_return, Key &key_in, const uint64_t &prev_address){
    address_return = prev_address; // will be updated by put if tombstone insertion
    uint64_t temp_addr = prev_address;
    if (temp_addr >= log_end_addr_) return Status::FAILED;

    //the tombstone (record type) shall include a ptr pointing to the actual record (daddress)
    //or the record type shall provide functions to set and unset reference
    ZeroValue * zv = new ZeroValue();
    Status status = Put(prev_address, key_in, *zv, address_return);
    delete zv;
    return status;  
}

Status FlexibleLog::BackgroundFlush(){
    std::unique_lock<std::mutex> latch(flush_loc);
    if (cv_flush_loc.wait_for(latch, std::chrono::microseconds(6), [&]{return flush_pending_count>0;})){
        return FlushPages(std::move(latch));
    } else return Status::FAILED;
}

Status FlexibleLog::Checkpoint(){
    uint64_t log_end_addr_copy = log_end_addr_;
    {
        std::unique_lock<std::mutex> latch(flush_loc);
        unsigned int count = 0;
        while (FlushPages(std::move(latch))==Status::SUCCESS){if (count++>2000) {count = 0; std::this_thread::yield();}}
    }
    file_->FlushPage(page_size_, GetPage(BufPageIdx(log_end_addr_copy)));
    void * first_page = nullptr;
    Status load_return_status;
    first_page = file_->LoadPage(page_size_, 0, first_page, load_return_status);
    if (load_return_status == Status::FAILED) return Status::FAILED;
    uint8_t * ptr = reinterpret_cast<uint8_t*> (first_page);
    memcpy(ptr, &page_size_, sizeof(page_size_));
    ptr +=sizeof(page_size_);
    memcpy(ptr, &log_end_addr_copy, sizeof(uint64_t));
    file_->FlushPage(page_size_, first_page);
    file_->EvictPage(page_size_, first_page);
    return Status::SUCCESS;
}

void FlexibleLog::Close(){
    EvictPage(kNumBufPage);
    buffer_=nullptr;
    if (span_record_addr_) free(span_record_);
    span_record_ = nullptr;
    pages_.Close();
}

inline void FlexibleLog::AdjustPutAddr(const uint64_t &record_size, uint64_t &num_extra_page_load){
    //padding to let the record start on a new page if appending at the current position lead the record to span more pages
    uint64_t num_page_span = (record_size-1)/page_size_+1;
    // -1 then +1 to handle the boundary case. when log_end_addr at boundary, the next page has not been loaded.
    uint64_t cur_page_left = page_size_ - ((log_end_addr_-1) % page_size_+1);
    if (cur_page_left < record_size){
        num_extra_page_load = (record_size-cur_page_left-1)/page_size_+1;
        //if the record span one more page due to filling what's left over in the current page
        //note the following condition is equivalent num_page_span == num_extra_page_load
        //also record header is never split by page boundary
        if ((num_page_span < num_extra_page_load+1)||cur_page_left<sizeof(Record)){
            log_end_addr_ = NextBoundary(log_end_addr_);
            if (log_end_addr_==(last_flush_request_until_addr_ + page_size_)){
                {
                    std::lock_guard<std::mutex> latch(flush_loc);
                    flush_pending_count++;
                    // std::cout << "\nincrement flush pending count to " << flush_pending_count << "\n"; 
                }
                cv_flush_loc.notify_one();
                last_flush_request_until_addr_ += page_size_;
            } 
            // std::cout << "adjusted end address to " << log_end_addr_ << "\n";
        } 
    }
}

// with time logging 
Status FlexibleLog::ExtendLog(const uint64_t record_size, const uint64_t &num_extra_page_load, const uint64_t &insert_addr){
    // auto extend_start = std::chrono::high_resolution_clock::now();
    unsigned int temp = BufPageIdx(log_end_addr_);
    log_end_addr_ +=record_size;
    //number of pages to be evicted: current number of pages + num extra page load - kNumBufpage
    temp = (NextBoundary(insert_addr)-log_head_addr_)/page_size_ + num_extra_page_load;
    unsigned int num_page_evict = (temp > kNumBufPage) ? (temp-kNumBufPage) : 0;
    {
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "EXT1: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
    }
    if (num_page_evict){
        // std::cout << "\n  flush needed: " << num_page_evict << " head: " << log_head_page_ << "persist: " << log_persist_page_ << "\n";
        // unsigned int count = 0; 
        while ((log_persist_page_+kNumBufPage-log_head_page_)%kNumBufPage < num_page_evict){
            // std::cout << "entered loop times: " << count++ << "\n";
            // if (count > 1000){std::this_thread::sleep_for(std::chrono::nanoseconds(60));}
            // if (count > 10000){std::cerr << "\n main thread stuck at extend!\n ";exit(1);}
            std::unique_lock<std::mutex> latch(flush_loc);
            if (FlushPages(std::move(latch))==Status::FAILED) break; // num_page_evict larger than log. need to evict all.
        } 

        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "EVI2-1: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
        }

        EvictPage(num_page_evict);
        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "EVI2-2: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
        }
        if((span_record_addr_)&&(log_head_addr_>span_record_addr_)){
            span_record_addr_ = 0;
            free(span_record_);
            span_record_ = nullptr;
        }
        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "EVI2-3: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
        }
    }
    if(num_extra_page_load){
        uint64_t start_load_idx = (insert_addr%page_size_) 
            ? ((BufPageIdx(insert_addr)+1)%kNumBufPage) 
            : (BufPageIdx(insert_addr)%kNumBufPage);
        Status load_return_status;
        void * page = GetPage(start_load_idx);
        {
            // auto bp_time = std::chrono::high_resolution_clock::now();
            // std::cout << "EXT3: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
        }

        if(num_extra_page_load <= (kNumBufPage-start_load_idx)){
            page = file_->LoadPage(num_extra_page_load*page_size_, NextBoundary(insert_addr), page, load_return_status);
            std::memset(page, 0, num_extra_page_load*page_size_);
            if (load_return_status==Status::FAILED) return Status::FAILED;
        } else {
            page = file_->LoadPage((kNumBufPage-start_load_idx)*page_size_, NextBoundary(insert_addr), page, load_return_status);
            std::memset(page, 0, (kNumBufPage-start_load_idx)*page_size_);
            buffer_ = file_->LoadPage((num_extra_page_load-(kNumBufPage-start_load_idx))*page_size_
                , NextBoundary(insert_addr)+(kNumBufPage-start_load_idx)*page_size_, buffer_, load_return_status);
            std::memset(buffer_, 0, (kNumBufPage-start_load_idx)*page_size_);
        }
        if (load_return_status==Status::FAILED) return Status::FAILED;
    }
    {
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "EXT3: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-extend_start).count() << " ";
    }
    return Status::SUCCESS;
}