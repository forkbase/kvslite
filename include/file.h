#ifndef KV_FILE_H_
#define KV_FILE_H_

#include "status.h"

class File{
public:
    File(){}

    virtual Status Open(const char * file_path)=0;

    virtual void * LoadPage(const uint64_t size, const uint64_t offset, void * buff, Status &status)=0;

    virtual Status FlushPage(const uint64_t size, void * buff)=0;

    virtual Status EvictPage(const uint64_t size, void * buff)=0;

    virtual Status Read(const uint64_t size, const uint64_t offset, void * buff)=0;

    virtual Status Write(const uint64_t size, const uint64_t offset, const void * buff)=0;

    virtual const uint64_t GetPageSize()=0;

    virtual Status Close()=0;
};

#endif