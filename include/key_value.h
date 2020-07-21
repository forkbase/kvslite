#ifndef KV_KEYVALUE_H_
#define KV_KEYVALUE_H_

#include <cstdint>
#include <string>

class Key{
public: 
    Key(){}
    virtual uint32_t Size()=0; //size after serializtion in bytes
    virtual void Serialize(void * buf)=0;
    virtual void Deserialize(void* buf)=0;
    virtual std::string Represent_str()=0; // a string representation to calculate hash value for index.
    virtual bool IsEqual(void * key_seri)=0; // compare if it equals to the serialized key pointed by key_seri 
    virtual ~Key(){}
};

class Value{
public: 
    Value(){}
    virtual uint32_t Size()=0; //size after serialization in bytes
    virtual void Serialize(void * buf)=0;
    virtual void Deserialize(void* buf)=0;
    virtual ~Value(){}
};

class ZeroValue : public Value{
public:
    ZeroValue(){}
    uint32_t Size(){return 0;}
    void Serialize(void * buf){return;}
    void Deserialize(void * buf){return;}
    ~ZeroValue(){}
};

#endif