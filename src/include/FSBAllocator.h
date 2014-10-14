/*===========================================================================
  This library is released under the MIT license. See FSBAllocator.html
  for further information and documentation.

Copyright (c) 2008-2011 Juha Nieminen

Trivially extended with non-static representation and flexible arena size.
Copyright (c) 2014 CohortFS, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
=============================================================================*/

#ifndef INCLUDE_FSBALLOCATOR_H
#define INCLUDE_FSBALLOCATOR_H

#include <new>
#include <cassert>
#include <vector>

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OPENMP
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_PTHREAD
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC
#define FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
#include <boost/thread.hpp>
typedef boost::mutex FSBAllocator_Mutex;
#endif

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OPENMP
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_PTHREAD
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC
#define FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
#include <omp.h>

class FSBAllocator_Mutex
{
    omp_lock_t mutex;

 public:
    FSBAllocator_Mutex() { omp_init_lock(&mutex); }
    ~FSBAllocator_Mutex() { omp_destroy_lock(&mutex); }
    void lock() { omp_set_lock(&mutex); }
    void unlock() { omp_unset_lock(&mutex); }
};
#endif

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_PTHREAD
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OPENMP
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC
#define FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
#include <pthread.h>

class FSBAllocator_Mutex
{
    pthread_mutex_t mutex;

 public:
    FSBAllocator_Mutex() { pthread_mutex_init(&mutex, NULL); }
    ~FSBAllocator_Mutex() { pthread_mutex_destroy(&mutex); }
    void lock() { pthread_mutex_lock(&mutex); }
    void unlock() { pthread_mutex_unlock(&mutex); }
};
#endif

#if defined(FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC) || defined(FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC_WITH_SCHED)
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OPENMP
#undef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_PTHREAD
#define FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC_WITH_SCHED
#include <sched.h>
#endif
class FSBAllocator_Mutex
{
    volatile int lockFlag;

 public:
    FSBAllocator_Mutex(): lockFlag(0) {}
    void lock()
    {
        while(!__sync_bool_compare_and_swap(&lockFlag, 0, 1))
        {
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_GCC_WITH_SCHED
            sched_yield();
#endif
        }
    }
    void unlock() { lockFlag = 0; }
};
#endif

template<unsigned ElemSize, unsigned NBlockElts = 512>
class FSBAllocator_ElemAllocator
{
    typedef std::size_t Data_t;
    static const Data_t BlockElements = NBlockElts;

    static const Data_t DSize = sizeof(Data_t);
    static const Data_t ElemSizeInDSize = (ElemSize + (DSize-1)) / DSize;
    static const Data_t UnitSizeInDSize = ElemSizeInDSize + 1;
    static const Data_t BlockSize = BlockElements*UnitSizeInDSize;

    class MemBlock
    {
        Data_t* block;
        Data_t firstFreeUnitIndex, allocatedElementsAmount, endIndex;

     public:
        MemBlock():
            block(0),
            firstFreeUnitIndex(Data_t(-1)),
            allocatedElementsAmount(0)
        {}

        bool isFull() const
        {
            return allocatedElementsAmount == BlockElements;
        }

        void clear()
        {
            delete[] block;
            block = 0;
            firstFreeUnitIndex = Data_t(-1);
        }

        void* allocate(Data_t vectorIndex)
        {
            if(firstFreeUnitIndex == Data_t(-1))
            {
                if(!block)
                {
                    block = new Data_t[BlockSize];
                    if(!block) return 0;
                    endIndex = 0;
                }

                Data_t* retval = block + endIndex;
                endIndex += UnitSizeInDSize;
                retval[ElemSizeInDSize] = vectorIndex;
                ++allocatedElementsAmount;
                return retval;
            }
            else
            {
                Data_t* retval = block + firstFreeUnitIndex;
                firstFreeUnitIndex = *retval;
                ++allocatedElementsAmount;
                return retval;
            }
        }

        void deallocate(Data_t* ptr)
        {
            *ptr = firstFreeUnitIndex;
            firstFreeUnitIndex = ptr - block;

            if(--allocatedElementsAmount == 0)
                clear();
        }
    };

    struct BlocksVector
    {
        std::vector<MemBlock> data;

        BlocksVector() { data.reserve(1024); }

        ~BlocksVector()
        {
            for(std::size_t i = 0; i < data.size(); ++i)
                data[i].clear();
        }
    };

    BlocksVector blocksVector;
    std::vector<Data_t> blocksWithFree;

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
    FSBAllocator_Mutex mutex;

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
    struct Lock: boost::mutex::scoped_lock
    {
        Lock(): boost::mutex::scoped_lock(mutex) {}
    };
#else
    struct Lock
    {
        Lock() { mutex.lock(); }
        ~Lock() { mutex.unlock(); }
    };
#endif
#endif

 public:
    void* allocate()
    {
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
        Lock lock;
#endif

        if(blocksWithFree.empty())
        {
            blocksWithFree.push_back(blocksVector.data.size());
            blocksVector.data.push_back(MemBlock());
        }

        const Data_t index = blocksWithFree.back();
        MemBlock& block = blocksVector.data[index];
        void* retval = block.allocate(index);

        if(block.isFull())
            blocksWithFree.pop_back();

        return retval;
    }

    void deallocate(void* ptr)
    {
        if(!ptr) return;

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
        Lock lock;
#endif

        Data_t* unitPtr = (Data_t*)ptr;
        const Data_t blockIndex = unitPtr[ElemSizeInDSize];
        MemBlock& block = blocksVector.data[blockIndex];

        if(block.isFull())
            blocksWithFree.push_back(blockIndex);
        block.deallocate(unitPtr);
    }
};

template<unsigned ElemSize, unsigned NBlockElts = 1024>
class FSBAllocator2_ElemAllocator
{
    static const std::size_t BlockElements = NBlockElts;

    static const std::size_t DSize = sizeof(std::size_t);
    static const std::size_t ElemSizeInDSize = (ElemSize + (DSize-1)) / DSize;
    static const std::size_t BlockSize = BlockElements*ElemSizeInDSize;

    struct Blocks
    {
        std::vector<std::size_t*> ptrs;

        Blocks()
        {
            ptrs.reserve(256);
            ptrs.push_back(new std::size_t[BlockSize]);
        }

        ~Blocks()
        {
            for(std::size_t i = 0; i < ptrs.size(); ++i)
                delete[] ptrs[i];
        }
    };

    Blocks blocks;
    std::size_t headIndex;
    std::size_t* freeList;
    std::size_t allocatedElementsAmount;

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
    FSBAllocator_Mutex mutex;

#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_BOOST
    struct Lock: boost::mutex::scoped_lock
    {
        Lock(): boost::mutex::scoped_lock(mutex) {}
    };
#else
    struct Lock
    {
        Lock() { mutex.lock(); }
        ~Lock() { mutex.unlock(); }
    };
#endif
#endif

    void freeAll()
    {
        for(std::size_t i = 1; i < blocks.ptrs.size(); ++i)
            delete[] blocks.ptrs[i];
        blocks.ptrs.resize(1);
        headIndex = 0;
        freeList = 0;
    }

 public:
    void* allocate()
    {
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
        Lock lock;
#endif

        ++allocatedElementsAmount;

        if(freeList)
        {
            std::size_t* retval = freeList;
            freeList = reinterpret_cast<std::size_t*>(*freeList);
            return retval;
        }

        if(headIndex == BlockSize)
        {
            blocks.ptrs.push_back(new std::size_t[BlockSize]);
            headIndex = 0;
        }

        std::size_t* retval = &(blocks.ptrs.back()[headIndex]);
        headIndex += ElemSizeInDSize;
        return retval;
    }

    void deallocate(void* ptr)
    {
        if(ptr)
        {
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
            Lock lock;
#endif

            std::size_t* sPtr = (std::size_t*)ptr;
            *sPtr = reinterpret_cast<std::size_t>(freeList);
            freeList = sPtr;

            if(--allocatedElementsAmount == 0)
                freeAll();
        }
    }

    void cleanSweep(std::size_t unusedValue = std::size_t(-1))
    {
#ifdef FSBALLOCATOR_USE_THREAD_SAFE_LOCKING_OBJECT
        Lock lock;
#endif

        while(freeList)
        {
            std::size_t* current = freeList;
            freeList = reinterpret_cast<std::size_t*>(*freeList);
            *current = unusedValue;
        }

        for(std::size_t i = headIndex; i < BlockSize; i += ElemSizeInDSize)
            blocks.ptrs.back()[i] = unusedValue;

        for(std::size_t blockInd = 1; blockInd < blocks.ptrs.size();)
        {
            std::size_t* block = blocks.ptrs[blockInd];
            std::size_t freeAmount = 0;
            for(std::size_t i = 0; i < BlockSize; i += ElemSizeInDSize)
                if(block[i] == unusedValue)
                    ++freeAmount;

            if(freeAmount == BlockElements)
            {
                delete[] block;
                blocks.ptrs[blockInd] = blocks.ptrs.back();
                blocks.ptrs.pop_back();
            }
            else ++blockInd;
        }

        const std::size_t* lastBlock = blocks.ptrs.back();
        for(headIndex = BlockSize; headIndex > 0; headIndex -= ElemSizeInDSize)
            if(lastBlock[headIndex-ElemSizeInDSize] != unusedValue)
                break;

        const std::size_t lastBlockIndex = blocks.ptrs.size() - 1;
        for(std::size_t blockInd = 0; blockInd <= lastBlockIndex; ++blockInd)
        {
            std::size_t* block = blocks.ptrs[blockInd];
            for(std::size_t i = 0; i < BlockSize; i += ElemSizeInDSize)
            {
                if(blockInd == lastBlockIndex && i == headIndex)
                    break;

                if(block[i] == unusedValue)
                    deallocate(block + i);
            }
        }
    }
};

template<typename Ty, unsigned NBlockElts>
class FSBAllocator
{
 public:
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef Ty *pointer;
    typedef const Ty *const_pointer;
    typedef Ty& reference;
    typedef const Ty& const_reference;
    typedef Ty value_type;

    pointer address(reference val) const { return &val; }
    const_pointer address(const_reference val) const { return &val; }

    template<class Other>
    struct rebind
    {
      typedef FSBAllocator<Other, NBlockElts> other;
    };

    FSBAllocator() throw() {}

    template<class Other>
      FSBAllocator(const FSBAllocator<Other, NBlockElts>&) throw() {}

    template<class Other>
      FSBAllocator& operator=(const FSBAllocator<Other, NBlockElts>&) { return *this; }

    pointer allocate(size_type count, const void* = 0)
    {
        assert(count == 1);
        return static_cast<pointer>(elt_alloc.allocate());
    }

    void deallocate(pointer ptr, size_type)
    {
      elt_alloc.deallocate(ptr);
    }

    void construct(pointer ptr, const Ty& val)
    {
        new ((void *)ptr) Ty(val);
    }

    void destroy(pointer ptr)
    {
        ptr->Ty::~Ty();
    }

    size_type max_size() const throw() { return 1; }

 private:
    FSBAllocator_ElemAllocator<sizeof(Ty), NBlockElts> elt_alloc;
};

template<typename Ty, unsigned NBlockElts>
class FSBAllocator2
{
 public:
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef Ty *pointer;
    typedef const Ty *const_pointer;
    typedef Ty& reference;
    typedef const Ty& const_reference;
    typedef Ty value_type;

    pointer address(reference val) const { return &val; }
    const_pointer address(const_reference val) const { return &val; }

    template<class Other>
    struct rebind
    {
      typedef FSBAllocator2<Other, NBlockElts> other;
    };

    FSBAllocator2() throw() {}

    template<class Other>
      FSBAllocator2(const FSBAllocator2<Other, NBlockElts>&) throw() {}

    template<class Other>
      FSBAllocator2& operator=(const FSBAllocator2<Other, NBlockElts>&) { return *this; }

    pointer allocate(size_type count, const void* = 0)
    {
        assert(count == 1);
        return static_cast<pointer>(elt_alloc.allocate());
    }

    void deallocate(pointer ptr, size_type)
    {
      elt_alloc.deallocate(ptr);
    }

    void construct(pointer ptr, const Ty& val)
    {
        new ((void *)ptr) Ty(val);
    }

    void destroy(pointer ptr)
    {
        ptr->Ty::~Ty();
    }

    size_type max_size() const throw() { return 1; }

    void cleanSweep(std::size_t unusedValue = std::size_t(-1))
    {
      FSBAllocator2_ElemAllocator<sizeof(Ty), NBlockElts>::cleanSweep(unusedValue);
    }

 private:
    FSBAllocator2_ElemAllocator<sizeof(Ty), NBlockElts> elt_alloc;
};

//typedef FSBAllocator2<std::size_t> FSBRefCountAllocator; // c++11 could parameterise
template<unsigned NBlockElts = 512>
  class FSBRefCountAllocator : public FSBAllocator2<std::size_t, NBlockElts> {};

#endif
