/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file BDD.h
 *
 * Implementation of Binary Decision Diagrams (BDDs).
 *
 ***********************************************************************/

#pragma once

#include <atomic>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "RamTypes.h"

namespace souffle {

template<typename T> class BDDNodeVec {
private:
    static constexpr int kBlockShift = 20;
    static constexpr size_t kBlockSize = 1L << kBlockShift;
    static constexpr size_t kBlockOffsetMask = kBlockSize - 1;
    static constexpr int kBlockCount = 4096;

    struct Block {
	T items[kBlockSize];
    };


    std::atomic<Block*> blocks_[kBlockCount];
    std::atomic<size_t> size_;

    Block* getOrAllocateBlock(size_t idx) {
	std::atomic<Block*>& at = blocks_[idx];
	Block* b = nullptr;
	Block* val = at.load(std::memory_order_acquire);
	if (!val) {
	    uint8_t* blockStore = new uint8_t[sizeof(Block)];
	    Block* newBlock = reinterpret_cast<Block*>(blockStore);
	    if (!at.compare_exchange_strong(val, newBlock,
					    std::memory_order_acq_rel)) {
		delete[] blockStore;
		val = at.load(std::memory_order_acquire);
	    } else {
		val = b;
	    }
	}
	return val;
    }

    Block* getBlock(int idx) {
	return blocks_[idx].load(std::memory_order_acquire);
    }

    const Block* getBlockConst(int idx) const {
	return blocks_[idx].load(std::memory_order_acquire);
    }

public:
    BDDNodeVec() : size_(0) {
	for (int i = 0; i < kBlockCount; i++) {
	    blocks_[i].store(nullptr);
	}
    }

    ~BDDNodeVec() {
	size_t len = size_.load();
	for (int i = 0; i < kBlockCount; i++) {
	    Block* b = getBlock(i);
	    if (!b) {
		break;
	    }
	    size_t off = 0;
	    for (; off < kBlockSize && off < len; off++) {
		b->items[off].T::~T();
	    }
	    len -= off;
	    uint8_t* blockStore = reinterpret_cast<uint8_t*>(b);
	    delete blockStore;
	}
    }

    size_t size() const {
	return size_.load(std::memory_order_acquire);
    }

    T& operator[](size_t index) {
	size_t block_idx = index >> kBlockShift;
	size_t block_offset = index & kBlockOffsetMask;
	Block* b = getBlock(block_idx);
	return b->items[block_offset];
    }

    const T& operator[](size_t index) const {
	size_t block_idx = index >> kBlockShift;
	size_t block_offset = index & kBlockOffsetMask;
	const Block* b = getBlockConst(block_idx);
	return b->items[block_offset];
    }

    template<typename... CtorArgs>
    size_t emplace_back(CtorArgs&&... args) {
	size_t new_index = size_.fetch_add(1, std::memory_order_acquire);
	size_t block_idx = new_index >> kBlockShift;
	size_t block_offset = new_index & kBlockOffsetMask;
	Block* b = getOrAllocateBlock(block_idx);
	new (&b->items[block_offset]) T(args...);
	return new_index;
    }
};

template<typename K, typename V> class BDDNodeMap {
private:
    static constexpr size_t kBucketBits = 14;
    static constexpr size_t kBucketCount = 1L << kBucketBits;
    static constexpr size_t kBucketMask = kBucketCount - 1;
    static constexpr size_t kBucketChunkSize = 16;
    

    struct HashBucketChunk {
	std::atomic<HashBucketChunk*> next;
	std::atomic<size_t> size;
	K keys[kBucketChunkSize];
	V values[kBucketChunkSize];
	std::atomic<bool> filled[kBucketChunkSize];
    };
    
    std::atomic<HashBucketChunk*> buckets_[kBucketCount];

    HashBucketChunk* get_block_with_space(size_t h) {
	HashBucketChunk* b = buckets_[h].load(std::memory_order_acquire);
	if (b && b->size.load(std::memory_order_acquire) < kBucketChunkSize) {
	    return b;
	}

	uint8_t* storage = new uint8_t[sizeof(HashBucketChunk)];
	memset(storage, 0, sizeof(HashBucketChunk));
	HashBucketChunk* newBlock = reinterpret_cast<HashBucketChunk*>(storage);
	new (newBlock) HashBucketChunk();
	newBlock->next.store(b, std::memory_order_relaxed);
	if (!buckets_[h].compare_exchange_strong(b, newBlock, std::memory_order_release)) {
	    delete[] storage;
	    return buckets_[h].load(std::memory_order_acquire);
	} else {
	    return newBlock;
	}
    }

public:
    BDDNodeMap() {
	for (size_t i = 0; i < kBucketCount; i++) {
	    buckets_[i].store(nullptr);
	}
    }
    ~BDDNodeMap() {
	for (size_t i = 0; i < kBucketCount; i++) {
	    HashBucketChunk* b = buckets_[i].load();
	    while (b) {
		HashBucketChunk* next = b->next.load();
		size_t size = b->size.load();
		for (size_t j = 0; j < size; j++) {
		    b->keys[j].K::~K();
		    b->values[j].V::~V();
		}
		uint8_t* storage = reinterpret_cast<uint8_t*>(b);
		delete[] storage;
		b = next;
	    }
	    buckets_[i].store(nullptr);
	}
    }

    struct iterator {
	HashBucketChunk* b;
	size_t idx;

	iterator(HashBucketChunk* b, size_t idx) : b(b), idx(idx) {}
	iterator(const iterator& other) : b(other.b), idx(other.idx) {}

	const K& key() const { return b->keys[idx]; }
	const V& value() const { return b->values[idx]; }

	void inc() {
	    idx++;
	    if (idx >= kBucketChunkSize || idx >= b->size.load(std::memory_order_acquire)) {
		idx = 0;
		b = b->next.load(std::memory_order_acquire);
	    }
	    if (b) {
		if (!b->filled[idx].load(std::memory_order_acquire)) {
		    b = nullptr;
		    idx = 0;
		}
	    }
	}

	bool done() const {
	    return !b;
	}

	static iterator empty() {
	    return iterator(nullptr, 0);
	}
    };

    iterator lookup(const K& key) {
	size_t h = key.hash() & kBucketMask;
	HashBucketChunk* b = buckets_[h].load(std::memory_order_acquire);
	while (b) {
	    size_t chunk_size = b->size.load(std::memory_order_acquire);
	    for (size_t i = 0; i < kBucketChunkSize && i < chunk_size; i++) {
		if (!b->filled[i].load()) {
		    return iterator::empty();
		}
		if (b->keys[i] == key) {
		    return iterator(b, i);
		}
	    }
	    b = b->next.load(std::memory_order_acquire);
	}
	return iterator::empty();
    }

    void insert(K&& key, V&& value) {
	size_t h = key.hash() & kBucketMask;
	HashBucketChunk* b = buckets_[h].load(std::memory_order_acquire);
	while (true) {
	    if (!b) {
		b = get_block_with_space(h);
		continue;
	    }
	    size_t s = b->size.load(std::memory_order_acquire);
	    if (s >= kBucketChunkSize) {
		b = get_block_with_space(h);
		continue;
	    }
	    if (!b->size.compare_exchange_strong(s, s + 1)) {
		continue;
	    }
	    new (&b->keys[s]) K(key);
	    new (&b->values[s]) V(value);
	    b->filled[s].store(true, std::memory_order_release);
	    return;
	}
    }
};
    
}  // namespace souffle
