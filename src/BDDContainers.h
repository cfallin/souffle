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

#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <stdint.h>
#include <assert.h>

#include "RamTypes.h"

namespace souffle {

template<typename T> class BDDNodeVec {
private:
    static constexpr int kBlockShift = 20;
    static constexpr size_t kBlockSize = 1L << kBlockShift;
    static constexpr size_t kBlockOffsetMask = kBlockSize - 1;
    static constexpr int kBlockCount = 4096;

    std::atomic<uintptr_t> blocks_[kBlockCount];
    std::atomic<size_t> size_;

    struct Block {
	T items_[kBlockSize];
    };

    Block* getOrAllocateBlock(size_t idx) {
	std::atomic<uintptr_t>& at = blocks_[idx];
	Block* b = nullptr;
	uintptr_t val = at.load(std::memory_order_acquire);
	if (val == 0) {
	    uint8_t* blockStore = new uint8_t[sizeof(Block)];
	    Block* newBlock = reinterpret_cast<Block*>(blockStore);
	    if (!at.compare_exchange_strong(val, reinterpret_cast<uintptr_t>(newBlock),
					    std::memory_order_acq_rel)) {
		delete[] blockStore;
		b = reinterpret_cast<Block*>(at.load(std::memory_order_acquire));
	    } else {
		b = newBlock;
	    }
	} else {
	    b = reinterpret_cast<Block*>(val);
	}
	return b;
    }

    Block* getBlock(int idx) {
	return reinterpret_cast<Block*>(blocks_[idx].load(std::memory_order_acquire));
    }

    const Block* getBlockConst(int idx) const {
	return reinterpret_cast<const Block*>(blocks_[idx].load(std::memory_order_acquire));
    }

public:
    BDDNodeVec() : size_(0) {
	for (int i = 0; i < kBlockCount; i++) {
	    blocks_[i].store(0);
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
		b->items_[off].T::~T();
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
	return b->items_[block_offset];
    }

    const T& operator[](size_t index) const {
	size_t block_idx = index >> kBlockShift;
	size_t block_offset = index & kBlockOffsetMask;
	const Block* b = getBlockConst(block_idx);
	return b->items_[block_offset];
    }

    template<typename... CtorArgs>
    size_t emplace_back(CtorArgs&&... args) {
	size_t new_index = size_.fetch_add(1, std::memory_order_acquire);
	size_t block_idx = new_index >> kBlockShift;
	size_t block_offset = new_index & kBlockOffsetMask;
	Block* b = getOrAllocateBlock(block_idx);
	new (&b->items_[block_offset]) T(args...);
	return new_index;
    }
};
    
}  // namespace souffle
