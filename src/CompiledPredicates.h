/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file CompiledPredicates.h
 *
 * The central interface for handling predicates in the compiled execution.
 *
 ***********************************************************************/

#pragma once

#include "RamTypes.h"
#include "Util.h"

#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace souffle {

class Predicates {
private:
    struct Entry {
        enum { And, Or, Not, Terminal } type;
        RamDomain a, b;
    };

    std::mutex mutex;
    std::unordered_map<Entry, RamDomain> entries;
    RamDomain next_entry;

    static const RamDomain TRUE = 0;
    static const RamDomain FALSE = MAX_RAM_DOMAIN;

    RamDomain insert_or_fetch(Entry e) {
        std::lock_guard<std::mutex> l(mutex);
        auto it = entries.find(e);
        if (it != entries.end()) {
            return it->second;
        } else {
            RamDomain idx = next_entry++;
            entries.insert(it, std::make_pair(e, idx));
            return idx;
        }
    }

public:
    Predicates() : next_entry(1) {}
    Predicates(const Predicates& other) = delete;
    Predicates& operator=(const Predicates& other) = delete;

    RamDomain one() const {
        return TRUE;
    }
    RamDomain zero() const {
        return FALSE;
    }
    RamDomain and (RamDomain a, RamDomain b) {
        if (a == FALSE || b == FALSE) {
            return FALSE;
        }
        if (a == TRUE) {
            return b;
        }
        if (b == TRUE) {
            return a;
        }
        if (a == b) {
            return a;
        }
        return insert_or_fetch({Entry::And, a, b});
    }
    RamDomain or (RamDomain a, RamDomain b) {
        if (a == TRUE || b == TRUE) {
            return TRUE;
        }
        if (a == FALSE) {
            return b;
        }
        if (b == FALSE) {
            return a;
        }
        if (a == b) {
            return a;
        }
        return insert_or_fetch({Entry::Or, a, b});
    }
    RamDomain not(RamDomain a) {
        if (a == TRUE) {
            return FALSE;
        }
        if (a == FALSE) {
            return TRUE;
        }
        return insert_or_fetch({Entry::Not, a, 0});
    }
    RamDomain terminal() {
        return insert_or_fetch({Entry::Term, 0, 0});
    }
};

}  // end of namespace souffle
