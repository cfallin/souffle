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
#include <string>
#include <iostream>
#include <fstream>
#include <stdint.h>
#include <assert.h>

#include "RamTypes.h"
#include "BDDContainers.h"
#include "ParallelUtils.h"

namespace souffle {

class BDDValue {
    friend class BDD;
    
    uint64_t val;

    static BDDValue make(uint64_t v) { BDDValue ret; ret.val = v; return ret; }
public:
    BDDValue() : val(0) {}
    BDDValue(bool b);
    static BDDValue from_domain(RamDomain d) { return make(static_cast<uint64_t>(d)); }
    RamDomain as_domain() const { return static_cast<RamDomain>(val); }
    bool operator==(const BDDValue& other) const { return val == other.val; }
    bool operator!=(const BDDValue& other) const { return val != other.val; }
    bool operator<(const BDDValue& other) const { return val < other.val; }
    bool operator>(const BDDValue& other) const { return val > other.val; }
    bool operator<=(const BDDValue& other) const { return val <= other.val; }
    bool operator>=(const BDDValue& other) const { return val >= other.val; }
    friend std::ostream& operator<<(std::ostream& os, const BDDValue& v) {
	os << "BDDValue(" << v.val << ")";
	return os;
    }

    BDDValue atomic_get() const {
	std::atomic<uint64_t>* at = reinterpret_cast<std::atomic<uint64_t>*>(
	    const_cast<uint64_t*>(&val));
	return make(at->load(std::memory_order_acquire));
    }

    bool atomic_cas(BDDValue oldVal, BDDValue newVal) const {
	std::atomic<uint64_t>* at = reinterpret_cast<std::atomic<uint64_t>*>(
	    const_cast<uint64_t*>(&val));
	return at->compare_exchange_weak(oldVal.val, newVal.val, std::memory_order_release);
    }
};
class BDDVar {
    friend class BDD;
    
    uint64_t val;

    static BDDVar make(uint64_t v) { BDDVar ret; ret.val = v; return ret; }
public:
    BDDVar() : val(0) {}
    static BDDVar from_domain(RamDomain d) { return make(static_cast<uint64_t>(d)); }
    RamDomain as_domain() const { return static_cast<RamDomain>(val); }
    bool operator==(const BDDVar& other) const { return val == other.val; }
    bool operator!=(const BDDVar& other) const { return val != other.val; }
    bool operator<(const BDDVar& other) const { return val < other.val; }
    bool operator>(const BDDVar& other) const { return val > other.val; }
    bool operator<=(const BDDVar& other) const { return val <= other.val; }
    bool operator>=(const BDDVar& other) const { return val >= other.val; }
    friend std::ostream& operator<<(std::ostream& os, const BDDVar& v) {
	os << "BDDVar(" << v.val << ")";
	return os;
    }
};

class BDD {
public:
    static BDDValue FALSE() { return BDDValue::make(0); }
    static BDDValue TRUE() { return BDDValue::make(1); }
    static BDDVar NO_VAR() { return BDDVar::make(0); }

    // TODO: re-implement pushed subframes
    struct SubFrame {
	SubFrame(BDD&) {}
	BDDValue ret(BDDValue v) {
	    return v;
	}
    };

private:
    friend class SubFrame;

    struct Node {
        BDDVar var;
        BDDValue hi;
        BDDValue lo;

        bool operator==(const Node& other) const {
            return var == other.var && hi == other.hi && lo == other.lo;
        }
        bool operator<(const Node& other) const {
            return var < other.var || (var == other.var && hi < other.hi) ||
                   (var == other.var && hi == other.hi && lo < other.lo);
        }
    };

    static BDDVar MAX_VAR() { return BDDVar::make(UINT64_MAX); }

    ReadWriteLock lock_;
    BDDNodeVec<Node> nodes_;
    std::map<Node, BDDValue> nodes_reverse_;
    std::atomic<uint64_t> next_var_;

    static bool not_terminal(BDDValue v) {
	return v != TRUE() && v != FALSE();
    }

    BDDValue intern(BDDVar var, BDDValue hi, BDDValue lo) {
	if (lo == hi) {
	    return lo;
	}
	Node n { var, hi, lo };
	lock_.start_read();
	auto it = nodes_reverse_.find(n);
	if (it != nodes_reverse_.end()) {
	    BDDValue v = it->second;
	    lock_.end_read();
	    return v;
	} else {
	    while (!lock_.try_upgrade_to_write()) {}
	    it = nodes_reverse_.find(n);
	    if (it != nodes_reverse_.end()) {
		BDDValue v = it->second;
		lock_.end_write();
		return v;
	    }
	    BDDValue val = BDDValue::make(nodes_.emplace_back(n));
	    nodes_reverse_.insert(it, std::make_pair(n, val));
	    assert(nodes_[val.val] == n);
	    lock_.end_write();
	    return val;
	}
    }

    Node node(BDDValue v) const {
	return nodes_[v.val];
    }

    BDDValue do_restrict(BDDValue func, BDDVar var, bool value) {
	if (func == FALSE() || func == TRUE()) {
	    return func;
	} else {
	    Node n = node(func);
	    if (var < n.var) {
		return func;
	    } else if (var == n.var) {
		return value ? n.hi : n.lo;
	    } else {
		BDDValue hi = do_restrict(n.hi, var, value);
		BDDValue lo = do_restrict(n.lo, var, value);
		return intern(n.var, hi, lo);
	    }
	}
    }

    BDDValue ite(BDDValue cond, BDDValue t, BDDValue f) {
	if (cond == TRUE()) {
	    return t;
	} else if (cond == FALSE()) {
	    return f;
	} else if (t == f) {
	    return t;
	} else if (t == TRUE() && f == FALSE()) {
	    return cond;
	} else {
	    Node cn = node(cond);
	    Node tn = node(t);
	    Node fn = node(f);
	    BDDVar min_var = MAX_VAR();
	    if (not_terminal(cond) && cn.var < min_var) {
		min_var = cn.var;
	    }
	    if (not_terminal(t) && tn.var < min_var) {
		min_var = tn.var;
	    }
	    if (not_terminal(f) && fn.var < min_var) {
		min_var = fn.var;
	    }
	    assert(min_var != MAX_VAR());

	    BDDValue c_hi = do_restrict(cond, min_var, true);
	    BDDValue t_hi = do_restrict(t, min_var, true);
	    BDDValue f_hi = do_restrict(f, min_var, true);
	    BDDValue c_lo = do_restrict(cond, min_var, false);
	    BDDValue t_lo = do_restrict(t, min_var, false);
	    BDDValue f_lo = do_restrict(f, min_var, false);
	    BDDValue hi = ite(c_hi, t_hi, f_hi);
	    BDDValue lo = ite(c_lo, t_lo, f_lo);
	    return intern(min_var, hi, lo);
	}
    }

public:
    BDD() : next_var_(1) {
	// false -- placeholder
	assert(BDDValue::make(nodes_.emplace_back()) == FALSE());
	// true -- placeholder
	assert(BDDValue::make(nodes_.emplace_back()) == TRUE());
    }

    BDDValue make_var(BDDVar var) {
	return intern(var, TRUE(), FALSE());
    }

    BDDValue make_not(BDDValue a) {
	return ite(a, FALSE(), TRUE());
    }
    BDDValue make_not(bool b) {
	return make_not(BDDValue(b));
    }

    BDDValue make_and(BDDValue a, BDDValue b) {
	return ite(a, b, FALSE());
    }
    BDDValue make_and(bool a, bool b) {
	return make_and(BDDValue(a), BDDValue(b));
    }
    BDDValue make_and(BDDValue a, bool b) {
	return make_and(a, BDDValue(b));
    }
    BDDValue make_and(bool a, BDDValue b) {
	return make_and(BDDValue(a), b);
    }

    BDDValue make_or(BDDValue a, BDDValue b) {
	return ite(a, TRUE(), b);
    }
    BDDValue make_or(bool a, bool b) {
	return make_or(BDDValue(a), BDDValue(b));
    }
    BDDValue make_or(BDDValue a, bool b) {
	return make_or(a, BDDValue(b));
    }
    BDDValue make_or(bool a, BDDValue b) {
	return make_or(BDDValue(a), b);
    }

    BDDVar alloc_var() {
	return BDDVar::make(next_var_.fetch_add(1));
    }

    void writeFile(const std::string& nodeFilename) {
	std::ofstream nodeFile(nodeFilename);

	nodeFile << "0\tFALSE\n" << "1\tTRUE\n";

	lock_.start_read();
	for (size_t i = 2; i < nodes_.size(); i++) {
	    const Node& n = nodes_[i];
	    nodeFile << i << "\t" << n.var.val << "\t" << n.hi.val << "\t" << n.lo.val << "\n";
	}
	lock_.end_read();
    }

    bool make_ge(RamDomain a, RamDomain b) {
	return (a >= b);
    }
    bool make_gt(RamDomain a, RamDomain b) {
	return (a > b);
    }
    bool make_le(RamDomain a, RamDomain b) {
	return (a <= b);
    }
    bool make_lt(RamDomain a, RamDomain b) {
	return (a < b);
    }
    bool make_eq(RamDomain a, RamDomain b) {
	return (a == b);
    }
    bool make_ne(RamDomain a, RamDomain b) {
	return (a != b);
    }

    BDDValue make_eq(BDDValue a, BDDValue b) {
	return make_or(make_and(a, b), make_and(make_not(a), make_not(b)));
    }
    BDDValue make_eq(bool a, bool b) {
	return make_eq(BDDValue(a), BDDValue(b));
    }
    BDDValue make_eq(BDDValue a, bool b) {
	return make_eq(a, BDDValue(b));
    }
    BDDValue make_eq(bool a, BDDValue b) {
	return make_eq(BDDValue(a), b);
    }
    BDDValue make_ne(BDDValue a, BDDValue b) {
	return make_or(make_and(a, make_not(b)), make_and(make_not(a), b));
    }
    BDDValue make_ne(bool a, bool b) {
	return make_ne(BDDValue(a), BDDValue(b));
    }
    BDDValue make_ne(BDDValue a, bool b) {
	return make_ne(a, BDDValue(b));
    }
    BDDValue make_ne(bool a, BDDValue b) {
	return make_ne(BDDValue(a), b);
    }
};

inline BDDValue::BDDValue(bool b) : val(b ? BDD::TRUE().val : BDD::FALSE().val) {}

}  // namespace souffle
