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
	size_t hash() const {
	    return (var.val * 77723 + hi.val * 2851 + lo.val);
	}
    };

    static BDDVar MAX_VAR() { return BDDVar::make(UINT64_MAX); }

    struct CachedQuery {
	enum Op { AND, OR, NOT, VAR } op;
	RamDomain a, b;

	bool operator==(const CachedQuery& other) const {
	    return op == other.op && a == other.a && b == other.b;
	}
	size_t hash() const {
	    return static_cast<size_t>(op) * 77723 + a * 2851 + b;
	}
    };

    BDDVec<Node> nodes_;
    BDDMap<Node, BDDValue> nodes_reverse_;
    BDDMap<CachedQuery, BDDValue> cache_;
    std::atomic<uint64_t> next_var_;

    static bool not_terminal(BDDValue v) {
	return v != TRUE() && v != FALSE();
    }

    BDDValue intern(BDDVar var, BDDValue hi, BDDValue lo) {
	if (lo == hi) {
	    return lo;
	}
	Node n { var, hi, lo };
	auto it = nodes_reverse_.lookup(n);
	if (!it.done()) {
	    return it.value();
	} else {
	    BDDValue val = BDDValue::make(nodes_.emplace_back(n));
	    if ((val.val & 0xfffff) == 0) {
		fprintf(stderr, "BDD size: %lu\n", val.val);
	    }
	    nodes_reverse_.insert(std::move(n), BDDValue(val));
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
	CachedQuery q { CachedQuery::VAR, var.as_domain(), 0 };
	auto it = cache_.lookup(q);
	if (!it.done()) {
	    return it.value();
	}
	BDDValue result = intern(var, TRUE(), FALSE());
	cache_.insert(std::move(q), BDDValue(result));
	return result;
    }

    BDDValue make_not(BDDValue a) {
	if (a == TRUE()) {
	    return FALSE();
	}
	if (a == FALSE()) {
	    return TRUE();
	}

	CachedQuery q { CachedQuery::NOT, a.as_domain(), 0 };
	auto it = cache_.lookup(q);
	if (!it.done()) {
	    return it.value();
	}
	BDDValue result = ite(a, FALSE(), TRUE());
	cache_.insert(std::move(q), BDDValue(result));
	return result;
    }
    BDDValue make_not(bool b) {
	return make_not(BDDValue(b));
    }

    BDDValue make_and(BDDValue a, BDDValue b) {
	if (a == TRUE()) {
	    return b;
	}
	if (b == TRUE()) {
	    return a;
	}
	if (a == FALSE()) {
	    return FALSE();
	}
	if (b == FALSE()) {
	    return FALSE();
	}
    if (a == b) {
        return a;
    }

	CachedQuery q { CachedQuery::AND, a.as_domain(), b.as_domain() };
	auto it = cache_.lookup(q);
	if (!it.done()) {
	    return it.value();
	}
	BDDValue result = ite(a, b, FALSE());
	cache_.insert(std::move(q), BDDValue(result));
	return result;
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
	if (a == TRUE()) {
	    return TRUE();
	}
	if (b == TRUE()) {
	    return TRUE();
	}
	if (a == FALSE()) {
	    return b;
	}
	if (b == FALSE()) {
	    return a;
	}
    if (a == b) {
        return a;
    }

	CachedQuery q { CachedQuery::OR, a.as_domain(), b.as_domain() };
	auto it = cache_.lookup(q);
	if (!it.done()) {
	    return it.value();
	}
	BDDValue result = ite(a, TRUE(), b);
	cache_.insert(std::move(q), BDDValue(result));
	return result;
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

	for (size_t i = 2; i < nodes_.size(); i++) {
	    const Node& n = nodes_[i];
	    nodeFile << i << "\t" << n.var.val << "\t" << n.hi.val << "\t" << n.lo.val << "\n";
	}
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
