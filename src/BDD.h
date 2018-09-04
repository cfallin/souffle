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
#include <fstream>
#include <stdint.h>
#include <assert.h>

namespace souffle {

typedef uint64_t BDDValue;
typedef uint64_t BDDVar;

class BDD {
public:
    static const BDDValue FALSE = static_cast<uint64_t>(0);
    static const BDDValue TRUE = static_cast<uint64_t>(1);

    struct SubFrame {
        BDD& bdd;
        std::lock_guard<std::recursive_mutex> guard;
        size_t node_size;
        BDDVar next_var;

        SubFrame(BDD& bdd)
                : bdd(bdd), guard(bdd.lock_), node_size(bdd.nodes_.size()), next_var(bdd.next_var_.load()) {}
        ~SubFrame() {
	    for (size_t i = node_size; i < bdd.nodes_.size(); i++) {
		bdd.nodes_reverse_.erase(bdd.nodes_[i]);
	    }
	    bdd.nodes_.resize(node_size);
	    bdd.next_var_.store(next_var);
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

    static const BDDVar MAX_VAR = UINT64_MAX;

    std::recursive_mutex lock_;
    std::vector<Node> nodes_;
    std::map<Node, BDDValue> nodes_reverse_;
    std::atomic<BDDVar> next_var_;

    static bool not_terminal(BDDValue v) {
	return v != TRUE && v != FALSE;
    }

    BDDValue intern(BDDVar var, BDDValue hi, BDDValue lo) {
	if (lo == hi) {
	    return lo;
	}
	Node n { var, hi, lo };
	auto it = nodes_reverse_.find(n);
	if (it != nodes_reverse_.end()) {
	    return it->second;
	} else {
	    BDDValue val = nodes_.size();
	    nodes_.push_back(n);
	    nodes_reverse_.insert(it, std::make_pair(n, val));
	    return val;
	}
    }

    Node node(BDDValue v) const {
	return nodes_[v];
    }

    BDDValue do_restrict(BDDValue func, BDDVar var, bool value) {
	if (func == FALSE || func == TRUE) {
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
	if (cond == TRUE) {
	    return t;
	} else if (cond == FALSE) {
	    return f;
	} else if (t == f) {
	    return t;
	} else if (t == TRUE && f == FALSE) {
	    return cond;
	} else {
	    Node cn = node(cond);
	    Node tn = node(t);
	    Node fn = node(f);
	    BDDVar min_var = MAX_VAR;
	    if (not_terminal(cond) && cn.var < min_var) {
		min_var = cn.var;
	    }
	    if (not_terminal(t) && tn.var < min_var) {
		min_var = tn.var;
	    }
	    if (not_terminal(f) && fn.var < min_var) {
		min_var = fn.var;
	    }
	    assert(min_var != MAX_VAR);

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
	nodes_.push_back(Node { 0, 0, 0 });
	// true -- placeholder
	nodes_.push_back(Node { 0, 0, 0 });
    }

    BDDValue zero() const {
	return FALSE;
    }
    BDDValue one() const {
	return TRUE;
    }

    BDDValue make_var(BDDVar var) {
	std::lock_guard<std::recursive_mutex> guard(lock_);
	return intern(var, one(), zero());
    }

    BDDValue make_not(BDDValue a) {
	std::lock_guard<std::recursive_mutex> guard(lock_);
	return ite(a, zero(), one());
    }

    BDDValue make_and(BDDValue a, BDDValue b) {
	std::lock_guard<std::recursive_mutex> guard(lock_);
	return ite(a, b, zero());
    }

    BDDValue make_or(BDDValue a, BDDValue b) {
	std::lock_guard<std::recursive_mutex> guard(lock_);
	return ite(a, one(), b);
    }

    BDDVar alloc_var() {
	return next_var_.fetch_add(1);
    }

    void writeFile(const std::string& nodeFilename) {
	std::lock_guard<std::recursive_mutex> guard(lock_);
	std::ofstream nodeFile(nodeFilename);

	nodeFile << "0\tFALSE\n" << "1\tTRUE\n";

	for (size_t i = 2; i < nodes_.size(); i++) {
	    const Node& n = nodes_[i];
	    nodeFile << i << "\t" << n.var << "\t" << n.hi << "\t" << n.lo << "\n";
	}
    }
};

}  // namespace souffle
