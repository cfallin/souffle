/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file CompiledTuple.h
 *
 * The central file covering the data structure utilized by
 * the souffle compiler for representing relations in compiled queries.
 *
 ***********************************************************************/

#pragma once

#include "RamTypes.h"

#include <iostream>

namespace souffle {

namespace ram {

/**
 * The type of object stored within relations representing the actual
 * tuple value. Each tuple consists of a constant number of components.
 *
 * @tparam Domain the domain of the component values
 * @tparam arity the number of components within an instance
 */
template <typename Domain, std::size_t _arity>
struct Tuple {
    // some features for template meta programming
    typedef Domain value_type;
    enum { arity = _arity };

    // the stored data
    Domain data[arity];

    // the predicate, if any (or 0).
    Domain pred;
    // the predicate variable index for this tuple, if any (or 0).
    // NOT included in pred, but rather, implicitly ANDed with it;
    // this is so that a table of Tuples (keyed on data and pred)
    // will correctly dedup the same tuple-with-pred rather than
    // allocate spurious additional copies with new pred vars.
    Domain pred_var;

    Tuple()
	: pred(0), pred_var(0)
    {
	for (int i = 0; i < arity; i++) {
	    data[i] = 0;
	}
    }
    Tuple(const Tuple& other)
	: Tuple()
    {
	*this = other;
    }
    Tuple& operator=(const Tuple& other) {
	pred = other.pred;
	pred_var = other.pred_var;
	for (int i = 0; i < arity; i++) {
	    data[i] = other.data[i];
	}
    }

    // provide access to components
    const Domain& operator[](std::size_t index) const {
        return data[index];
    }

    // provide access to components
    Domain& operator[](std::size_t index) {
        return data[index];
    }

    Domain getPred() const { return pred; }
    void setPred(Domain p) { pred = p; }
    Domain getPredVar() const { return pred_var; }
    void setPredVar(Domain p) { pred_var = p; }

    // a comparison operation
    bool operator==(const Tuple& other) const {
        for (std::size_t i = 0; i < arity; i++) {
            if (data[i] != other.data[i]) return false;
        }
	if (pred != other.pred) return false;
	// pred_var not compared.
        return true;
    }

    // inequality comparison
    bool operator!=(const Tuple& other) const {
        return !(*this == other);
    }

    // required to put tuples into e.g. a std::set container
    bool operator<(const Tuple& other) const {
        for (std::size_t i = 0; i < arity; ++i) {
            if (data[i] < other.data[i]) return true;
            if (data[i] > other.data[i]) return false;
        }
	if (pred < other.pred) return true;
	if (pred > other.pred) return false;
	// pred_var not compared.
        return false;
    }

    // required to put tuples into e.g. a btree container
    bool operator>(const Tuple& other) const {
        for (std::size_t i = 0; i < arity; ++i) {
            if (data[i] > other.data[i]) return true;
            if (data[i] < other.data[i]) return false;
        }
	if (pred > other.pred) return true;
	if (pred < other.pred) return false;
	// pred_var not compared.
        return false;
    }

    // allow tuples to be printed
    friend std::ostream& operator<<(std::ostream& out, const Tuple& tuple) {
        if (arity == 0) return out << "[]";
        out << "[";
        for (std::size_t i = 0; i < (std::size_t)(arity - 1); ++i) {
            out << tuple.data[i];
            out << ",";
        }
        out << tuple.data[arity - 1] << "]";
	if (pred != 0) {
	    out << "@" << pred;
	}
	if (pred_var != 0) {
	    out << "/" << pred_var;
	}
	return out;
    }

    std::string printRaw(std::string& delimiter) {
	std::stringstream ss;
        if (arity == 0) ss << 0;
	else {
	    ss << arity << delimiter;
	    for (std::size_t i = 0; i < (std::size_t)(arity - 1); ++i) {
		ss << data[i];
		ss << delimiter;
	    }
	    ss << data[arity - 1];
	    if (pred != 0) {
		ss << delimiter << "@" << pred;
	    }
	    if (pred_var != 0) {
		ss << delimiter << "/" << pred_var;
	    }
	}
	return ss.str();
    }
};

}  // end namespace ram
}  // end of namespace souffle

// -- add hashing support --

namespace std {

template <typename Domain, std::size_t arity>
struct hash<souffle::ram::Tuple<Domain, arity>> {
    size_t operator()(const souffle::ram::Tuple<Domain, arity>& value) const {
        std::hash<Domain> hash;
        size_t res = 0;
        for (unsigned i = 0; i < arity; i++) {
            // from boost hash combine
            res ^= hash(value[i]) + 0x9e3779b9 + (res << 6) + (res >> 2);
        }
        return res;
    }
};
}  // namespace std
