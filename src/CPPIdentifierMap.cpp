/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file CPPIdentifierMap.cpp
 *
 * Implementation of a singleton which provides a mapping from strings to unique valid CPP identifiers.
 *
 ***********************************************************************/

#include "CPPIdentifierMap.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <memory>
#include <regex>
#include <utility>

#include <unistd.h>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace souffle {
CPPIdentifierMap& CPPIdentifierMap::getInstance() {
    if (instance == nullptr) {
	instance = new CPPIdentifierMap();
    }
    return *instance;
}

std::string CPPIdentifierMap::getIdentifier(const std::string& name) {
    return getInstance().identifier(name);
}

CPPIdentifierMap::~CPPIdentifierMap() = default;

CPPIdentifierMap::CPPIdentifierMap() {}

const std::string CPPIdentifierMap::identifier(const std::string& name) {
    auto it = identifiers.find(name);
    if (it != identifiers.end()) {
	return it->second;
    }
    // strip leading numbers
    unsigned int i;
    for (i = 0; i < name.length(); ++i) {
	if (isalnum(name.at(i)) || name.at(i) == '_') {
	    break;
	}
    }
    std::string id;
    for (auto ch : std::to_string(identifiers.size() + 1) + '_' + name.substr(i)) {
	// alphanumeric characters are allowed
	if (isalnum(ch)) {
	    id += ch;
	}
	// all other characters are replaced by an underscore, except when
	// the previous character was an underscore as double underscores
	// in identifiers are reserved by the standard
	else if (id.size() == 0 || id.back() != '_') {
	    id += '_';
	}
    }
    // most compilers have a limit of 2048 characters (if they have a limit at all) for
    // identifiers; we use half of that for safety
    id = id.substr(0, 1024);
    identifiers.insert(std::make_pair(name, id));
    return id;
}

// See the CPPIdentifierMap, (it is a singleton class).
CPPIdentifierMap* CPPIdentifierMap::instance = nullptr;
}
