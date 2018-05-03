/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file CPPIdentifierMap.h
 *
 * A singleton which provides a mapping from strings to unique valid CPP identifiers.
 *
 ***********************************************************************/

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
class CPPIdentifierMap {
public:
    /**
     * Obtains the singleton instance.
     */
    static CPPIdentifierMap& getInstance();

    /**
     * Given a string, returns its corresponding unique valid identifier;
     */
    static std::string getIdentifier(const std::string& name);

    ~CPPIdentifierMap();

private:
    CPPIdentifierMap();

    static CPPIdentifierMap* instance;

    /**
     * Instance method for getIdentifier above.
     */
    const std::string identifier(const std::string& name);

    // The map of identifiers.
    std::map<const std::string, const std::string> identifiers;
};
}
