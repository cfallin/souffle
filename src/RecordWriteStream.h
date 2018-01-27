/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RecordWriteStream.h
 *
 ***********************************************************************/

#pragma once

#include "IODirectives.h"
#include "RamTypes.h"
#include "SymbolMask.h"
#include "SymbolTable.h"

#include <vector>

namespace souffle {

class RecordWriteStream {
public:
    RecordWriteStream(const SymbolTable& symbolTable) : symbolTable(symbolTable) {}

    void writeAll(const std::vector<std::vector<RamDomain>> records) {
        auto lease = symbolTable.acquireLock();
        (void)lease;
        for (std::vector<RamDomain> current : records) {
            writeNext(current);
        }
    }
    virtual ~RecordWriteStream() = default;

protected:
    virtual void writeNext(const std::vector<RamDomain>& record);
    const SymbolTable& symbolTable;
};

class RecordWriteStreamFactory {
public:
    virtual std::unique_ptr<RecordWriteStream> getRecordWriter(const SymbolTable& symbolTable, const IODirectives& ioDirectives) = 0;

    virtual const std::string& getName() const = 0;
    virtual ~RecordWriteStreamFactory() = default;
};
} /* namespace souffle */
