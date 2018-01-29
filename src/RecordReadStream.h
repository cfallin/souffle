/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file ReadStream.h
 *
 ***********************************************************************/

#pragma once

#include "IODirectives.h"
#include "InterpreterRecords.h"
#include "RamTypes.h"
#include "SymbolMask.h"
#include "SymbolTable.h"

#include <memory>

namespace souffle {

class RecordReadStream {
public:
    RecordReadStream(SymbolTable& symbolTable)
            : symbolTable(symbolTable) {}
    void readAll() {
        auto lease = symbolTable.acquireLock();
        (void)lease;
	size_t arity = -1;
	while(auto recordData = readNextRecord(arity)) {
	    pack(recordData.get(), arity);
	}
    }

    void readIntoSymbolTable() {
	int numSymbols = -1;
	std::unique_ptr<std::string[]> symtabData = readSymbolTable(numSymbols);
	for (int i = 0; i < numSymbols; ++i) {
	    symbolTable.insert(symtabData[i].c_str());
	}
    }

    virtual ~RecordReadStream() = default;

protected:
    virtual std::unique_ptr<RamDomain[]> readNextRecord(size_t& arity) = 0;
    virtual std::unique_ptr<std::string[]> readSymbolTable(int& numSymbols) = 0;
    SymbolTable& symbolTable;
};

class RecordReadStreamFactory {
public:
    virtual std::unique_ptr<RecordReadStream> getReader(SymbolTable& symbolTable, const IODirectives& ioDirectives) = 0;
    virtual const std::string& getName() const = 0;
    virtual ~RecordReadStreamFactory() = default;
};

} /* namespace souffle */
