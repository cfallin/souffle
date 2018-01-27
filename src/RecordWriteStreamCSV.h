/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RecordWriteStreamCSV.h
 *
 ***********************************************************************/

#pragma once

#include "IODirectives.h"
#include "SymbolTable.h"
#include "RecordWriteStream.h"
#ifdef USE_LIBZ
#include "gzfstream.h"
#endif

#include <fstream>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

namespace souffle {

class RecordWriteStreamCSV {
protected:
    virtual std::string getDelimiter(const IODirectives& ioDirectives) const {
        if (ioDirectives.has("delimiter")) {
            return ioDirectives.get("delimiter");
        }
        return "\t";
    }
};
/*
class RecordWriteFileCSV : public RecordWriteStreamCSV, public WriteStream {
public:
    RecordWriteFileCSV(const SymbolTable& symbolTable, const IODirectives& ioDirectives)
            : RecordWriteStream(symbolMask, symbolTable, provenance), delimiter(getDelimiter(ioDirectives)),
              file(ioDirectives.getFileName()) {
        if (ioDirectives.has("headers") && ioDirectives.get("headers") == "true") {
            file << ioDirectives.get("attributeNames") << std::endl;
        }
    }

    ~RecordWriteFileCSV() override = default;

protected:
    void writeNextTuple(const RamDomain* tuple) override {
    }

protected:
    const std::string delimiter;
    std::ofstream file;
};

#ifdef USE_LIBZ
class RecordWriteGZipFileCSV : public RecordWriteStreamCSV, public RecordWriteStream {
public:
    RecordWriteGZipFileCSV(const SymbolTable& symbolTable, const IODirectives& ioDirectives)
            : RecordWriteStream(symbolTable), delimiter(getDelimiter(ioDirectives)),
              file(ioDirectives.getFileName()) {
        if (ioDirectives.has("headers") && ioDirectives.get("headers") == "true") {
            file << ioDirectives.get("attributeNames") << std::endl;
        }
    }

    ~RecordWriteGZipFileCSV() override = default;

protected:
    void writeNextTuple(const RamDomain* tuple) override {
    }

    const std::string delimiter;
    gzfstream::ogzfstream file;
};
#endif
*/
 class RecordWriteCoutCSV : public RecordWriteStreamCSV, public RecordWriteStream {
public:
    RecordWriteCoutCSV(const SymbolTable& symbolTable, const IODirectives& ioDirectives)
            : RecordWriteStream(symbolTable), delimiter(getDelimiter(ioDirectives)) {
        std::cout << "---------------\n" << ioDirectives.getRelationName();
        if (ioDirectives.has("headers") && ioDirectives.get("headers") == "true") {
            std::cout << "\n" << ioDirectives.get("attributeNames");
        }
        std::cout << "\n===============\n";
    }

    ~RecordWriteCoutCSV() override {
        std::cout << "===============\n";
    }

protected:
    void writeNext(const std::vector<RamDomain>& record) override {
    }

    const std::string delimiter;
};
 /*
class RecordWriteFileCSVFactory : public RecordsWriteStreamFactory {
public:
    std::unique_ptr<WriteStream> getWriter(const SymbolTable& symbolTable, const IODirectives& ioDirectives) override {
#ifdef USE_LIBZ
        if (ioDirectives.has("compress")) {
            return std::unique_ptr<RecordWriteGZipFileCSV>(
                    new RecordWriteGZipFileCSV(symbolTable, ioDirectives));
        }
#endif
        return std::unique_ptr<RecordWriteFileCSV>(
                new RecordsWriteFileCSV(symbolTable, ioDirectives));
    }
    const std::string& getName() const override {
        static const std::string name = "file";
        return name;
    }
    ~RecordWriteFileCSVFactory() override = default;
};
 */
class RecordWriteCoutCSVFactory : public RecordWriteStreamFactory {
public:
    std::unique_ptr<RecordWriteStream> getRecordWriter(const SymbolTable& symbolTable, const IODirectives& ioDirectives) override {
        return std::unique_ptr<RecordWriteCoutCSV>(new RecordWriteCoutCSV(symbolTable, ioDirectives));
    }
    const std::string& getName() const override {
        static const std::string name = "stdout";
        return name;
    }
    virtual ~RecordWriteCoutCSVFactory() override = default;
};

} /* namespace souffle */
