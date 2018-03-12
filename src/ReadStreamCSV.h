/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file ReadStreamCSV.h
 *
 ***********************************************************************/

#pragma once

#include "IODirectives.h"
#include "RamTypes.h"
#include "ReadStream.h"
#include "SymbolMask.h"
#include "SymbolTable.h"
#include "Util.h"

#include <fstream>

#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

namespace souffle {

class ReadStreamCSV : public ReadStream {
public:
    ReadStreamCSV(std::vector<std::istream*> files, const SymbolMask& symbolMask, SymbolTable& symbolTable,
            const IODirectives& ioDirectives, const bool provenance = false)
            : ReadStream(symbolMask, symbolTable, provenance), delimiter(getDelimiter(ioDirectives)),
              files(files), fileIdx(0), lineNumber(0), inputMap(getInputColumnMap(ioDirectives, symbolMask.getArity())) {
        while (this->inputMap.size() < symbolMask.getArity()) {
            int size = this->inputMap.size();
            this->inputMap[size] = size;
        }
    }

    ~ReadStreamCSV() override = default;

protected:
    /**
     * Read and return the next tuple.
     *
     * Returns nullptr if no tuple was readable.
     * @return
     */
    std::unique_ptr<RamDomain[]> readNextTuple() override {
	std::istream* file = files[fileIdx];
        if (file->eof()) {
	    // Try next file.
	    fileIdx++;
	    if (fileIdx >= files.size()) {
		return nullptr;
	    }
	    file = files[fileIdx];
        }
        std::string line;
        std::unique_ptr<RamDomain[]> tuple = std::make_unique<RamDomain[]>(symbolMask.getArity());
        bool error = false;

        if (!getline(*file, line)) {
            return nullptr;
        }
        // Handle Windows line endings on non-Windows systems
        if (line.back() == '\r') {
            line = line.substr(0, line.length() - 1);
        }
        ++lineNumber;

        size_t start = 0, end = 0, columnsFilled = 0;
        for (uint32_t column = 0; end < line.length(); column++) {
            end = line.find(delimiter, start);
            if (end == std::string::npos) {
                end = line.length();
            }
            std::string element;
            if (start <= end && end <= line.length()) {
                element = line.substr(start, end - start);
                if (element == "") {
                    element = "n/a";
                }
            } else {
                if (!error) {
                    std::stringstream errorMessage;
                    errorMessage << "Value missing in column " << column + 1 << " in line " << lineNumber
                                 << "; ";
                    throw std::invalid_argument(errorMessage.str());
                }
                element = "n/a";
            }
            start = end + delimiter.size();
            if (inputMap.count(column) == 0) {
                continue;
            }
            ++columnsFilled;
            if (symbolMask.isSymbol(column)) {
                tuple[inputMap[column]] = symbolTable.unsafeLookup(element.c_str());
            } else {
                try {
#if RAM_DOMAIN_SIZE == 64
                    tuple[inputMap[column]] = std::stoll(element.c_str());
#else
                    tuple[inputMap[column]] = std::stoi(element.c_str());
#endif
                } catch (...) {
                    if (!error) {
                        std::stringstream errorMessage;
                        errorMessage << "Error converting number <" + element + "> in column " << column + 1
                                     << " in line " << lineNumber << "; ";
                        throw std::invalid_argument(errorMessage.str());
                    }
                }
            }
        }

        // add two provenance columns
        if (isProvenance) {
            tuple[symbolMask.getArity() - 2] = 0;
            tuple[symbolMask.getArity() - 1] = 0;
            columnsFilled += 2;
        }

        if (columnsFilled != symbolMask.getArity()) {
            std::stringstream errorMessage;
            errorMessage << "Values missing in line " << lineNumber << "; ";
            throw std::invalid_argument(errorMessage.str());
        }
        if (end != line.length()) {
            if (!error) {
                std::stringstream errorMessage;
                errorMessage << "Too many cells in line " << lineNumber << "; ";
                throw std::invalid_argument(errorMessage.str());
            }
        }
        if (error) {
            throw std::invalid_argument("cannot parse fact file");
        }

        return tuple;
    }

    std::string getDelimiter(const IODirectives& ioDirectives) const {
        if (ioDirectives.has("delimiter")) {
            return ioDirectives.get("delimiter");
        }
        return "\t";
    }

    std::map<int, int> getInputColumnMap(const IODirectives& ioDirectives, const unsigned arity) const {
        std::string columnString = "";
        if (ioDirectives.has("columns")) {
            columnString = ioDirectives.get("columns");
        }
        std::map<int, int> inputMap;

        if (!columnString.empty()) {
            std::istringstream iss(columnString);
            std::string mapping;
            int index = 0;
            while (std::getline(iss, mapping, ':')) {
                inputMap[stoi(mapping)] = index++;
            }
            if (inputMap.size() < arity) {
                throw std::invalid_argument("Invalid column set was given: <" + columnString + ">");
            }
        } else {
            while (inputMap.size() < arity) {
                int size = inputMap.size();
                inputMap[size] = size;
            }
        }
        return inputMap;
    }

    const std::string delimiter;
    std::vector<std::istream*> files;
    size_t fileIdx;
    size_t lineNumber;
    std::map<int, int> inputMap;
};

class ReadFileCSV : public ReadStreamCSV {
public:
    ReadFileCSV(const SymbolMask& symbolMask, SymbolTable& symbolTable, const IODirectives& ioDirectives,
            const bool provenance = false)
            : ReadStreamCSV(fileHandlePtrs, symbolMask, symbolTable, ioDirectives, provenance),
              fileHandles(getFileHandles(ioDirectives)), fileHandlePtrs(getFileHandlePtrs(fileHandles)) {}
    /**
     * Read and return the next tuple.
     *
     * Returns nullptr if no tuple was readable.
     * @return
     */
    std::unique_ptr<RamDomain[]> readNextTuple() override {
        try {
            return ReadStreamCSV::readNextTuple();
        } catch (std::exception& e) {
            std::stringstream errorMessage;
            errorMessage << e.what();
            errorMessage << "cannot parse fact file!\n";
            throw std::invalid_argument(errorMessage.str());
        }
    }

    ~ReadFileCSV() override = default;

protected:
    std::vector<std::ifstream> getFileHandles(const IODirectives& ioDirectives) const {
	if (ioDirectives.has("combineSearchPath")) {
	    std::vector<std::ifstream> ret;
	    std::string path = ioDirectives.get("combineSearchPath");
	    while (path.size() > 0) {
		int sep = path.find(':');
		std::string part;
		if (sep == -1) {
		    part = path;
		    path = "";
		} else {
		    part = path.substr(0, sep);
		    path = path.substr(sep + 1);
		}
		std::string thisFile = part + "/" + ioDirectives.getRelationName() + ".facts";
		std::ifstream i(thisFile);
		if (i.good()) {
		    ret.push_back(std::move(i));
		}
	    }
	    return ret;
	} else if (ioDirectives.has("filename")) {
	    std::vector<std::ifstream> ret;
	    ret.push_back(std::ifstream(ioDirectives.get("filename")));
	    return ret;
        } else {
	    std::vector<std::ifstream> ret;
	    ret.push_back(std::ifstream(ioDirectives.getRelationName() + ".facts"));
	    return ret;
	}
    }

    std::vector<std::istream*> getFileHandlePtrs(std::vector<std::ifstream>& v) const {
	std::vector<std::istream*> ret;
	for (auto& i : v) {
	    ret.push_back(&i);
	}
	return ret;
    }
    
    std::vector<std::ifstream> fileHandles;
    std::vector<std::istream*> fileHandlePtrs;
};

class ReadCinCSVFactory : public ReadStreamFactory {
public:
    std::unique_ptr<ReadStream> getReader(const SymbolMask& symbolMask, SymbolTable& symbolTable,
            const IODirectives& ioDirectives, const bool provenance) override {
	std::vector<std::istream*> ifstreams = { &std::cin };
        return std::unique_ptr<ReadStreamCSV>(
	    new ReadStreamCSV(ifstreams, symbolMask, symbolTable, ioDirectives, provenance));
    }
    const std::string& getName() const override {
        static const std::string name = "stdin";
        return name;
    }
    ~ReadCinCSVFactory() override = default;
};

class ReadFileCSVFactory : public ReadStreamFactory {
public:
    std::unique_ptr<ReadStream> getReader(const SymbolMask& symbolMask, SymbolTable& symbolTable,
            const IODirectives& ioDirectives, const bool provenance) override {
        return std::unique_ptr<ReadFileCSV>(
                new ReadFileCSV(symbolMask, symbolTable, ioDirectives, provenance));
    }
    const std::string& getName() const override {
        static const std::string name = "file";
        return name;
    }

    ~ReadFileCSVFactory() override = default;
};

} /* namespace souffle */
