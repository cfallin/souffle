/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file Synthesiser.cpp
 *
 * Implementation of the C++ synthesiser for RAM programs.
 *
 ***********************************************************************/

#include "Synthesiser.h"
#include "AstLogStatement.h"
#include "AstRelation.h"
#include "AstVisitor.h"
#include "BinaryConstraintOps.h"
#include "BinaryFunctorOps.h"
#include "Global.h"
#include "IOSystem.h"
#include "IndexSetAnalysis.h"
#include "Logger.h"
#include "Macro.h"
#include "RamRelation.h"
#include "RamVisitor.h"
#include "RuleScheduler.h"
#include "SignalHandler.h"
#include "TypeSystem.h"
#include "UnaryFunctorOps.h"
#include "sha1.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <utility>

#include <assert.h>
#include <unistd.h>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace souffle {

/**
 * A singleton which provides a mapping from strings to unique valid CPP identifiers.
 */
class CPPIdentifierMap {
public:
    /**
     * Obtains the singleton instance.
     */
    static CPPIdentifierMap& getInstance() {
        if (instance == nullptr) {
            instance = new CPPIdentifierMap();
        }
        return *instance;
    }

    /**
     * Given a string, returns its corresponding unique valid identifier;
     */
    static std::string getIdentifier(const std::string& name) {
        return getInstance().identifier(name);
    }

    ~CPPIdentifierMap() = default;

private:
    CPPIdentifierMap() {}

    static CPPIdentifierMap* instance;

    /**
     * Instance method for getIdentifier above.
     */
    const std::string identifier(const std::string& name) {
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
        for (auto ch : name.substr(i)) {
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

    // The map of identifiers.
    std::map<const std::string, const std::string> identifiers;
};

// See the CPPIdentifierMap, (it is a singleton class).
CPPIdentifierMap* CPPIdentifierMap::instance = nullptr;

namespace {

static const char ENV_NO_INDEX[] = "SOUFFLE_USE_NO_INDEX";

bool useNoIndex() {
    static bool flag = std::getenv(ENV_NO_INDEX);
    static bool first = true;
    if (first && flag) {
        std::cout << "WARNING: indexes are ignored!\n";
        first = false;
    }
    return flag;
}

// Static wrapper to get relation names without going directly though the CPPIdentifierMap.
static const std::string getRelationName(const RamRelation& rel) {
    return "rel_" + CPPIdentifierMap::getIdentifier(rel.getName());
}

// Static wrapper to get op context names without going directly though the CPPIdentifierMap.
static const std::string getOpContextName(const RamRelation& rel) {
    return getRelationName(rel) + "_op_ctxt";
}

std::string getTupleType(std::size_t arity) {
    std::stringstream res;
    bool predicated = Global::config().has("predicated");
    res << "ram::Tuple<RamDomain, " << (predicated ? (arity + 2) : arity) << ">";
    return res.str();
}

std::string getRecordTupleType(std::size_t arity) {
    std::stringstream res;
    res << "ram::Tuple<RamDomain, " << arity << ">";
    return res.str();
}

std::string getRelationType(const RamRelation& rel, std::size_t arity, const IndexSet& indexes, bool predicated) {
    std::stringstream res;
    res << "ram::Relation";
    res << "<";

    if (rel.isBTree()) {
        res << "BTree,";
    } else if (rel.isRbtset()) {
        res << "Rbtset,";
    } else if (rel.isHashset()) {
        res << "Hashset,";
    } else if (rel.isBrie()) {
        res << "Brie,";
    } else if (rel.isEqRel()) {
        res << "EqRel,";
    } else {
        auto data_structure = Global::config().get("data-structure");
        if (data_structure == "btree") {
            res << "BTree,";
        } else if (data_structure == "rbtset") {
            res << "Rbtset,";
        } else if (data_structure == "hashset") {
            res << "Hashset,";
        } else if (data_structure == "brie") {
            res << "Brie,";
        } else if (data_structure == "eqrel") {
            res << "Eqrel,";
        } else {
            res << "Auto,";
        }
    }

    res << (predicated ? (arity + 2) : arity);

    if (!useNoIndex()) {
        for (auto& cur : indexes.getAllOrders()) {
            res << ", ram::index<";
            res << join(cur, ",");
            res << ">";
        }
    }
    res << ">";
    return res.str();
}

std::string toIndex(SearchColumns key) {
    std::stringstream tmp;
    tmp << "<";
    int i = 0;
    while (key != 0) {
        if (key % 2) {
            tmp << i;
            if (key > 1) {
                tmp << ",";
            }
        }
        key >>= 1;
        i++;
    }

    tmp << ">";
    return tmp.str();
}

std::set<RamRelation> getReferencedRelations(const RamOperation& op) {
    std::set<RamRelation> res;
    visitDepthFirst(op, [&](const RamNode& node) {
        if (auto scan = dynamic_cast<const RamScan*>(&node)) {
            res.insert(scan->getRelation());
        } else if (auto agg = dynamic_cast<const RamAggregate*>(&node)) {
            res.insert(agg->getRelation());
	} else if (auto forall = dynamic_cast<const RamForall*>(&node)) {
	    res.insert(*forall->getDomRelation());
        } else if (auto notExist = dynamic_cast<const RamNotExists*>(&node)) {
            res.insert(notExist->getRelation());
        } else if (auto project = dynamic_cast<const RamProject*>(&node)) {
            res.insert(project->getRelation());
            if (project->hasFilter()) {
                res.insert(project->getFilter());
            }
        }
    });
    return res;
}

class RelationDeclEmitter : public RamVisitor<void> {

};

class CodeEmitter : public RamVisitor<void, std::ostream&> {
// macros to add comments to generated code for debugging
#ifndef PRINT_BEGIN_COMMENT
#define PRINT_BEGIN_COMMENT(os)                                                  \
    if (Global::config().has("debug-report") || Global::config().has("verbose")) \
    os << "/* BEGIN " << __FUNCTION__ << " @" << __FILE__ << ":" << __LINE__ << " */\n"
#endif

#ifndef PRINT_END_COMMENT
#define PRINT_END_COMMENT(os)                                                    \
    if (Global::config().has("debug-report") || Global::config().has("verbose")) \
    os << "/* END " << __FUNCTION__ << " @" << __FILE__ << ":" << __LINE__ << " */\n"
#endif

    std::function<void(std::ostream&, const RamNode*)> rec;
    bool predicated = Global::config().has("predicated");

    // Map from method name to (body, decls) pair
    std::map<std::string, std::pair<std::string, std::string>> separate_methods;
    const std::map<std::string, std::string>& relation_decls;
    int no_separate_method_count;

    struct printer {
        CodeEmitter& p;
        const RamNode& node;

        printer(CodeEmitter& p, const RamNode& n) : p(p), node(n) {}

        printer(const printer& other) = default;

        friend std::ostream& operator<<(std::ostream& out, const printer& p) {
            p.p.visit(p.node, out);
            return out;
        }
    };

public:
    CodeEmitter(const std::map<std::string, std::string>& rel_decls)
	: relation_decls(rel_decls), no_separate_method_count(0) {
        rec = [&](std::ostream& out, const RamNode* node) { this->visit(*node, out); };
    }

    // -- relation statements --

    void visitCreate(const RamCreate& /*create*/, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        PRINT_END_COMMENT(out);
    }

    void visitFact(const RamFact& fact, std::ostream& out) override {
	std::stringstream ss;
	visitFactInternal(fact, ss);
	handleSeparateMethod(out, fact, ss.str());
    }

    void visitFactInternal(const RamFact& fact, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
	std::string pred = predicated ? "BDD::TRUE().as_domain(), BDD::NO_VAR().as_domain()" : "";
	if (predicated && fact.getValues().size() > 0) {
	    pred = ", " + pred;
	}

        out << getRelationName(fact.getRelation()) << "->"
            << "insert(" << join(fact.getValues(), ",", rec) << pred << ");\n";
        PRINT_END_COMMENT(out);
    }

    void visitLoad(const RamLoad& load, std::ostream& out) override {
	std::stringstream ss;
	visitLoadInternal(load, ss);
	handleSeparateMethod(out, load, ss.str());
    }

    void visitLoadInternal(const RamLoad& load, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        out << "if (performIO) {\n";
        // get some table details
        out << "try {";
	out << "extern std::string inputDirectory;\n";
        out << "std::map<std::string, std::string> directiveMap(";
        out << load.getIODirectives() << ");\n";
        out << "if (!inputDirectory.empty() && directiveMap[\"IO\"] == \"file\" && ";
        out << "directiveMap[\"filename\"].front() != '/') {";
        out << "directiveMap[\"filename\"] = inputDirectory + \"/\" + directiveMap[\"filename\"];";
        out << "}\n";
        out << "IODirectives ioDirectives(directiveMap);\n";
        out << "IOSystem::getInstance().getReader(";
        out << "SymbolMask({" << load.getRelation().getSymbolMask() << "})";
        out << ", symTable, ioDirectives";
        out << ", " << Global::config().has("provenance");
	out << ", " << Global::config().has("predicated");
        out << ")->readAll(*" << getRelationName(load.getRelation());
        out << ");\n";
        out << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
        out << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitStore(const RamStore& store, std::ostream& out) override {
	std::stringstream ss;
	visitStoreInternal(store, ss);
	handleSeparateMethod(out, store, ss.str());
    }

    void visitStoreInternal(const RamStore& store, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        out << "if (performIO) {\n";
        for (IODirectives ioDirectives : store.getIODirectives()) {
            out << "try {";
	    out << "extern std::string outputDirectory;\n";
            out << "std::map<std::string, std::string> directiveMap(" << ioDirectives << ");\n";
            out << "if (!outputDirectory.empty() && directiveMap[\"IO\"] == \"file\" && ";
            out << "directiveMap[\"filename\"].front() != '/') {";
            out << "directiveMap[\"filename\"] = outputDirectory + \"/\" + directiveMap[\"filename\"];";
            out << "}\n";
            out << "IODirectives ioDirectives(directiveMap);\n";
            out << "IOSystem::getInstance().getWriter(";
            out << "SymbolMask({" << store.getRelation().getSymbolMask() << "})";
            out << ", symTable, ioDirectives";
            out << ", " << Global::config().has("provenance");
	    out << ", " << Global::config().has("predicated");
            out << ")->writeAll(*" << getRelationName(store.getRelation()) << ");\n";
            out << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
        }
        out << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitInsert(const RamInsert& insert, std::ostream& out) override {
	std::stringstream ss;
	visitInsertInternal(insert, ss);
	handleSeparateMethod(out, insert, ss.str());
    }

    void visitInsertInternal(const RamInsert& insert, std::ostream& out) {
	
        PRINT_BEGIN_COMMENT(out);

        // enclose operation with a check for an empty relation
        std::set<RamRelation> input_relations;
        visitDepthFirst(insert, [&](const RamScan& scan) { input_relations.insert(scan.getRelation()); });
        if (!input_relations.empty()) {
            out << "if (" << join(input_relations, "&&", [&](std::ostream& out, const RamRelation& rel) {
		    out << "!" << getRelationName(rel) << "->"
			<< "empty()";

            }) << ") ";
        }

        // outline each search operation to improve compilation time
        // Disabled to work around issue #345 with clang 3.7-3.9 & omp.
        // out << "[&]()";

        // enclose operation in its own scope
        out << "{\n";

	if (predicated) {
	    out << "BDDValue pred = BDD::TRUE();\n";
	}

        // create proof counters
        if (Global::config().has("profile")) {
            out << "std::atomic<uint64_t> num_failed_proofs(0);\n";
        }

        // check whether loop nest can be parallelized
        bool parallel = false;
        if (const RamScan* scan = dynamic_cast<const RamScan*>(&insert.getOperation())) {
            // if it is a full scan
            if (scan->getRangeQueryColumns() == 0 && !scan->isPureExistenceCheck()) {
                // yes it can!
                parallel = true;

                // partition outermost relation
                out << "auto part = " << getRelationName(scan->getRelation()) << "->"
                    << "partition();\n";

                // build a parallel block around this loop nest
                out << "PARALLEL_START;\n";
            }
        }

        // add local counters
        if (Global::config().has("profile")) {
            out << "uint64_t private_num_failed_proofs = 0;\n";
        }

        // create operation contexts for this operation
        for (const RamRelation& rel : getReferencedRelations(insert.getOperation())) {
            // TODO (#467): this causes bugs for subprogram compilation for record types if artificial
            // dependencies are introduces in the precedence graph
            out << "CREATE_OP_CONTEXT(" << getOpContextName(rel) << "," << getRelationName(rel) << "->"
                << "createContext());\n";
        }

        out << print(insert.getOperation());

        // aggregate proof counters
        if (Global::config().has("profile")) {
            out << "num_failed_proofs += private_num_failed_proofs;\n";
        }

        if (parallel) {
            out << "PARALLEL_END;\n";  // end parallel

            // aggregate proof counters
        }
        if (Global::config().has("profile")) {
            // TODO: this should be moved to AstTranslator, as all other such logging is done there

            // get target relation
            const RamRelation* rel = nullptr;
            visitDepthFirst(insert, [&](const RamProject& project) { rel = &project.getRelation(); });

            // build log message
            auto& clause = insert.getOrigin();
            std::string clauseText = toString(clause);
            replace(clauseText.begin(), clauseText.end(), '"', '\'');
            replace(clauseText.begin(), clauseText.end(), '\n', ' ');

            // print log entry
            out << "{ auto lease = getOutputLock().acquire(); ";
            out << "(void)lease;\n";
            out << "profile << R\"(";
            out << AstLogStatement::pProofCounter(rel->getName(), clause.getSrcLoc(), clauseText);
            out << ")\" << num_failed_proofs << ";
            if (fileExtension(Global::config().get("profile")) == "json") {
                out << "\"},\" << ";
            }
            out << "std::endl;\n";
            out << "}";
        }

        out << "}\n";  // end lambda
        // out << "();";  // call lambda
        PRINT_END_COMMENT(out);
    }

    void visitMerge(const RamMerge& merge, std::ostream& out) override {
	std::stringstream ss;
	visitMergeInternal(merge, ss);
	handleSeparateMethod(out, merge, ss.str());
    }

    void visitMergeInternal(const RamMerge& merge, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        if (merge.getTargetRelation().isEqRel()) {
            out << getRelationName(merge.getSourceRelation()) << "->"
                << "extend("
                << "*" << getRelationName(merge.getTargetRelation()) << ");\n";
        }
	if (predicated) {
	    out << "predHelperMergeWithPredicates(bdd, "
		<< getRelationName(merge.getTargetRelation()) << ", "
		<< getRelationName(merge.getSourceRelation()) << ");\n";
	} else {
	    out << getRelationName(merge.getTargetRelation()) << "->"
		<< "insertAll("
		<< "*" << getRelationName(merge.getSourceRelation()) << ");\n";
	}
        PRINT_END_COMMENT(out);
    }

    void visitClear(const RamClear& clear, std::ostream& out) override {
	std::stringstream ss;
	visitClearInternal(clear, ss);
	handleSeparateMethod(out, clear, ss.str());
    }

    void visitClearInternal(const RamClear& clear, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        out << getRelationName(clear.getRelation()) << "->"
            << "purge();\n";
        PRINT_END_COMMENT(out);
    }

    void visitDrop(const RamDrop& drop, std::ostream& out) override {
	std::stringstream ss;
	visitDropInternal(drop, ss);
	handleSeparateMethod(out, drop, ss.str());
    }

    void visitDropInternal(const RamDrop& drop, std::ostream& out)  {
        PRINT_BEGIN_COMMENT(out);
        out << "if (!isHintsProfilingEnabled() && (performIO || " << drop.getRelation().isTemp() << ")) ";
        out << getRelationName(drop.getRelation()) << "->"
            << "purge();\n";
        PRINT_END_COMMENT(out);
    }

    void visitPrintSize(const RamPrintSize& print, std::ostream& out) override {
	std::stringstream ss;
	visitPrintSizeInternal(print, ss);
	handleSeparateMethod(out, print, ss.str());
    }

    void visitPrintSizeInternal(const RamPrintSize& print, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        out << "if (performIO) {\n";
        out << "{ auto lease = getOutputLock().acquire(); \n";
        out << "(void)lease;\n";
        out << "std::cout << R\"(" << print.getMessage() << ")\" <<  ";
        out << getRelationName(print.getRelation()) << "->"
            << "size() << std::endl;\n";
        out << "}";
        out << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitLogSize(const RamLogSize& print, std::ostream& out) override {
	std::stringstream ss;
	visitLogSizeInternal(print, ss);
	handleSeparateMethod(out, print, ss.str());
    }

    void visitLogSizeInternal(const RamLogSize& print, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        const std::string ext = fileExtension(Global::config().get("profile"));
        out << "{ auto lease = getOutputLock().acquire(); \n";
        out << "(void)lease;\n";
        out << "profile << R\"(" << print.getMessage() << ")\" << ";
        out << getRelationName(print.getRelation()) << "->size() << ";
        if (ext == "json") {
            out << "\"},\" << ";
        }
        out << "std::endl;\n";
        out << "}";
        PRINT_END_COMMENT(out);
    }

    // -- control flow statements --

    void visitSequence(const RamSequence& seq, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        for (const auto& cur : seq.getStatements()) {
            out << print(cur);
        }
        PRINT_END_COMMENT(out);
    }

    void visitParallel(const RamParallel& parallel, std::ostream& out) override {
	std::stringstream ss;
	visitParallelInternal(parallel, ss);
	handleSeparateMethod(out, parallel, ss.str());
    }

    void visitParallelInternal(const RamParallel& parallel, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        auto stmts = parallel.getStatements();

        // special handling cases
        if (stmts.empty()) {
            return;
            PRINT_END_COMMENT(out);
        }

        // a single statement => save the overhead
        if (stmts.size() == 1) {
            out << print(stmts[0]);
            return;
            PRINT_END_COMMENT(out);
        }

        // more than one => parallel sections

        // start parallel section
        out << "SECTIONS_START;\n";

        // put each thread in another section
        for (const auto& cur : stmts) {
            out << "SECTION_START;\n";
            out << print(cur);
            out << "SECTION_END\n";
        }

        // done
        out << "SECTIONS_END;\n";
        PRINT_END_COMMENT(out);
    }

    void visitLoop(const RamLoop& loop, std::ostream& out) override {
	std::stringstream ss;
	visitLoopInternal(loop, ss);
	handleSeparateMethod(out, loop, ss.str());
    }

    void visitLoopInternal(const RamLoop& loop, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        out << "for(;;) {\n" << print(loop.getBody()) << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitSwap(const RamSwap& swap, std::ostream& out) override {
	std::stringstream ss;
	visitSwapInternal(swap, ss);
	handleSeparateMethod(out, swap, ss.str());
    }

    void visitSwapInternal(const RamSwap& swap, std::ostream& out) {
        PRINT_BEGIN_COMMENT(out);
        const std::string tempKnowledge = "rel_0";
        const std::string& deltaKnowledge = getRelationName(swap.getFirstRelation());
        const std::string& newKnowledge = getRelationName(swap.getSecondRelation());

        // perform a triangular swap of pointers
        out << "{\nauto " << tempKnowledge << " = " << deltaKnowledge << ";\n"
            << deltaKnowledge << " = " << newKnowledge << ";\n"
            << newKnowledge << " = " << tempKnowledge << ";\n"
            << "}\n";
        PRINT_END_COMMENT(out);
    }

    // For differential debugging (useful for "big features",
    // e.g. predication): print the sizes of relations in each
    // iteration of a corecursive loop of rules, in order to compare
    // two or more runs.
    void printDebugSize(const RamCondition& cond, std::ostream& out) {
	if (const RamAnd* a = dynamic_cast<const RamAnd*>(&cond)) {
	    printDebugSize(a->getLHS(), out);
	    printDebugSize(a->getRHS(), out);
	} else if (const RamEmpty* e = dynamic_cast<const RamEmpty*>(&cond)) {
	    out << "std::cout << \"size(" << getRelationName(e->getRelation()) << ") = \" << "
		<< getRelationName(e->getRelation()) << "->size() << \"\\n\";\n";
	}
    }

    void printDebugSizes(const RamExit& exit, std::ostream& out) {
	out << "std::cout << \"---- loop exit condition: ----\\n\";\n";
	printDebugSize(exit.getCondition(), out);
	out << "std::cout << \"---- end loop exit condition ----\\n\";\n";
    }

    void visitExit(const RamExit& exit, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
	//printDebugSizes(exit, out);
	if (predicated) {
	    out << "if(" << print(exit.getCondition()) << " == BDD::TRUE()) break;\n";
	} else {
	    out << "if(" << print(exit.getCondition()) << ") break;\n";
	}
        PRINT_END_COMMENT(out);
    }

    void visitLogTimer(const RamLogTimer& timer, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        // create local scope for name resolution
        out << "{\n";

        const std::string ext = fileExtension(Global::config().get("profile"));

        // create local timer
        out << "\tLogger logger(R\"(" << timer.getMessage() << ")\",profile, \"" << ext << "\");\n";

        // insert statement to be measured
        visit(timer.getStatement(), out);

        // done
        out << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitDebugInfo(const RamDebugInfo& dbg, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        out << "SignalHandler::instance()->setMsg(R\"_(";
        out << dbg.getMessage();
        out << ")_\");\n";

        // insert statements of the rule
        visit(dbg.getStatement(), out);
        PRINT_END_COMMENT(out);
    }

    // -- operations --

    void visitSearch(const RamSearch& search, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        auto condition = search.getCondition();
        if (condition) {
	    if (predicated) {
		out << "BDDValue cond = " << print(condition) << ";\n";
		out << "if (cond != BDD::FALSE()) {\n";
		out << "BDDValue old_pred = pred;\n";
		out << "BDDValue pred = bdd.make_and(old_pred, cond);\n";
		out << "if (pred != BDD::FALSE()) {\n";
		out << print(search.getNestedOperation());
		out << "}\n";
		out << "}\n";
	    } else {
		out << "if( " << print(condition) << ") {\n" << print(search.getNestedOperation()) << "}\n";
	    }
	    if (Global::config().has("profile")) {
		out << " else { ++private_num_failed_proofs; }";
	    }
        } else {
	    if (predicated) {
		out << "if (pred != BDD::FALSE()) {\n";
		out << print(search.getNestedOperation());
		out << "}\n";
	    } else {
		out << print(search.getNestedOperation());
	    }
        }
        PRINT_END_COMMENT(out);
    }

    void visitScan(const RamScan& scan, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        // get relation name
        const auto& rel = scan.getRelation();
        auto relName = getRelationName(rel);
        auto ctxName = "READ_OP_CONTEXT(" + getOpContextName(rel) + ")";
        auto level = scan.getLevel();

        // if this search is a full scan
        if (scan.getRangeQueryColumns() == 0) {
            if (scan.isPureExistenceCheck()) {
		if (predicated) {
		    out << "{";
		    out << "BDDValue new_pred = bdd.make_not(predHelperEmpty(bdd, *" << relName << ", pred));\n";
		    if (scan.isHypFilter()) {
			out << "if (new_pred == BDD::TRUE()) {\n";
		    } else {
			out << "if (new_pred != BDD::FALSE()) {\n";
		    }
		    out << "BDDValue pred = new_pred;\n";
		} else {
		    out << "if(!" << relName << "->"
			<< "empty()) {\n";
		}
                visitSearch(scan, out);
                out << "}\n";
		if (predicated) {
		    out << "}\n";
		}
            } else if (scan.getLevel() == 0) {
                // make this loop parallel
                out << "pfor(auto it = part.begin(); it<part.end(); ++it) \n";
                out << "try{";
		if (predicated) {
		    out << "BDDValue old_pred = pred;\n";
		}
                out << "for(const auto& env0 : *it) {\n";
		if (predicated) {
		    out << "BDDValue old_pred = pred;\n";
		    out << "BDDValue pred = predHelperTuple(bdd, old_pred, env0);\n";
		    if (scan.isHypFilter()) {
			out << "if (pred != BDD::TRUE()) { continue; }\n";
		    }
		}
                visitSearch(scan, out);
                out << "}\n";
                out << "} catch(std::exception &e) { SignalHandler::instance()->error(e.what());}\n";
            } else {
                out << "for(const auto& env" << level << " : "
                    << "*" << relName << ") {\n";
		if (predicated) {
		    out << "BDDValue old_pred = pred;\n";
		    out << "BDDValue pred = predHelperTuple(bdd, old_pred, env" << level << ");\n";
		    if (scan.isHypFilter()) {
			out << "if (pred != BDD::TRUE()) { continue; }\n";
		    }
		}
                visitSearch(scan, out);
                out << "}\n";
            }
	    PRINT_END_COMMENT(out);
            return;
        }

        // check list of keys
        auto arity = rel.getArity();
        const auto& rangePattern = scan.getRangePattern();

        // a lambda for printing boundary key values
        auto printKeyTuple = [&]() {
            for (size_t i = 0; i < arity; i++) {
                if (rangePattern[i] != nullptr) {
                    out << this->print(rangePattern[i]);
                } else {
                    out << "0";
                }
                if (i + 1 < arity) {
                    out << ",";
                }
            }
	    if (predicated) {
		out << ",0,0";
	    }
        };

        // get index to be queried
        auto keys = scan.getRangeQueryColumns();
        auto index = toIndex(keys);

        // if it is a equality-range query
        out << "const " << getTupleType(arity) << " key({";
        printKeyTuple();
        out << "});\n";
        out << "auto range = " << relName << "->"
            << "equalRange" << index << "(key," << ctxName << ");\n";
        if (Global::config().has("profile")) {
            out << "if (range.empty()) ++private_num_failed_proofs;\n";
        }
        if (scan.isPureExistenceCheck()) {
	    if (predicated) {
		out << "if(!range.empty()) {\n";
		out << "BDDValue old_pred = pred;\n";
		out << "BDDValue pred = bdd.make_not(predHelperRangeEmpty(bdd, range, old_pred));\n";
		if (scan.isHypFilter()) {
		    out << "if (pred == BDD::TRUE()) {\n";
		} else {
		    out << "if (pred != BDD::FALSE()) {\n";
		}
		visitSearch(scan, out);
		out << "}\n";
		out << "}\n";
	    } else {
		out << "if(!range.empty()) {\n";
		visitSearch(scan, out);
		out << "}\n";
	    }
        } else {
	    if (predicated) {
		out << "for(const auto& env" << level << " : range) {\n";
		out << "BDDValue old_pred = pred;\n";
		out << "BDDValue pred = predHelperTuple(bdd, old_pred, env" << level << ");\n";
		if (scan.isHypFilter()) {
		    out << "if (pred == BDD::TRUE()) {\n";
		} else {
		    out << "if (pred != BDD::FALSE()) {\n";
		}
		visitSearch(scan, out);
		out << "}\n";
		out << "}\n";
	    } else {
		out << "for(const auto& env" << level << " : range) {\n";
		visitSearch(scan, out);
		out << "}\n";
	    }
        }
        PRINT_END_COMMENT(out);
    }

    void visitLookup(const RamLookup& lookup, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        auto arity = lookup.getArity();

        // get the tuple type working with
        std::string tuple_type = getTupleType(arity);

        // look up reference
        out << "auto ref = env" << lookup.getReferenceLevel() << "[" << lookup.getReferencePosition()
            << "];\n";
        out << "if (isNull<" << tuple_type << ">(ref)) continue;\n";
        out << tuple_type << " env" << lookup.getLevel() << " = unpack<" << tuple_type << ">(ref);\n";

        out << "{\n";

        // continue with condition checks and nested body
        visitSearch(lookup, out);

        out << "}\n";
        PRINT_END_COMMENT(out);
    }

    void visitForallContext(const RamForallContext& fctx, std::ostream& out) override {
	out << "{\n";

	std::vector<std::string> keyColList;
	for (size_t col = 0; col < fctx.getArity(); col++) {
	    bool isKey = fctx.getKeyCols() & (1L << col);
	    if (isKey) {
		keyColList.push_back(toString(col));
	    }
	}
	size_t arity = fctx.getArity();
	if (predicated) {
	    arity += 2;
	}
	out << "ram::Relation<Auto, " << arity << ", ram::index<" << join(keyColList, ", ") << ">> forallValsByKey;\n";
	out << "CREATE_OP_CONTEXT(forallValsByKeyOpCtxt, forallValsByKey.createContext());\n";
	out << "std::mutex forallValsByKeyLock;\n";

	no_separate_method_count++;
	visit(*fctx.getNested(), out);
	no_separate_method_count--;

	out << "}\n";
    }

    void visitForall(const RamForall& forall, std::ostream& out) override {
	PRINT_BEGIN_COMMENT(out);
        const auto* domRel = forall.getDomRelation();
        auto arity = domRel->getArity();
        auto relName = getRelationName(*domRel);
        auto ctxName = "READ_OP_CONTEXT(" + getOpContextName(*domRel) + ")";
        auto level = forall.getLevel();

	// Construct the tuple being projected at this level.
	std::string tuple_type = getTupleType(arity);
	out << tuple_type << " env" << level << "({\n";
	for (const auto* arg : forall.getArgs()) {
	    out << this->print(arg) << ",\n";
	}
	out << "});\n";

	std::string keyRange;
	std::string keyVals;
	size_t keyArity = 0;
	bool first = true;
	for (size_t col = 0; col < arity; col++) {
	    if  (forall.getKeyColumns() & (1L << col)) {
		if (first) {
		    first = false;
		} else {
		    keyRange += ", ";
		}
		keyRange += toString(col);
		keyVals += "env" + toString(level) + "[" + toString(col) + "], ";
		keyArity++;
	    } else {
		keyVals += "0, ";
	    }
	}

	out << tuple_type << " forallKey({" << keyVals << "});\n";

	// Step 0: probe the domain relation by the specific tuple to
	// see if this tuple is in the domain. Skip the rest if not.
	out << "if (" << relName << "->contains(env" << level << ")) {\n";

	// Step 1: insert into forallValsByKey.
	out << "forallValsByKeyLock.lock();\n";
	out << "forallValsByKey.insert(env" << level << ", READ_OP_CONTEXT(forallValsByKeyOpCtxt));\n";

	// Step 2: probe the values-by-key relation and get the count
	// for this key.
	out << "auto valRange = forallValsByKey.equalRange<" << keyRange << ">(" <<
	    "forallKey, READ_OP_CONTEXT(forallValsByKeyOpCtxt));\n";
	out << "size_t valCount = 0;\n";
	out << "for (const auto& t : valRange) { valCount++; }\n";
	out << "forallValsByKeyLock.unlock();\n";

	// Step 3: probe the domain relation and get the count for
	// this key.
	out << "auto domRange = " << relName << "->equalRange<" << keyRange << ">(" <<
	    "forallKey, " << ctxName << ");\n";
	out << "size_t domCount = 0;\n";
	out << "for (const auto& t : domRange) { domCount++; if (domCount > valCount) break; }\n";

	// Step 4: if the value count is equal to the domain count,
	// then execute the nested operation.
	out << "if (valCount == domCount) {\n";
	visit(*forall.getNested(), out);
	out << "}\n";

	out << "}\n";  // close step 0's conditional.
    }

    void visitAggregate(const RamAggregate& aggregate, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        // get some properties
        const auto& rel = aggregate.getRelation();
        auto arity = rel.getArity();
        auto relName = getRelationName(rel);
        auto ctxName = "READ_OP_CONTEXT(" + getOpContextName(rel) + ")";
        auto level = aggregate.getLevel();

        // get the tuple type working with
        std::string tuple_type = getTupleType(arity);

        // declare environment variable
        out << tuple_type << " env" << level << ";\n";

        // special case: counting of number elements in a full relation
        if (aggregate.getFunction() == RamAggregate::COUNT && aggregate.getRangeQueryColumns() == 0) {
            // shortcut: use relation size
            out << "env" << level << "[0] = " << relName << "->"
                << "size();\n";
            visitSearch(aggregate, out);
            PRINT_END_COMMENT(out);
            return;
        }

        // init result
        std::string init;
        switch (aggregate.getFunction()) {
            case RamAggregate::MIN:
                init = "MAX_RAM_DOMAIN";
                break;
            case RamAggregate::MAX:
                init = "MIN_RAM_DOMAIN";
                break;
            case RamAggregate::COUNT:
                init = "0";
                break;
            case RamAggregate::SUM:
                init = "0";
                break;
            case RamAggregate::PRODUCT:
                init = "1";
                break;
        }
        out << "RamDomain res = " << init << ";\n";

        // get range to aggregate
        auto keys = aggregate.getRangeQueryColumns();

        // check whether there is an index to use
        if (keys == 0) {
            // no index => use full relation
            out << "auto& range = "
                << "*" << relName << ";\n";
        } else {
            // a lambda for printing boundary key values
            auto printKeyTuple = [&]() {
                for (size_t i = 0; i < arity; i++) {
                    if (aggregate.getPattern()[i] != nullptr) {
                        out << this->print(aggregate.getPattern()[i]);
                    } else {
                        out << "0";
                    }
                    if (i + 1 < arity) {
                        out << ",";
                    }
                }
            };

            // get index
            auto index = toIndex(keys);
            out << "const " << tuple_type << " key({";
            printKeyTuple();
            out << "});\n";
            out << "auto range = " << relName << "->"
                << "equalRange" << index << "(key," << ctxName << ");\n";
        }

        // add existence check
        if (aggregate.getFunction() != RamAggregate::COUNT) {
            out << "if(!range.empty()) {\n";
        }

        // aggregate result
        out << "for(const auto& cur : range) {\n";

        // create aggregation code
        if (aggregate.getFunction() == RamAggregate::COUNT) {
            // count is easy
            out << "++res\n;";
        } else if (aggregate.getFunction() == RamAggregate::SUM) {
            out << "env" << level << " = cur;\n";
            out << "res += ";
            visit(*aggregate.getTargetExpression(), out);
            out << ";\n";
        } else if (aggregate.getFunction() == RamAggregate::PRODUCT) {
            out << "env" << level << " = cur;\n";
            out << "res *= ";
            visit(*aggregate.getTargetExpression(), out);
            out << ";\n";
        } else {
            // pick function
            std::string fun = "min";
            switch (aggregate.getFunction()) {
                case RamAggregate::MIN:
                    fun = "std::min";
                    break;
                case RamAggregate::MAX:
                    fun = "std::max";
                    break;
                case RamAggregate::COUNT:
                    assert(false);
                case RamAggregate::SUM:
                    assert(false);
                case RamAggregate::PRODUCT:
                    assert(false);
            }

            out << "env" << level << " = cur;\n";
            out << "res = " << fun << "(res,";
            visit(*aggregate.getTargetExpression(), out);
            out << ");\n";
        }

        // end aggregator loop
        out << "}\n";

        // write result into environment tuple
        out << "env" << level << "[0] = res;\n";

        // continue with condition checks and nested body
        out << "{\n";

        auto condition = aggregate.getCondition();
        if (condition) {
            out << "if( " << print(condition) << ") {\n";
            visitSearch(aggregate, out);
            out << "}\n";
            if (Global::config().has("profile")) {
                out << " else { ++private_num_failed_proofs; }";
            }
        } else {
            visitSearch(aggregate, out);
        }

        out << "}\n";

        // end conditional nested block
        if (aggregate.getFunction() != RamAggregate::COUNT) {
            out << "}\n";
        }
        PRINT_END_COMMENT(out);
    }

    void visitFindDuplicate(const RamFindDuplicate& finddup, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);

	assert(!predicated);  // predicated mode not supported (yet)

	auto dupVars = finddup.getDupVars();
	auto givenVars = finddup.getGivenVars();
	int arity = givenVars.size() + dupVars.size();
	auto rel = finddup.getSrcRelation();

	// Create the streaming state: previous tuple, count for these
	// given-vars.
	out << getTupleType(arity) << " finddup_prev_tuple;\n";
	out << getTupleType(arity) << " finddup_this_tuple;\n";
	out << "bool finddup_prev_tuple_init = false;\n";
	out << "bool finddup_dup_emitted = false;\n";

	// Create the main loop over the src relation.
	out << "for (const auto &env0 : *" << getRelationName(*rel) << ") {\n";

	// Construct the current tuple.
	for (size_t i = 0; i < givenVars.size(); i++) {
	    out << "finddup_this_tuple[" << i << "] = env0[" << givenVars[i] << "];\n";
	}
	for (size_t i = 0; i < dupVars.size(); i++) {
	    out << "finddup_this_tuple[" << (givenVars.size() + i) << "] = env0[" <<
		dupVars[i] << "];\n";
	}

	// If this tuple has all of the same given-vars as the previous,
	// but at least one of the dup-vars is different, then emit the
	// tuple (run nested statement).
	std::vector<std::string> givenSameConds;
	std::vector<std::string> dupDifferentConds;
	for (size_t i = 0; i < givenVars.size(); i++) {
	    std::ostringstream os;
	    os << "(finddup_this_tuple[" << i
	       << "] == finddup_prev_tuple[" << i << "])";
	    givenSameConds.push_back(os.str());
	}
	for (size_t i = 0; i < dupVars.size(); i++) {
	    std::ostringstream os;
	    os << "(finddup_this_tuple["
	       << (givenVars.size() + i) << "] != finddup_prev_tuple["
	       << (givenVars.size() + i) << "])";
	    dupDifferentConds.push_back(os.str());
	}

	// If this 'given' is same as last...
	out << "if (finddup_prev_tuple_init && (" << join(givenSameConds, " && ") << ")) {\n";
	// If the 'dup' vars are different and we haven't invoked the nested insn yet...
	out << "if (!finddup_dup_emitted && (" << join(dupDifferentConds, " || ") << ")) {\n";
	out << "finddup_dup_emitted = true;\n";
	visit(*finddup.getNested(), out);
	out << "}\n";
	// Otherwise, given is different than the last
	out << "} else {\n";
	out << "finddup_dup_emitted = false;\n";
	out << "}\n";
	out << "finddup_prev_tuple = finddup_this_tuple;\n";
	out << "finddup_prev_tuple_init = true;\n";

	// End main loop.
	out << "}\n";

        PRINT_END_COMMENT(out);
    }

    void visitProject(const RamProject& project, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        const auto& rel = project.getRelation();
        auto arity = rel.getArity();
        auto relName = getRelationName(rel);
        auto ctxName = "READ_OP_CONTEXT(" + getOpContextName(rel) + ")";

	if (predicated) {
	    arity += 2;
	}

        // check condition
        auto condition = project.getCondition();
	if (condition) {
	    if (predicated) {
		out << "BDDValue old_pred = pred;\n";
		out << "BDDValue cond = " << print(condition) << ";\n";
		out << "BDDValue pred = bdd.make_and(old_pred, cond);\n";
		out << "if (pred != BDD::FALSE()) {\n";
	    } else {
		out << "if (" << print(condition) << ") {\n";
	    }
	}

        // create projected tuple
        if (project.getValues().empty()) {
            out << "Tuple<RamDomain," << arity << "> tuple({});\n";
        } else {
            out << "Tuple<RamDomain," << arity << "> tuple({(RamDomain)("
                << join(project.getValues(), "),(RamDomain)(", rec) << ")});\n";
        }
	if (predicated) {
	    if (project.getHypothetical()) {
		out << "BDDVar tuple_var = bdd.alloc_var();\n";
		out << "tuple[" << (arity - 1) << "] = tuple_var.as_domain();\n";
		out << "tuple[" << (arity - 2) << "] = (bdd.make_and(pred, bdd.make_var(tuple_var))).as_domain();\n";
	    } else {
		out << "tuple[" << (arity - 2) << "] = pred.as_domain();\n";
	    }
	}

	// check filter
        if (project.hasFilter()) {
            auto relFilter = getRelationName(project.getFilter());
            auto ctxFilter = "READ_OP_CONTEXT(" + getOpContextName(project.getFilter()) + ")";
	    if (predicated) {
		out << "BDDValue new_pred = predHelperContains(bdd, " << relFilter << ", tuple, " << ", pred, " << ctxFilter << ")) {\n";
		out << "if (new_pred != BDD::FALSE()) {\n";
		out << "BDDValue pred = new_pred;\n";
	    } else {
		out << "if (!" << relFilter << ".contains(tuple," << ctxFilter << ")) {\n";
	    }
        }

        // insert tuple
	if (predicated) {
	    out << "predHelperInsert(bdd, " << relName << ", tuple, " << ctxName << ");\n";
	} else {
	    if (Global::config().has("profile")) {
		out << "if (!(" << relName << "->"
		    << "insert(tuple," << ctxName << "))) { ++private_num_failed_proofs; }\n";
	    } else {
		out << relName << "->"
		    << "insert(tuple," << ctxName << ");\n";
	    }
	}

        // end filter
        if (project.hasFilter()) {
            out << "}";

            // add fail counter
            if (Global::config().has("profile")) {
                out << " else { ++private_num_failed_proofs; }";
            }
        }

        // end condition
        if (condition) {
            out << "}\n";

            // add fail counter
            if (Global::config().has("profile")) {
                out << " else { ++private_num_failed_proofs; }";
            }
        }
        PRINT_END_COMMENT(out);
    }

    // -- conditions --

    void visitAnd(const RamAnd& c, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
	if (predicated) {
	    out << "bdd.make_and((" << print(c.getLHS()) << "), (" << print(c.getRHS()) << "))";
	} else {
	    out << "((" << print(c.getLHS()) << ") && (" << print(c.getRHS()) << "))";
	}
        PRINT_END_COMMENT(out);
    }

    void visitBinaryRelation(const RamBinaryRelation& rel, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);

	switch (rel.getOperator()) {
	    // comparison operators
	case BinaryConstraintOp::EQ:
	    out << "((" << print(rel.getLHS()) << ") == (" << print(rel.getRHS()) << "))";
	    break;
	case BinaryConstraintOp::NE:
	    out << "((" << print(rel.getLHS()) << ") != (" << print(rel.getRHS()) << "))";
	    break;
	case BinaryConstraintOp::LT:
	    out << "((" << print(rel.getLHS()) << ") < (" << print(rel.getRHS()) << "))";
	    break;
	case BinaryConstraintOp::LE:
	    out << "((" << print(rel.getLHS()) << ") <= (" << print(rel.getRHS()) << "))";
	    break;
	case BinaryConstraintOp::GT:
	    out << "((" << print(rel.getLHS()) << ") > (" << print(rel.getRHS()) << "))";
	    break;
	case BinaryConstraintOp::GE:
	    out << "((" << print(rel.getLHS()) << ") >= (" << print(rel.getRHS()) << "))";
	    break;

	    // strings
	case BinaryConstraintOp::MATCH: {
	    out << "regex_wrapper(symTable.resolve((size_t)";
	    out << print(rel.getLHS());
	    out << "),symTable.resolve((size_t)";
	    out << print(rel.getRHS());
	    out << "))";
	    break;
	}
	case BinaryConstraintOp::NOT_MATCH: {
	    out << "!regex_wrapper(symTable.resolve((size_t)";
	    out << print(rel.getLHS());
	    out << "),symTable.resolve((size_t)";
	    out << print(rel.getRHS());
	    out << "))";
	    break;
	}
	case BinaryConstraintOp::CONTAINS: {
	    out << "(std::string(symTable.resolve((size_t)";
	    out << print(rel.getRHS());
	    out << ")).find(symTable.resolve((size_t)";
	    out << print(rel.getLHS());
	    out << "))!=std::string::npos)";
	    break;
	}
	case BinaryConstraintOp::NOT_CONTAINS: {
	    out << "(std::string(symTable.resolve((size_t)";
	    out << print(rel.getRHS());
	    out << ")).find(symTable.resolve((size_t)";
	    out << print(rel.getLHS());
	    out << "))==std::string::npos)";
	    break;
	}
	default:
	    assert(0 && "Unsupported Operation!");
	    break;
	}
        PRINT_END_COMMENT(out);
    }

    void visitEmpty(const RamEmpty& empty, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
	if (predicated) {
	    out << "predHelperEmpty(bdd, *" << getRelationName(empty.getRelation()) << ", pred)";
	} else {
	    out << getRelationName(empty.getRelation()) << "->"
		<< "empty()";
	}
        PRINT_END_COMMENT(out);
    }

    void visitNotExists(const RamNotExists& ne, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);

        // get some details
        const auto& rel = ne.getRelation();
        auto relName = getRelationName(rel);
        auto ctxName = "READ_OP_CONTEXT(" + getOpContextName(rel) + ")";
        auto arity = rel.getArity();
	if (predicated) {
	    arity += 2;
	}

        // if it is total we use the contains function
        if (ne.isTotal()) {
	    if (predicated) {
		out << "predHelperNotExists(bdd, *" << relName
		    << ", Tuple<RamDomain," << arity << ">({" << join(ne.getValues(), ",", rec)
		    << "}), pred, " << ctxName << ")";
	    } else {
		out << "!" << relName << "->"
		    << "contains(Tuple<RamDomain," << arity << ">({" << join(ne.getValues(), ",", rec) << "}),"
		    << ctxName << ")";
	    }
	    PRINT_END_COMMENT(out);
            return;
        }

        // else we conduct a range query
	if (predicated) {
	    out << "predHelperRangeEmpty(bdd, ";
	}
        out << relName << "->"
            << "equalRange";
        out << toIndex(ne.getKey());
        out << "(Tuple<RamDomain," << arity << ">({";
        out << join(ne.getValues(), ",", [&](std::ostream& out, RamValue* value) {
            if (!value) {
                out << "0";
            } else {
                visit(*value, out);
            }
        });
        out << "})," << ctxName << ")";
	if (predicated) {
	    out << ", pred)";
	} else {
	    out << ".empty()";
	}
        PRINT_END_COMMENT(out);
    }

    // -- values --
    void visitNumber(const RamNumber& num, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        out << "RamDomain(" << num.getConstant() << ")";
        PRINT_END_COMMENT(out);
    }

    void visitElementAccess(const RamElementAccess& access, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        out << "env" << access.getLevel() << "[" << access.getElement() << "]";
        PRINT_END_COMMENT(out);
    }

    void visitAutoIncrement(const RamAutoIncrement& /*inc*/, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        out << "(ctr++)";
        PRINT_END_COMMENT(out);
    }

    void visitUnaryOperator(const RamUnaryOperator& op, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        switch (op.getOperator()) {
            case UnaryOp::ORD:
                out << print(op.getValue());
                break;
            case UnaryOp::STRLEN:
                out << "strlen(symTable.resolve((size_t)" << print(op.getValue()) << "))";
                break;
            case UnaryOp::NEG:
                out << "(-(" << print(op.getValue()) << "))";
                break;
            case UnaryOp::BNOT:
                out << "(~(" << print(op.getValue()) << "))";
                break;
            case UnaryOp::LNOT:
                out << "(!(" << print(op.getValue()) << "))";
                break;
            case UnaryOp::SIN:
                out << "sin((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::COS:
                out << "cos((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::TAN:
                out << "tan((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ASIN:
                out << "asin((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ACOS:
                out << "acos((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ATAN:
                out << "atan((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::SINH:
                out << "sinh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::COSH:
                out << "cosh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::TANH:
                out << "tanh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ASINH:
                out << "asinh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ACOSH:
                out << "acosh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::ATANH:
                out << "atanh((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::LOG:
                out << "log((" << print(op.getValue()) << "))";
                break;
            case UnaryOp::EXP:
                out << "exp((" << print(op.getValue()) << "))";
                break;
            default:
                assert(0 && "Unsupported Operation!");
                break;
        }
        PRINT_END_COMMENT(out);
    }

    void visitBinaryOperator(const RamBinaryOperator& op, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        switch (op.getOperator()) {
            // arithmetic
            case BinaryOp::ADD: {
                out << "(" << print(op.getLHS()) << ") + (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::SUB: {
                out << "(" << print(op.getLHS()) << ") - (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::MUL: {
                out << "(" << print(op.getLHS()) << ") * (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::DIV: {
                out << "(" << print(op.getLHS()) << ") / (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::EXP: {
                out << "(AstDomain)(std::pow((AstDomain)" << print(op.getLHS()) << ","
                    << "(AstDomain)" << print(op.getRHS()) << "))";
                break;
            }
            case BinaryOp::MOD: {
                out << "(" << print(op.getLHS()) << ") % (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::BAND: {
                out << "(" << print(op.getLHS()) << ") & (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::BOR: {
                out << "(" << print(op.getLHS()) << ") | (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::BXOR: {
                out << "(" << print(op.getLHS()) << ") ^ (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::LAND: {
                out << "(" << print(op.getLHS()) << ") && (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::LOR: {
                out << "(" << print(op.getLHS()) << ") || (" << print(op.getRHS()) << ")";
                break;
            }
            case BinaryOp::MAX: {
                out << "(AstDomain)(std::max((AstDomain)" << print(op.getLHS()) << ","
                    << "(AstDomain)" << print(op.getRHS()) << "))";
                break;
            }
            case BinaryOp::MIN: {
                out << "(AstDomain)(std::min((AstDomain)" << print(op.getLHS()) << ","
                    << "(AstDomain)" << print(op.getRHS()) << "))";
                break;
            }

            // strings
            case BinaryOp::CAT: {
                out << "(RamDomain)symTable.lookup(";
                out << "(std::string(symTable.resolve((size_t)";
                out << print(op.getLHS());
                out << ")) + std::string(symTable.resolve((size_t)";
                out << print(op.getRHS());
                out << "))).c_str())";
                break;
            }
            default:
                assert(0 && "Unsupported Operation!");
        }
        PRINT_END_COMMENT(out);
    }

    void visitTernaryOperator(const RamTernaryOperator& op, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        switch (op.getOperator()) {
            case TernaryOp::SUBSTR:
                out << "(RamDomain)symTable.lookup(";
                out << "(substr_wrapper(symTable.resolve((size_t)";
                out << print(op.getArg(0));
                out << "),(";
                out << print(op.getArg(1));
                out << "),(";
                out << print(op.getArg(2));
                out << ")).c_str()))";
                break;
            default:
                assert(0 && "Unsupported Operation!");
        }
        PRINT_END_COMMENT(out);
    }

    // -- records --

    void visitPack(const RamPack& pack, std::ostream& out) override {
        PRINT_BEGIN_COMMENT(out);
        out << "pack("
            << getTupleType(pack.getValues().size()) << "({" << join(pack.getValues(), ",", rec)
            << "})"
            << ")";
        PRINT_END_COMMENT(out);
    }

    // -- subroutine argument --

    void visitArgument(const RamArgument& arg, std::ostream& out) override {
        out << "(args)[" << arg.getArgNumber() << "]";
    }

    // -- subroutine return --

    void visitReturn(const RamReturn& ret, std::ostream& out) override {
        for (auto val : ret.getValues()) {
            if (val == nullptr) {
                out << "ret.push_back(0);\n";
                out << "err.push_back(true);\n";
            } else {
                out << "ret.push_back(" << print(val) << ");\n";
                out << "err.push_back(false);\n";
            }
        }
    }

    // -- safety net --

    void visitNode(const RamNode& node, std::ostream& /*out*/) override {
        std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
        assert(false && "Unsupported Node Type!");
    }

    void emitSeparateMethods(std::map<std::string, std::string>& out) {
	bool predicated = Global::config().has("predicated");
	for (auto& p : separate_methods) {
	    const auto& method_name = p.first;
	    const auto& filename = method_name + ".cpp";
	    const auto& method_body = p.second.first;
	    const auto& method_decls = p.second.second;

	    std::stringstream contents;
	    // Headers and namespace decl
	    contents << "#include \"souffle/CompiledSouffle.h\"\n";
	    contents << "namespace souffle {\n";
	    contents << "using namespace ram;\n";
	    contents << "extern SymbolTable symTable;\n";
	    if (predicated) {
		contents << "extern BDD bdd;\n";
	    }
	    contents << "extern bool regex_wrapper(const char *pattern, const char *text);\n";
	    contents << "extern std::string substr_wrapper(const char *str, size_t idx, size_t len);\n";

	    contents << method_decls << "\n";
	    
	    contents << "void " << method_name << "() {\n";
	    contents << "const bool performIO = true;\n";
	    contents << method_body;
	    contents << "}\n";  // end of function
	    contents << "}\n";  // end of namespace

	    out.insert(std::make_pair(filename, contents.str()));
	}
    }

private:
    printer print(const RamNode& node) {
        return printer(*this, node);
    }

    printer print(const RamNode* node) {
        return print(*node);
    }

    std::string SHA1Hash(const std::string& data) {
	static const char* hexchars = "0123456789abcdef";
	SHA1_CTX sha;
	uint8_t digest[20];
	SHA1Init(&sha);
	SHA1Update(&sha, reinterpret_cast<const unsigned char*>(data.data()), data.size());
	SHA1Final(digest, &sha);
	std::string hash;
	for (int i = 0; i < 20; i++) {
	    hash += hexchars[(digest[i] >> 4) & 0x0f];
	    hash += hexchars[digest[i] & 0x0f];
	}
	return hash;
    }

    std::string getSeparateMethodDecls(const RamNode& node) {

	std::set<std::string> relations;
	visitDepthFirst(node, [&](const RamRelation& rel) {
		relations.insert(getRelationName(rel));
	    });

	std::stringstream ss;
	
	for (const auto& rel : relations) {
	    auto it = relation_decls.find(rel);
	    assert(it != relation_decls.end());
	    ss << "extern " << it->second << ";\n";
	    const std::string& wrapper_rel = "wrapper_" + rel;
	    it = relation_decls.find(wrapper_rel);
	    if (it != relation_decls.end()) {
		ss << "extern " << it->second << ";\n";
	    }
	}

	return ss.str();
    }

    void handleSeparateMethod(std::ostream& out, const RamNode& node, const std::string& body) {
	if (predicated || no_separate_method_count > 0) {
	    out << body;
	} else {
	    auto decls = getSeparateMethodDecls(node);
	
	    auto hash = SHA1Hash(decls + "\n" + body);
	    std::string method_name = std::string("function_") + hash;
	    if (separate_methods.find(method_name) == separate_methods.end()) {
		separate_methods.insert(std::make_pair(method_name, std::make_pair(body, decls)));
	    }
	    out << "extern void " << method_name << "();\n";
	    out << method_name << "();\n";
	}
    }
};

CodeEmitter genCode(std::ostream& out, const std::map<std::string, std::string> relationDecls, const RamStatement& stmt) {
    // use printer
    CodeEmitter emit(relationDecls);
    
    emit.visit(stmt, out);
    return emit;
}
}  // namespace

// Returns a map of additional output files.
std::map<std::string, std::string> Synthesiser::generateCode(
        const RamTranslationUnit& unit, std::ostream& os, const std::string& id) const {

    const SymbolTable& symTable = unit.getSymbolTable();
    const RamProgram& prog = unit.getP();

    // compute the set of all record arities
    std::set<int> recArities;
    visitDepthFirst(prog, [&](const RamNode& node) {
        if (const RamPack* pack = dynamic_cast<const RamPack*>(&node)) {
            recArities.insert(pack->getValues().size());
        }
    });

    // ---------------------------------------------------------------
    //                      Auto-Index Generation
    // ---------------------------------------------------------------
    IndexSetAnalysis* idxAnalysis = unit.getAnalysis<IndexSetAnalysis>();

    // ---------------------------------------------------------------
    //                      Code Generation
    // ---------------------------------------------------------------

    bool predicated = Global::config().has("predicated");

    std::string classname = "Sf_" + id;

    // generate C++ program
    os << "#include \"souffle/CompiledSouffle.h\"\n";
    if (Global::config().has("provenance")) {
        os << "#include \"souffle/Explain.h\"\n";
        os << "#include <ncurses.h>\n";
    }
    os << "\n";
    os << "namespace souffle {\n";
    os << "using namespace ram;\n";

    os << "bool regex_wrapper(const char *pattern, const char *text) {\n";
    os << "   bool result = false; \n";
    os << "   try { result = std::regex_match(text, std::regex(pattern)); } catch(...) { \n";
    os << "     std::cerr << \"warning: wrong pattern provided for match(\\\"\" << pattern << \"\\\",\\\"\" "
          "<< text << \"\\\")\\n\";\n}\n";
    os << "   return result;\n";
    os << "}\n";
    os << "std::string substr_wrapper(const char *str, size_t idx, size_t len) {\n";
    os << "   std::string sub_str, result; \n";
    os << "   try { result = std::string(str).substr(idx,len); } catch(...) { \n";
    os << "     std::cerr << \"warning: wrong index position provided by substr(\\\"\";\n";
    os << "     std::cerr << str << \"\\\",\" << (int32_t)idx << \",\" << (int32_t)len << \") "
          "functor.\\n\";\n";
    os << "   } return result;\n";
    os << "}\n";


    // N.B.: we make the symtab, BDD, and relations global vars so
    // that modifying the set of relations does not change the class
    // layout and invalidate separate compilation of all RAM
    // statements, one per .cpp file. This is an important
    // optimization for compile time. In effect, the linker patches up
    // what would have been class-layout offsets with global variable
    // addresses instead.

    // declare symbol table
    os << "SymbolTable symTable;\n";

    // declare predicate table
    if (predicated) {
	os << "BDD bdd;\n";
    }

    os << "std::string inputDirectory, outputDirectory;\n";

    // print relation definitions
    std::map<std::string, std::string> relationDecls;

    std::string registerRel;   // registration of relations
    int relCtr = 0;
    std::string tempType;  // string to hold the type of the temporary relations
    visitDepthFirst(*(prog.getMain()), [&](const RamCreate& create) {

        // get some table details
        const auto& rel = create.getRelation();
        int arity = rel.getArity();
	int physArity = arity;
	if (predicated) {
	    physArity += 2;
	}
        const std::string& raw_name = rel.getName();
        const std::string& name = getRelationName(rel);

        // ensure that the type of the new knowledge is the same as that of the delta knowledge
        tempType = (rel.isTemp() && raw_name.find("@delta") != std::string::npos)
                           ? getRelationType(rel, arity, idxAnalysis->getIndexes(rel), predicated)
                           : tempType;
        const std::string& type = (rel.isTemp()) ? tempType : getRelationType(rel, arity,
                                                                      idxAnalysis->getIndexes(rel), predicated);

        // defining table
        os << "// -- Table: " << raw_name << "\n";
	relationDecls.insert(std::make_pair(name, type + "* " + name));
        os << type << "* " << name << " = new " + type + "();\n";
        if ((rel.isInput() || rel.isComputed() || Global::config().has("provenance")) && !rel.isTemp()) {
            // construct types
            std::string tupleType = "std::array<const char *," + std::to_string(physArity) + ">{";
            std::string tupleName = "std::array<const char *," + std::to_string(physArity) + ">{";

            if (rel.getArity()) {
                tupleType += "\"" + rel.getArgTypeQualifier(0) + "\"";
                for (int i = 1; i < arity; i++) {
                    tupleType += ",\"" + rel.getArgTypeQualifier(i) + "\"";
                }

                tupleName += "\"" + rel.getArg(0) + "\"";
                for (int i = 1; i < arity; i++) {
                    tupleName += ",\"" + rel.getArg(i) + "\"";
                }
            }
            tupleType += "}";
            tupleName += "}";

	    std::stringstream wraptype;
            wraptype << "souffle::RelationWrapper<";
            wraptype << relCtr++ << ",";
            wraptype << type << ",";
            wraptype << "Tuple<RamDomain," << physArity << ">,";
            wraptype << physArity << ",";
            wraptype << (rel.isInput() ? "true" : "false") << ",";
            wraptype << (rel.isComputed() ? "true" : "false");
	    wraptype << ">";
	    os << wraptype.str();
            os << " wrapper_" << name << "(*" << name << ", symTable, \"" << raw_name << "\"," <<
		tupleType << ", " << tupleName << ");\n";
	    std::string wrapper_name = std::string("wrapper_") + name;
	    relationDecls.insert(std::make_pair(name, wraptype.str() + " " + wrapper_name));

            registerRel += "addRelation(\"" + raw_name + "\",&" + wrapper_name + "," +
                           std::to_string(rel.isInput()) + "," + std::to_string(rel.isOutput()) + ");\n";
        }
    });


    // print wrapper for regex
    os << "class " << classname << " : public SouffleProgram {\n";
    os << "private:\n";

    if (Global::config().has("profile")) {
        os << "std::string profiling_fname;\n";
    }

    os << "public:\n";

    // -- constructor --

    os << classname;
    if (Global::config().has("profile")) {
        os << "(std::string pf=\"profile.log\", std::string inputDirectory = \".\") : profiling_fname(pf)";
    } else {
        os << "(std::string inputDirectory = \".\")";
    }
    os << "{\n";
    os << registerRel;

    os << "// -- initialize symbol table --\n";
    // Read symbol table
    os << "std::string symtab_filepath = inputDirectory + \"/\" + \"" + Global::getSymtabFilename() + "\";\n";
    os << "if (fileExists(symtab_filepath)) {\n";
	os << "std::map<std::string, std::string> readIODirectivesMap = {\n";
	os << "{\"IO\", \"file\"},\n";
	    os << "{\"filename\", symtab_filepath},\n";
	os << "{\"symtabfilename\", symtab_filepath},\n";
	    os << "{\"name\", \"souffle_records\"}\n";
	os << "};\n";
	os << "IODirectives readIODirectives(readIODirectivesMap);\n";
	os << "std::unique_ptr<RecordReadStream> reader = IOSystem::getInstance()\n";
	    os << ".getRecordReader(symTable, readIODirectives);\n";
	os << "reader->readIntoSymbolTable(symTable);\n";
    os << "}\n";
    if (symTable.size() > 0) {
        os << "static const char *symbols[]={\n";
        for (size_t i = 0; i < symTable.size(); i++) {
            os << "\tR\"(" << symTable.resolve(i) << ")\",\n";
        }
        os << "};\n";
        os << "symTable.insert(symbols," << symTable.size() << ");\n";
        os << "\n";
    }

    os << "std::string recordsInFilepath = inputDirectory + \"/\" + \"" + Global::getRecordFilename() + "\";\n";
    os << "if (fileExists(recordsInFilepath)) {\n";
    os << "std::map<std::string, std::string> readIODirectivesMap = {\n";
    os << "{\"IO\", \"file\"},\n";
    os << "{\"filename\", recordsInFilepath},\n";
    os << "{\"name\", \"souffle_records\"}\n";
    os << "};\n";
    os << "IODirectives readIODirectives(readIODirectivesMap);\n";
    os << "try {\n";
    os << "std::unique_ptr<RecordReadStream> reader = IOSystem::getInstance()\n";
    os << ".getRecordReader(symTable, readIODirectives);\n";
    os << "auto records = reader->readAllRecords();\n";
    os << "for (auto r_it = records->begin(); r_it != records->end(); ++r_it) {\n";
    os << "for (RamDomain* record: r_it->second) {\n";
    os << "switch(r_it->first) {";
    for (int arity: recArities) {
	os << "case " << arity << ": {\n";
	os << getTupleType(arity) << " tuple" << arity << ";\n";
	for (int i = 0; i < arity; ++i) {
	    os << "tuple" << arity << "["<< i << "] = record[" << i << "];\n";
	}
	os << "pack<" << getTupleType(arity) << ">(tuple" << arity << ");\n";
	os << "break;\n}\n";
    }
    os << "default: break; \n";
    os << "}\n";
    os << "}\n";
    os << "}\n";
    os << "} catch (std::exception& e) {\n";
    os << "std::cerr << e.what();\n";
    os << "}\n";
    os << "}\n";

    os << "}\n";

    // -- destructor --

    os << "~" << classname << "() {\n";
    os << "}\n";

    // -- run function --

    os << "private:\ntemplate <bool performIO> void runFunction(std::string _inputDirectory = \".\", "
          "std::string _outputDirectory = \".\") {\n";
    os << "inputDirectory = _inputDirectory;\n";
    os << "outputDirectory = _outputDirectory;\n";

    os << "SignalHandler::instance()->set();\n";

    // initialize counter
    os << "// -- initialize counter --\n";
    os << "std::atomic<RamDomain> ctr(0);\n\n";

    if (Global::config().has("predicated")) {
	os << "BDDValue pred = BDD::TRUE();\n\n";
    }

    // set default threads (in embedded mode)
    if (std::stoi(Global::config().get("jobs")) > 0) {
        os << "#if defined(__EMBEDDED_SOUFFLE__) && defined(_OPENMP)\n";
        os << "omp_set_num_threads(" << std::stoi(Global::config().get("jobs")) << ");\n";
        os << "#endif\n\n";
    }

    std::vector<CodeEmitter> code_emitters;

    // add actual program body
    os << "// -- query evaluation --\n";
    if (Global::config().has("profile")) {
        os << "std::ofstream profile(profiling_fname);\n";
        os << "profile << \"" << AstLogStatement::startDebug() << "\" << std::endl;\n";
        code_emitters.emplace_back(genCode(os, relationDecls, *(prog.getMain())));
    } else {
        code_emitters.emplace_back(genCode(os, relationDecls, *(prog.getMain())));
    }
    // add code printing hint statistics
    os << "\n// -- relation hint statistics --\n";
    os << "if(isHintsProfilingEnabled()) {\n";
    os << "std::cout << \" -- Operation Hint Statistics --\\n\";\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamCreate& create) {
        auto name = getRelationName(create.getRelation());
        os << "std::cout << \"Relation " << name << ":\\n\";\n";
        os << name << "->printHintStatistics(std::cout,\"  \");\n";
        os << "std::cout << \"\\n\";\n";
    });
    os << "}\n";

    os << "SignalHandler::instance()->reset();\n";

    os << "}\n";  // end of runFunction() method

    // add methods to run with and without performing IO (mainly for the interface)
    os << "public:\nvoid run() { runFunction<false>(); }\n";
    os << "public:\nvoid runAll(std::string inputDirectory = \".\", std::string outputDirectory = \".\") { "
          "runFunction<true>(inputDirectory, outputDirectory); }\n";

    // issue printAll method
    os << "public:\n";
    os << "void printAll(std::string outputDirectory = \".\") {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamStatement& node) {
        if (auto store = dynamic_cast<const RamStore*>(&node)) {
            for (IODirectives ioDirectives : store->getIODirectives()) {
                os << "try {";
                os << "std::map<std::string, std::string> directiveMap(" << ioDirectives << ");\n";
                os << "if (!outputDirectory.empty() && directiveMap[\"IO\"] == \"file\" && ";
                os << "directiveMap[\"filename\"].front() != '/') {";
                os << "directiveMap[\"filename\"] = outputDirectory + \"/\" + directiveMap[\"filename\"];";
                os << "}\n";
                os << "IODirectives ioDirectives(directiveMap);\n";
                os << "IOSystem::getInstance().getWriter(";
                os << "SymbolMask({" << store->getRelation().getSymbolMask() << "})";
                os << ", symTable, ioDirectives, " << Global::config().has("provenance");
		os << ", " << Global::config().has("predicated");
                os << ")->writeAll(*" << getRelationName(store->getRelation()) << ");\n";

                os << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
            }
        } else if (auto print = dynamic_cast<const RamPrintSize*>(&node)) {
            os << "{ auto lease = getOutputLock().acquire(); \n";
            os << "(void)lease;\n";
            os << "std::cout << R\"(" << print->getMessage() << ")\" <<  ";
            os << getRelationName(print->getRelation()) << "->"
               << "size() << std::endl;\n";
            os << "}";
        }
    });
    os << "}\n";  // end of printAll() method

    // issue printAllRecords method
    os << "public:\n";
    os << "void printAllRecords(std::string outputDirectory = \".\") {\n";

    // Print record tables and symtab
    if (Global::config().has("recorddump")) {
	os << "std::string recordsOutFilepath = outputDirectory + \"/\" + \"" + Global::getRecordFilename() + "\";\n";
	os << "std::string symtabOutFilepath = outputDirectory + \"/\" + \"" + Global::getSymtabFilename() + "\";\n";
	os << "std::map<std::string, std::string> writeIODirectivesMap = {\n";
	os << "{\"IO\", \"file\"},\n";
	os << "{\"filename\", recordsOutFilepath},\n";
	os << "{\"symtabfilename\", symtabOutFilepath},\n";
	os << "{\"name\", \"souffle_records\"}\n";
	os << "};\n";
	os << "IODirectives writeIODirectives(writeIODirectivesMap);\n";
	os << "try {\n";
	os << "auto writer = IOSystem::getInstance().getRecordWriter(symTable, writeIODirectives);\n";
	for (int arity: recArities) {
	    os << "printRecords<" << getRecordTupleType(arity) << ">(writer);\n";
	}
	os << "writer->writeSymbolTable();\n";
	os << "} catch (std::exception& e) {\n";
	os << "std::cerr << e.what();\n";
	os << "exit(1);\n";
	os << "}\n";
    }

    // Print BDD, if predicated
    if (Global::config().has("predicated")) {
	os << "std::string bddOutFilepath = outputDirectory + \"/\" + \"" + Global::getBDDNodesFilename() + "\";\n";
	os << "try {\n";
	os << "bdd.writeFile(bddOutFilepath);\n";
	os << "} catch (std::exception& e) {\n";
	os << "std::cerr << e.what();\n";
	os << "exit(1);\n";
	os << "}\n";
    }

    os << "}\n";  // end of printAll() method

    // issue loadAll method
    os << "public:\n";
    os << "void loadAll(std::string inputDirectory = \".\") {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamLoad& load) {
        // get some table details
        os << "try {";
        os << "std::map<std::string, std::string> directiveMap(";
        os << load.getIODirectives() << ");\n";
        os << "if (!inputDirectory.empty() && directiveMap[\"IO\"] == \"file\" && ";
        os << "directiveMap[\"filename\"].front() != '/') {";
        os << "directiveMap[\"filename\"] = inputDirectory + \"/\" + directiveMap[\"filename\"];";
        os << "}\n";
        os << "IODirectives ioDirectives(directiveMap);\n";
        os << "IOSystem::getInstance().getReader(";
        os << "SymbolMask({" << load.getRelation().getSymbolMask() << "})";
        os << ", symTable, ioDirectives";
        os << ", " << Global::config().has("provenance");
	os << ", " << Global::config().has("predicated");
        os << ")->readAll(*" << getRelationName(load.getRelation());
        os << ");\n";
        os << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
    });
    os << "}\n";  // end of loadAll() method

    // issue dump methods
    auto dumpRelation = [&](const std::string& name, const SymbolMask& mask, size_t arity) {
        auto relName = name;

        os << "try {";
        os << "IODirectives ioDirectives;\n";
        os << "ioDirectives.setIOType(\"stdout\");\n";
        os << "ioDirectives.setRelationName(\"" << name << "\");\n";
        os << "IOSystem::getInstance().getWriter(";
        os << "SymbolMask({" << mask << "})";
        os << ", symTable, ioDirectives, " << Global::config().has("provenance");
	os << ", " << Global::config().has("predicated");
        os << ")->writeAll(*" << relName << ");\n";
        os << "} catch (std::exception& e) {std::cerr << e.what();exit(1);}\n";
    };

    // dump inputs
    os << "public:\n";
    os << "void dumpInputs(std::ostream& out = std::cout) {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamLoad& load) {
        auto& name = getRelationName(load.getRelation());
        auto& mask = load.getRelation().getSymbolMask();
        size_t arity = load.getRelation().getArity();
        dumpRelation(name, mask, arity);
    });
    os << "}\n";  // end of dumpInputs() method

    // dump outputs
    os << "public:\n";
    os << "void dumpOutputs(std::ostream& out = std::cout) {\n";
    visitDepthFirst(*(prog.getMain()), [&](const RamStore& store) {
        auto& name = getRelationName(store.getRelation());
        auto& mask = store.getRelation().getSymbolMask();
        size_t arity = store.getRelation().getArity();
        dumpRelation(name, mask, arity);
    });
    os << "}\n";  // end of dumpOutputs() method

    os << "public:\n";
    os << "const SymbolTable &getSymbolTable() const {\n";
    os << "return symTable;\n";
    os << "}\n";  // end of getSymbolTable() method

    // TODO: generate code for subroutines
    if (Global::config().has("provenance")) {
        // generate subroutine adapter
        os << "void executeSubroutine(std::string name, const std::vector<RamDomain>& args, "
              "std::vector<RamDomain>& ret, std::vector<bool>& err) override {\n";

        // subroutine number
        size_t subroutineNum = 0;
        for (auto& sub : prog.getSubroutines()) {
            os << "if (name == \"" << sub.first << "\") {\n"
               << "subproof_" << subroutineNum
               << "(args, ret, err);\n"  // subproof_i to deal with special characters in relation names
               << "}\n";
            subroutineNum++;
        }
        os << "}\n";  // end of executeSubroutine

        // generate method for each subroutine
        subroutineNum = 0;
        for (auto& sub : prog.getSubroutines()) {
            // method header
            os << "void "
               << "subproof_" << subroutineNum << "(const std::vector<RamDomain>& args, "
                                                  "std::vector<RamDomain>& ret, std::vector<bool>& err) {\n";

            // generate code for body
            code_emitters.emplace_back(genCode(os, relationDecls, *sub.second));

            os << "return;\n";
            os << "}\n";  // end of subroutine
            subroutineNum++;
        }
    }

    os << "};\n";  // end of class declaration

    // emit separate methods into separate files.
    std::map<std::string, std::string> separate_files;
    for (auto& emit : code_emitters) {
	emit.emitSeparateMethods(separate_files);
    }

    // hidden hooks
    os << "SouffleProgram *newInstance_" << id << "(){return new " << classname << ";}\n";
    os << "SymbolTable *getST_" << id << "(SouffleProgram *p){return &symTable;}\n";

    os << "#ifdef __EMBEDDED_SOUFFLE__\n";
    os << "class factory_" << classname << ": public souffle::ProgramFactory {\n";
    os << "SouffleProgram *newInstance() {\n";
    os << "return new " << classname << "();\n";
    os << "};\n";
    os << "public:\n";
    os << "factory_" << classname << "() : ProgramFactory(\"" << id << "\"){}\n";
    os << "};\n";
    os << "static factory_" << classname << " __factory_" << classname << "_instance;\n";
    os << "}\n";
    os << "#else\n";
    os << "}\n";
    os << "int main(int argc, char** argv)\n{\n";
    os << "try{\n";

    // parse arguments
    os << "souffle::CmdOptions opt(";
    os << "R\"(" << Global::config().get("") << ")\",\n";
    os << "R\"(.)\",\n";
    os << "R\"(.)\",\n";
    if (Global::config().has("profile")) {
        os << "true,\n";
        os << "R\"(" << Global::config().get("profile") << ")\",\n";
    } else {
        os << "false,\n";
        os << "R\"()\",\n";
    }
    os << std::stoi(Global::config().get("jobs")) << "\n";
    os << ");\n";

    os << "if (!opt.parse(argc,argv)) return 1;\n";

    os << "#if defined(_OPENMP) \n";
    os << "omp_set_nested(true);\n";
    os << "#endif\n";

    os << "souffle::";
    if (Global::config().has("profile")) {
        os << classname + " obj(opt.getProfileName(), opt.getInputFileDir());\n";
    } else {
        os << classname + " obj(opt.getInputFileDir());\n";
    }

    os << "obj.runAll(opt.getInputFileDir(), opt.getOutputFileDir());\n";
    os << "obj.printAllRecords(opt.getOutputFileDir());\n";
    if (Global::config().get("provenance") == "1") {
        os << "explain(obj, true, false);\n";
    } else if (Global::config().get("provenance") == "2") {
        os << "explain(obj, true, true);\n";
    }

    os << "return 0;\n";
    os << "} catch(std::exception &e) { souffle::SignalHandler::instance()->error(e.what());}\n";
    os << "}\n";
    os << "#endif\n";

    return separate_files;
}
}  // end of namespace souffle
