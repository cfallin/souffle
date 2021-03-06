/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file Interpreter.h
 *
 * Declares the interpreter class for executing RAM programs.
 *
 ***********************************************************************/

#pragma once

#include "InterpreterIndex.h"
#include "RamProgram.h"
#include "RamRelation.h"
#include "RamTranslationUnit.h"
#include "SymbolTable.h"
#include "BDD.h"

#include "RamStatement.h"

#include <functional>
#include <ostream>
#include <vector>

namespace souffle {

/**
 * Interpreter Relation
 */
class InterpreterRelation {
private:
    /** Arity of relation */
    size_t arity;

    /** Size of blocks containing tuples */
    static const int BLOCK_SIZE = 1024;

    /** Block data structure for storing tuples */
    struct Block {
        size_t size;
        size_t used;
        // TODO (#421): replace linked list by STL linked list
        // block becomes payload of STL linked list only
        std::unique_ptr<Block> next;
        std::unique_ptr<RamDomain[]> data;

        Block(size_t s = BLOCK_SIZE) : size(s), used(0), next(nullptr), data(new RamDomain[size]) {}

        size_t getFreeSpace() const {
            return size - used;
        }
    };

    /** Number of tuples in relation */
    size_t num_tuples;

    /** Head of block list */
    std::unique_ptr<Block> head;

    /** Tail of block list */
    Block* tail;

    /** List of all allocated blocks */
    std::list<RamDomain*> allocatedBlocks;

    /** List of indices */
    mutable std::map<InterpreterIndexOrder, std::unique_ptr<InterpreterIndex>> indices;

    /** Total index for existence checks */
    mutable InterpreterIndex* totalIndex;

    /** Lock for parallel execution */
    mutable Lock lock;

    /** Is hypothetical reasoning enabled? If so, each tuple has an
     * extra two values: predicate, and variable introduced by this
     * tuple. */
    bool enableHypotheses;

    /** Physical-storage arity, after accounting for hypothetical
     * reasoning. */
    size_t physArity;

    /** Pointer to BDD, to be used when manipulating predicates */
    BDD* bdd;

    /** If 0-arity relation (just 1 bit), carry the predicate explicitly */
    BDDValue empty_rel_pred;

public:
    InterpreterRelation(size_t relArity, bool enableHypotheses)
            : arity(relArity), num_tuples(0), head(std::make_unique<Block>()), tail(head.get()),
              totalIndex(nullptr), enableHypotheses(enableHypotheses), empty_rel_pred(BDD::TRUE()) {
	physArity = enableHypotheses ? (relArity + 2) : relArity;
    }

    InterpreterRelation(const InterpreterRelation& other) = delete;

    InterpreterRelation(InterpreterRelation&& other)
            : arity(other.arity), num_tuples(other.num_tuples), tail(other.tail),
              totalIndex(other.totalIndex), enableHypotheses(other.enableHypotheses),
              physArity(other.physArity) {
        // take over ownership
        head.swap(other.head);
        indices.swap(other.indices);

        allocatedBlocks.swap(other.allocatedBlocks);
    }

    virtual ~InterpreterRelation() {
        for (auto x : allocatedBlocks) delete[] x;
    }

    void setBDD(BDD* bdd) {
	this->bdd = bdd;
    }

    // TODO (#421): check whether still required
    InterpreterRelation& operator=(const InterpreterRelation& other) = delete;

    InterpreterRelation& operator=(InterpreterRelation&& other) {
        ASSERT(getArity() == other.getArity());
	ASSERT(enableHypotheses == other.enableHypotheses);

        num_tuples = other.num_tuples;
        tail = other.tail;
        totalIndex = other.totalIndex;

        // take over ownership
        head.swap(other.head);
        indices.swap(other.indices);

        return *this;
    }

    /** Get arity of relation */
    size_t getArity() const {
        return arity;
    }

    bool getEnableHypotheses() const {
	return enableHypotheses;
    }

    /** Check whether relation is empty */
    bool empty() const {
        return num_tuples == 0;
    }

    BDDValue empty(BDDValue pred) const {
	if (empty()) {
	    return BDD::TRUE();
	}
	if (enableHypotheses) {
	    if (arity == 0) {
		return empty_rel_pred;
	    }
	    BDDValue ret = BDD::TRUE();
	    for (const auto& tuple : *this) {
		BDDValue thisPred = BDDValue::from_domain(tuple[arity]);
		BDDVar thisVar = BDDVar::from_domain(tuple[arity + 1]);
		thisPred = thisVar != BDD::NO_VAR() ?
		    bdd->make_and(thisPred, bdd->make_var(thisVar)) : thisPred;
		ret = bdd->make_and(ret, bdd->make_not(thisPred));
	    }
	    return ret;
	} else {
	    return BDD::FALSE();
	}
    }

    /** Gets the number of contained tuples */
    size_t size() const {
        return num_tuples;
    }

    /** Insert tuple */
    virtual void insert(const RamDomain* tuple) {
	insert(tuple, BDD::TRUE(), BDD::NO_VAR());
    }

    /** Insert tuple */
    virtual void insert(const RamDomain* tuple, BDDValue pred, BDDVar pred_var) {
        // check for null-arity
        if (arity == 0) {
            // set number of tuples to one -- that's it
            num_tuples = 1;
	    if (enableHypotheses) {
		BDDValue thisPred = pred_var != BDD::NO_VAR() ?
		    bdd->make_and(pred, bdd->make_var(pred_var)) : pred;
		empty_rel_pred = bdd->make_and(empty_rel_pred, bdd->make_not(thisPred));
	    }
            return;
        }

        ASSERT(tuple);

        // make existence check
        if (exists(tuple, pred, pred_var) == BDD::TRUE()) {
            return;
        }

	// with predication only: look for another tuple with same
	// values but possibly different predicate, and update the
	// predicate.
	if (enableHypotheses && pred_var == BDD::NO_VAR()) {
	    if (!totalIndex) {
		totalIndex = getIndex(getTotalIndexKey());
	    }
	    auto p = totalIndex->equalRange(tuple);
	    if (p.first != p.second) {
		RamDomain* curTuple = const_cast<RamDomain*>(*p.first);
		// Only possible to merge when no predvar is involved.
		if (BDDVar::from_domain(curTuple[arity + 1]) == BDD::NO_VAR()) {
		    curTuple[arity] = bdd->make_or(BDDValue::from_domain(curTuple[arity]), pred).as_domain();
		    return;
		}
	    }
	    // Otherwise, no merge possible -- continue below and
	    // insert new tuple.
	}

        // prepare tail
        if (tail->getFreeSpace() < physArity || physArity == 0) {
            tail->next = std::make_unique<Block>();
            tail = tail->next.get();
        }

        // insert element into tail
        RamDomain* newTuple = &tail->data[tail->used];
        for (size_t i = 0; i < arity; ++i) {
            newTuple[i] = tuple[i];
        }
	if (enableHypotheses) {
	    newTuple[arity] = pred.as_domain();
	    newTuple[arity + 1] = pred_var.as_domain();
	}
        tail->used += physArity;

        // update all indexes with new tuple
        for (const auto& cur : indices) {
            cur.second->insert(newTuple);
        }

        // increment relation size
        num_tuples++;
    }

    /** Insert tuple via arguments */
    template <typename... Args>
    void insert(RamDomain first, Args... rest) {
        RamDomain tuple[] = {first, RamDomain(rest)...};
        insert(tuple);
    }

    /** Merge another relation into this relation */
    void insert(const InterpreterRelation& other) {
        assert(getArity() == other.getArity());
	assert(enableHypotheses == other.enableHypotheses);
	if (enableHypotheses) {
	    for (const auto& cur : other) {
		insert(cur, BDDValue::from_domain(cur[arity]), BDDVar::from_domain(cur[arity + 1]));
	    }
	} else {
	    for (const auto& cur : other) {
		insert(cur);
	    }
	}
    }

    /** Purge table */
    void purge() {
        std::unique_ptr<Block> newHead = std::make_unique<Block>();
        head.swap(newHead);
        tail = head.get();
        for (const auto& cur : indices) {
            cur.second->purge();
        }
        num_tuples = 0;
    }

    /** get index for a given set of keys using a cached index as a helper. Keys are encoded as bits for each
     * column */
    InterpreterIndex* getIndex(const SearchColumns& key, InterpreterIndex* cachedIndex) const {
        if (!cachedIndex) {
            return getIndex(key);
        }
        return getIndex(cachedIndex->order());
    }

    /** get index for a given set of keys. Keys are encoded as bits for each column */
    InterpreterIndex* getIndex(const SearchColumns& key) const {
        // suffix for order, if no matching prefix exists
        std::vector<unsigned char> suffix;
        suffix.reserve(getArity());

        // convert to order
        InterpreterIndexOrder order;
        for (size_t k = 1, i = 0; i < getArity(); i++, k *= 2) {
            if (key & k) {
                order.append(i);
            } else {
                suffix.push_back(i);
            }
        }

        // see whether there is an order with a matching prefix
        InterpreterIndex* res = nullptr;
        {
            auto lease = lock.acquire();
            (void)lease;
            for (auto it = indices.begin(); !res && it != indices.end(); ++it) {
                if (order.isCompatible(it->first)) {
                    res = it->second.get();
                }
            }
        }
        // if found, use compatible index
        if (res) {
            return res;
        }

        // extend index to full index
        for (auto cur : suffix) {
            order.append(cur);
        }
        assert(order.isComplete());

        // get a new index
        return getIndex(order);
    }

    /** get index for a given order. Keys are encoded as bits for each column */
    InterpreterIndex* getIndex(const InterpreterIndexOrder& order) const {
        // TODO: improve index usage by re-using indices with common prefix
        InterpreterIndex* res = nullptr;
        {
            auto lease = lock.acquire();
            (void)lease;
            auto pos = indices.find(order);
            if (pos == indices.end()) {
                std::unique_ptr<InterpreterIndex>& newIndex = indices[order];
                newIndex = std::make_unique<InterpreterIndex>(order);
                newIndex->insert(this->begin(), this->end());
                res = newIndex.get();
            } else {
                res = pos->second.get();
            }
        }
        return res;
    }

    /** Obtains a full index-key for this relation */
    SearchColumns getTotalIndexKey() const {
        return (1 << (getArity())) - 1;
    }

    /** check whether a tuple exists in the relation */
    bool exists(const RamDomain* tuple) const {
	return exists(tuple, BDD::TRUE(), BDD::NO_VAR()) != BDD::FALSE();
    }

    /** check whether a tuple exists in the relation with at least the
     *  given predicate */
    BDDValue exists(const RamDomain* tuple, BDDValue pred, BDDVar var) const {
	// handle arity 0
        if (getArity() == 0) {
	    if (empty()) {
		return BDD::FALSE();
	    }
	    BDDValue n = var != BDD::NO_VAR() ? bdd->make_and(pred, bdd->make_var(var)) : pred;
            return bdd->make_not(empty(n));
        }

        // handle all other arities
        if (!totalIndex) {
            totalIndex = getIndex(getTotalIndexKey());
        }
        if (!totalIndex->exists(tuple)) {
	    return BDD::FALSE();
	}
	if (!enableHypotheses) {
	    return BDD::TRUE();
	}

	// Tuple exists, but its predicate may be too narrow -- we
	// determine here whether the existing predicate "covers"
	// the new one.
	auto p = totalIndex->equalRange(tuple);
	const RamDomain* curTuple = *p.first;
	BDDValue curPred = BDDValue::from_domain(curTuple[arity]);
	BDDVar curPredVar = BDDVar::from_domain(curTuple[arity + 1]);

	// keep the below new BDD nodes local -- we don't save them, so avoid polluting the BDD
	BDD::SubFrame sf(*bdd);
	
	BDDValue c = curPredVar != BDD::NO_VAR() ? bdd->make_and(curPred, bdd->make_var(curPredVar)) : curPred;
	BDDValue n = var != BDD::NO_VAR() ? bdd->make_and(pred, bdd->make_var(var)) : pred;
	// The current tuple's predicate covers the new tuple's
	// predicate whenever the current tuple's predicate is true or
	// the new tuple's predicate is false.
	BDDValue result = bdd->make_or(c, bdd->make_not(n));
	// Any non-false value indicates we'll need to make an update.
	return sf.ret(result);
    }

    // --- iterator ---

    /** Iterator for relation */
    class iterator : public std::iterator<std::forward_iterator_tag, RamDomain*> {
        Block* cur;
        RamDomain* tuple;
        size_t arity;
	RamDomain fake_empty_tuple[2];

    public:
        iterator() : cur(nullptr), tuple(nullptr), arity(0) {}

        iterator(Block* c, RamDomain* t, size_t a) : cur(c), tuple(t), arity(a) {}

	iterator(BDDValue pred) :
	    cur(nullptr), tuple(fake_empty_tuple), arity(0),
	    fake_empty_tuple { pred.as_domain(), BDD::NO_VAR().as_domain() } {
	}

        const RamDomain* operator*() {
            return tuple;
        }

        bool operator==(const iterator& other) const {
            return tuple == other.tuple;
        }

        bool operator!=(const iterator& other) const {
            return (tuple != other.tuple);
        }

        iterator& operator++() {
	    // support 0-arity
            if (arity == 0) {
                // move to end
                *this = iterator();
                return *this;
            }

            // check for end
            if (!cur) {
                return *this;
            }

            // support all other arities
            tuple += arity;
            if (tuple >= &cur->data[cur->used]) {
                cur = cur->next.get();
                tuple = (cur) ? cur->data.get() : nullptr;
            }
            return *this;
        }
    };

    /** get iterator begin of relation */
    inline iterator begin() const {
        // check for emptiness
        if (empty()) {
            return end();
        }

        // support 0-arity
        if (getArity() == 0) {
            return iterator(empty_rel_pred);
        }

        // support non-empty non-zero arity relation
        return iterator(head.get(), &head->data[0], physArity);
    }

    /** get iterator begin of relation */
    inline iterator end() const {
        return iterator();
    }

    /** Extend tuple */
    virtual std::vector<RamDomain*> extend(const RamDomain* tuple) {
        std::vector<RamDomain*> newTuples;

        // A standard relation does not generate extra new knowledge on insertion.
        newTuples.push_back(new RamDomain[2]{tuple[0], tuple[1]});

        return newTuples;
    }

    /** Extend relation */
    virtual void extend(const InterpreterRelation& rel) {}
};

/**
 * Interpreter Equivalence Relation
 */

class InterpreterEqRelation : public InterpreterRelation {
public:
    InterpreterEqRelation(size_t relArity, bool enableHypotheses)
	: InterpreterRelation(relArity, enableHypotheses) {}

    /** Insert tuple */
    void insert(const RamDomain* tuple) override {
        // TODO: (pnappa) an eqrel check here is all that appears to be needed for implicit additions
        // TODO: future optimisation would require this as a member datatype
        // brave soul required to pass this quest
        // // specialisation for eqrel defs
        // std::unique_ptr<binaryrelation> eqreltuples;
        // in addition, it requires insert functions to insert into that, and functions
        // which allow reading of stored values must be changed to accommodate.
        // e.g. insert =>  eqRelTuples->insert(tuple[0], tuple[1]);

        // for now, we just have a naive & extremely slow version, otherwise known as a O(n^2) insertion
        // ):

        for (auto* newTuple : extend(tuple)) {
            InterpreterRelation::insert(newTuple);
            delete[] newTuple;
        }
    }

    /** Find the new knowledge generated by inserting a tuple */
    std::vector<RamDomain*> extend(const RamDomain* tuple) override {
        std::vector<RamDomain*> newTuples;

        newTuples.push_back(new RamDomain[2]{tuple[0], tuple[0]});
        newTuples.push_back(new RamDomain[2]{tuple[0], tuple[1]});
        newTuples.push_back(new RamDomain[2]{tuple[1], tuple[0]});
        newTuples.push_back(new RamDomain[2]{tuple[1], tuple[1]});

        std::vector<const RamDomain*> relevantStored;
        for (const RamDomain* vals : *this) {
            if (vals[0] == tuple[0] || vals[0] == tuple[1] || vals[1] == tuple[0] || vals[1] == tuple[1]) {
                relevantStored.push_back(vals);
            }
        }

        for (const auto vals : relevantStored) {
            newTuples.push_back(new RamDomain[2]{vals[0], tuple[0]});
            newTuples.push_back(new RamDomain[2]{vals[0], tuple[1]});
            newTuples.push_back(new RamDomain[2]{vals[1], tuple[0]});
            newTuples.push_back(new RamDomain[2]{vals[1], tuple[1]});
            newTuples.push_back(new RamDomain[2]{tuple[0], vals[0]});
            newTuples.push_back(new RamDomain[2]{tuple[0], vals[1]});
            newTuples.push_back(new RamDomain[2]{tuple[1], vals[0]});
            newTuples.push_back(new RamDomain[2]{tuple[1], vals[1]});
        }

        return newTuples;
    }
    /** Extend this relation with new knowledge generated by inserting all tuples from a relation */
    void extend(const InterpreterRelation& rel) override {
        std::vector<RamDomain*> newTuples;
        // store all values that will be implicitly relevant to the those that we will insert
        for (const auto* tuple : rel) {
            for (auto* newTuple : extend(tuple)) {
                newTuples.push_back(newTuple);
            }
        }
        for (const auto* newTuple : newTuples) {
            InterpreterRelation::insert(newTuple);
            delete[] newTuple;
        }
    }
};

/**
 * An environment encapsulates all the context information required for
 * processing a RAM program.
 */
class InterpreterEnvironment {
    /** The type utilized for storing relations */
    typedef std::map<std::string, InterpreterRelation*> relation_map;

    /** The symbol table to be utilized by an evaluation */
    SymbolTable& symbolTable;

    /** The relations manipulated by a ram program */
    relation_map data;

    /** The increment counter utilized by some RAM language constructs */
    int counter;

    /** The BDD that defines predicates on tuples */
    BDD bdd;

    /** Is hypothetical reasoning enabled? */
    bool enableHypotheses;

public:
    InterpreterEnvironment(SymbolTable& symbolTable)
            : symbolTable(symbolTable), counter(0), enableHypotheses(false) {}

    virtual ~InterpreterEnvironment() {
        for (auto& x : data) {
            delete x.second;
        }
    }

    /**
     * Obtains a reference to the enclosed symbol table.
     */
    SymbolTable& getSymbolTable() {
        return symbolTable;
    }

    /**
     * Obtains the current value of the internal counter.
     */
    int getCounter() const {
        return counter;
    }

    /**
     * Increments the internal counter and obtains the
     * old value.
     */
    int incCounter() {
        return counter++;
    }

    /** Enables hypothetical reasoning. MUST be called
     * before any relation is created.
     */
    void setEnableHypotheses(bool value) {
	enableHypotheses = value;
    }

    bool getEnableHypotheses() const {
	return enableHypotheses;
    }

    BDD& getBDD() {
	return bdd;
    }

    /**
     * Obtains a mutable reference to one of the relations maintained
     * by this environment. If the addressed relation does not exist,
     * a new, empty relation will be created.
     */
    InterpreterRelation& getRelation(const RamRelation& id) {
        InterpreterRelation* res = nullptr;
        auto pos = data.find(id.getName());
        if (pos != data.end()) {
            res = (pos->second);
        } else {
            if (!id.isEqRel()) {
                res = new InterpreterRelation(id.getArity(), enableHypotheses);
            } else {
                res = new InterpreterEqRelation(id.getArity(), enableHypotheses);
            }
	    if (enableHypotheses) {
		res->setBDD(&getBDD());
	    }
            data[id.getName()] = res;
        }
        // return result
        return *res;
    }

    /**
     * Obtains an immutable reference to the relation identified by
     * the given identifier. If no such relation exist, a reference
     * to an empty relation will be returned (not exhibiting the proper
     * id, but the correct content).
     */
    const InterpreterRelation& getRelation(const RamRelation& id) const {
        // look up relation
        auto pos = data.find(id.getName());
        assert(pos != data.end());

        // cache result
        return *pos->second;
    }

    /**
     * Obtains an immutable reference to the relation identified by
     * the given identifier. If no such relation exist, a reference
     * to an empty relation will be returned (not exhibiting the proper
     * id, but the correct content).
     */
    const InterpreterRelation& getRelation(const std::string& name) const {
        auto pos = data.find(name);
        assert(pos != data.end());
        return *pos->second;
    }

    /**
     * Returns the relation map
     */
    relation_map& getRelationMap() const {
        return const_cast<relation_map&>(data);
    }

    /**
     * Tests whether a relation with the given name is present.
     */
    bool hasRelation(const std::string& name) const {
        return data.find(name) != data.end();
    }

    /**
     * Deletes the referenced relation from this environment.
     */
    void dropRelation(const RamRelation& id) {
        data.erase(id.getName());
    }
};

/**
 * A class representing the order of predicates in the body of a rule
 */
class Order {
    /** The covered order */
    std::vector<unsigned> order;

public:
    static Order getIdentity(unsigned size) {
        Order res;
        for (unsigned i = 0; i < size; i++) {
            res.append(i);
        }
        return res;
    }

    void append(unsigned pos) {
        order.push_back(pos);
    }

    unsigned operator[](unsigned index) const {
        return order[index];
    }

    std::size_t size() const {
        return order.size();
    }

    bool isComplete() const {
        for (size_t i = 0; i < order.size(); i++) {
            if (!contains(order, i)) {
                return false;
            }
        }
        return true;
    }

    const std::vector<unsigned>& getOrder() const {
        return order;
    }

    void print(std::ostream& out) const {
        out << order;
    }

    friend std::ostream& operator<<(std::ostream& out, const Order& order) {
        order.print(out);
        return out;
    }
};

/**
 * The summary to be returned from a statement
 */
struct ExecutionSummary {
    Order order;
    long time;
};

/** Defines the type of execution strategies for interpreter */
typedef std::function<ExecutionSummary(const RamInsert&, InterpreterEnvironment& env, std::ostream*)>
        QueryExecutionStrategy;

/** With this strategy queries will be processed without profiling */
extern const QueryExecutionStrategy DirectExecution;

/** With this strategy queries will be dynamically with profiling */
extern const QueryExecutionStrategy ScheduledExecution;

/** The type to reference indices */
typedef unsigned Column;

/**
 * A summary of statistical properties of a ram relation.
 */
class RelationStats {
    /** The arity - accurate */
    uint8_t arity;

    /** The number of tuples - accurate */
    uint64_t size;

    /** The sample size estimations are based on */
    uint32_t sample_size;

    /** The cardinality of the various components of the tuples - estimated */
    std::vector<uint64_t> cardinalities;

public:
    RelationStats() : arity(0), size(0), sample_size(0) {}

    RelationStats(uint64_t size, const std::vector<uint64_t>& cards)
            : arity(cards.size()), size(size), sample_size(0), cardinalities(cards) {}

    RelationStats(const RelationStats&) = default;
    RelationStats(RelationStats&&) = default;

    RelationStats& operator=(const RelationStats&) = default;
    RelationStats& operator=(RelationStats&&) = default;

    /**
     * A factory function extracting statistical information form the given relation
     * base on a given sample size. If the sample size is not specified, the full
     * relation will be processed.
     */
    static RelationStats extractFrom(
            const InterpreterRelation& rel, uint32_t sample_size = std::numeric_limits<uint32_t>::max());

    uint8_t getArity() const {
        return arity;
    }

    uint64_t getCardinality() const {
        return size;
    }

    uint32_t getSampleSize() const {
        return sample_size;
    }

    uint64_t getEstimatedCardinality(Column c) const {
        if (c >= cardinalities.size()) {
            return 0;
        }
        return cardinalities[c];
    }

    void print(std::ostream& out) const {
        out << cardinalities;
    }

    friend std::ostream& operator<<(std::ostream& out, const RelationStats& stats) {
        stats.print(out);
        return out;
    }
};

/**
 * A RAM interpreter. The RAM program will
 * be processed within the callers process. Before every query operation, an
 * optional scheduling step will be conducted.
 */
class Interpreter {
protected:
    /** An optional stream to print logging information to an output stream */
    std::ostream* report;

public:
    /**
     * Update logging stream
     */
    void setReportTarget(std::ostream& report) {
        this->report = &report;
    }

    /**
     * Runs the given RAM statement on an empty environment and returns
     * this environment after the completion of the execution.
     */
    std::unique_ptr<InterpreterEnvironment> execute(SymbolTable& table, const RamProgram& prog,
						    bool enableHypotheses = false) const {
        auto env = std::make_unique<InterpreterEnvironment>(table);
	env->setEnableHypotheses(enableHypotheses);
        invoke(prog, *env);
        return env;
    }

    /**
     * Runs the given RAM statement on an empty environment and returns
     * this environment after the completion of the execution.
     */
    std::unique_ptr<InterpreterEnvironment> execute(const RamTranslationUnit& tu,
						    bool enableHypotheses = false) const {
        return execute(tu.getSymbolTable(), *tu.getProgram(), enableHypotheses);
    }

    /** An execution strategy for the interpreter */
    QueryExecutionStrategy queryStrategy;

public:
    /** A constructor accepting a query strategy */
    Interpreter(const QueryExecutionStrategy& queryStrategy)
            : report(nullptr), queryStrategy(queryStrategy) {}

    /** run the program for a given interpreter environment */
    void invoke(const RamProgram& prog, InterpreterEnvironment& env) const;

    /**
     * Runs a subroutine of a RamProgram
     */
    virtual void executeSubroutine(InterpreterEnvironment& env, const RamStatement& stmt,
            const std::vector<RamDomain>& arguments, std::vector<RamDomain>& returnValues,
            std::vector<bool>& returnErrors) const;
};

}  // end of namespace souffle
