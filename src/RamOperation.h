/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RamOperation.h
 *
 * Defines the Operation of a relational algebra query.
 *
 ***********************************************************************/

#pragma once

#include "RamCondition.h"
#include "RamNode.h"
#include "RamRelation.h"

#include <memory>
#include <set>

namespace souffle {

/**
 * Abstract class for a relational algebra operation
 */
class RamOperation : public RamNode {
protected:
    /** the nesting level of this operation */
    size_t level;

    /**
     * condition that is checked for each obtained tuple
     *
     * If condition is a nullptr, then no condition applies
     */
    std::unique_ptr<RamCondition> condition;

public:
    RamOperation(RamNodeType type, size_t l) : RamNode(type), level(l), condition(nullptr) {}

    /** Get level */
    // TODO (#541): move to analysis
    size_t getLevel() const {
        return level;
    }

    /** Get depth */
    // TODO (#541): move to analysis
    virtual size_t getDepth() const = 0;

    /** Print */
    virtual void print(std::ostream& os, int tabpos) const = 0;

    /** Pretty print */
    void print(std::ostream& os) const override {
        print(os, 0);
    }

    /** Add condition */
    virtual void addCondition(std::unique_ptr<RamCondition> c, const RamOperation& root);

    /** Add condition */
    void addCondition(std::unique_ptr<RamCondition> c) {
        addCondition(std::move(c), *this);
    }

    /** Get condition */
    RamCondition* getCondition() const {
        return condition.get();
    }

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        if (!condition) {
            return toVector<const RamNode*>();
        }
        return {condition.get()};
    }

    /** Create clone */
    RamOperation* clone() const override = 0;

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        if (condition) {
            condition = map(std::move(condition));
        }
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamOperation*>(&node));
        const RamOperation& other = static_cast<const RamOperation&>(node);
        if (getCondition() != nullptr && other.getCondition() != nullptr) {
            return *getCondition() == *other.getCondition();
        } else if (getCondition() == nullptr && other.getCondition() == nullptr) {
            return true;
        } else {
            return false;
        }
    }
};

/**
 * Abstract class for relation scans and lookups
 */
class RamSearch : public RamOperation {
    /** Nested operation */
    std::unique_ptr<RamOperation> nestedOperation;

public:
    RamSearch(RamNodeType type, std::unique_ptr<RamOperation> nested)
            : RamOperation(type, nested->getLevel() - 1), nestedOperation(std::move(nested)) {}

    /** get nested operation */
    RamOperation* getNestedOperation() const {
        return nestedOperation.get();
    }
    /** get nested operation */
    const RamOperation& getOperation() const {
        ASSERT(nestedOperation);
        return *nestedOperation;
    }

    /** Add condition */
    void addCondition(std::unique_ptr<RamCondition> c, const RamOperation& root) override;

    /** Get depth of query */
    size_t getDepth() const override {
        return 1 + nestedOperation->getDepth();
    }

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        auto res = RamOperation::getChildNodes();
        res.push_back(nestedOperation.get());
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamOperation::apply(map);
        nestedOperation = map(std::move(nestedOperation));
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamSearch*>(&node));
        const RamSearch& other = static_cast<const RamSearch&>(node);
        return RamOperation::equal(other) && getOperation() == other.getOperation();
    }
};

/**
 * Relation Scan
 *
 * Iterate all tuples of a relation and filter them via a condition
 */
class RamScan : public RamSearch {
protected:
    /** Search relation */
    std::unique_ptr<RamRelation> relation;

    /** Values of index per column of table (if indexable) */
    std::vector<std::unique_ptr<RamValue>> queryPattern;

    /** Indexable columns for a range query */
    SearchColumns keys;

    /**
     * Determines whether this scan operation is merely verifying the existence
     * of a value (e.g. rel(_,_), rel(1,2), rel(1,_) or rel(X,Y) where X and Y are bound)
     * or actually contributing new variable bindings (X or Y are not bound).
     *
     * The exists-only can be check much more efficient than the other case.
     */
    bool pureExistenceCheck;

    /** Is this a hypothetical-filter scan (accepting only
     * universally-true tuples)? */
    bool hypFilter;

public:
    RamScan(std::unique_ptr<RamRelation> r, std::unique_ptr<RamOperation> nested, bool pureExistenceCheck)
            : RamSearch(RN_Scan, std::move(nested)), relation(std::move(r)),
              queryPattern(relation->getArity()), keys(0), pureExistenceCheck(pureExistenceCheck),
	      hypFilter(false) {}

    /** Get search relation */
    const RamRelation& getRelation() const {
        return *relation;
    }

    /** Get indexable columns of scan */
    const SearchColumns& getRangeQueryColumns() const {
        return keys;
    }

    /** Get range pattern */
    std::vector<RamValue*> getRangePattern() const {
        return toPtrVector(queryPattern);
    }

    /** Check for pure existence check */
    // TODO (#541): rename pure existence check to complete/whole etc.
    bool isPureExistenceCheck() const {
        return pureExistenceCheck;
    }

    void setHypFilter(bool value) {
	hypFilter = value;
    }

    bool isHypFilter() const {
	return hypFilter;
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Add condition */
    void addCondition(std::unique_ptr<RamCondition> c, const RamOperation& root) override;

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        auto res = RamSearch::getChildNodes();
        for (auto& cur : queryPattern) {
            if (cur) {
                res.push_back(cur.get());
            }
        }
        return res;
    }

    /** Create clone */
    RamScan* clone() const override {
        RamScan* res = new RamScan(std::unique_ptr<RamRelation>(relation->clone()),
                std::unique_ptr<RamOperation>(getNestedOperation()->clone()), pureExistenceCheck);
        res->condition = std::unique_ptr<RamCondition>(condition->clone());
        res->keys = keys;
        for (auto& cur : queryPattern) {
            if (cur) {
                res->queryPattern.push_back(std::unique_ptr<RamValue>(cur->clone()));
            }
        }
	res->hypFilter = hypFilter;
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamSearch::apply(map);
        for (auto& cur : queryPattern) {
            if (cur) {
                cur = map(std::move(cur));
            }
        }
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamScan*>(&node));
        const RamScan& other = static_cast<const RamScan&>(node);
        return RamSearch::equal(other) && getRelation() == other.getRelation() &&
               equal_targets(queryPattern, other.queryPattern) && keys == other.keys &&
	       pureExistenceCheck == other.pureExistenceCheck &&
	       hypFilter == other.hypFilter;
    }
};

/**
 * Record lookup
 */
// TODO (#541): wrong class hierarchy, no condition in RAMOperation necessary
class RamLookup : public RamSearch {
    /** Level of the tuple containing record reference */
    std::size_t refLevel;

    /** Position of the tuple containing record reference */
    std::size_t refPos;

    /** Arity of the unpacked tuple */
    std::size_t arity;

public:
    RamLookup(std::unique_ptr<RamOperation> nested, std::size_t ref_level, std::size_t ref_pos,
            std::size_t arity)
            : RamSearch(RN_Lookup, std::move(nested)), refLevel(ref_level), refPos(ref_pos), arity(arity) {}

    /** Get reference level */
    std::size_t getReferenceLevel() const {
        return refLevel;
    }

    /** Get reference position */
    std::size_t getReferencePosition() const {
        return refPos;
    }

    /** Get Arity */
    std::size_t getArity() const {
        return arity;
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Create clone */
    RamLookup* clone() const override {
        RamLookup* res = new RamLookup(
                std::unique_ptr<RamOperation>(getNestedOperation()->clone()), refLevel, refPos, arity);
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamSearch::apply(map);
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamLookup*>(&node));
        const RamLookup& other = static_cast<const RamLookup&>(node);
        return RamSearch::equal(other) && getReferencePosition() == other.getReferencePosition() &&
               getReferenceLevel() == other.getReferenceLevel() && getArity() == other.getArity();
    }
};

/**
 * Aggregation
 */
class RamAggregate : public RamSearch {
public:
    /** Types of aggregation functions */
    enum Function { MAX, MIN, COUNT, SUM, PRODUCT };

private:
    /** Aggregation function */
    Function fun;

    /** Aggregation value */
    // TODO (#541): rename to target expression
    std::unique_ptr<RamValue> value;

    /** Aggregation relation */
    std::unique_ptr<RamRelation> relation;

    /** Pattern for filtering tuples */
    std::vector<std::unique_ptr<RamValue>> pattern;

    /** Columns to be matched when using a range query */
    SearchColumns keys;

public:
    RamAggregate(std::unique_ptr<RamOperation> nested, Function fun, std::unique_ptr<RamValue> value,
            std::unique_ptr<RamRelation> rel)
            : RamSearch(RN_Aggregate, std::move(nested)), fun(fun), value(std::move(value)),
              relation(std::move(rel)), pattern(relation->getArity()), keys(0) {}

    /** Get aggregation function */
    Function getFunction() const {
        return fun;
    }

    /** Get target expression */
    // TODO (#541): rename to getExpression
    const RamValue* getTargetExpression() const {
        ASSERT(value);
        return value.get();
    }

    /** Get relation */
    const RamRelation& getRelation() const {
        return *relation;
    }

    /** Get pattern */
    std::vector<RamValue*> getPattern() const {
        return toPtrVector(pattern);
    }

    /** Add condition */
    void addCondition(std::unique_ptr<RamCondition> c, const RamOperation& root) override;

    /** Get range query columns */
    SearchColumns getRangeQueryColumns() const {
        return keys;
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Create clone */
    RamAggregate* clone() const override {
        RamAggregate* res = new RamAggregate(std::unique_ptr<RamOperation>(getNestedOperation()->clone()),
                fun, std::unique_ptr<RamValue>(value->clone()),
                std::unique_ptr<RamRelation>(relation->clone()));
        res->keys = keys;
        for (auto& cur : pattern) {
            if (cur) {
                res->pattern.push_back(std::unique_ptr<RamValue>(cur->clone()));
            }
        }
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamSearch::apply(map);
        relation = map(std::move(relation));
        value = map(std::move(value));
        for (auto& cur : pattern) {
            if (cur) {
                cur = map(std::move(cur));
            }
        }
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamAggregate*>(&node));
        const RamAggregate& other = static_cast<const RamAggregate&>(node);
        return RamSearch::equal(other) && getRelation() == other.getRelation() &&
               equal_targets(pattern, other.pattern) && keys == other.keys && fun == other.fun &&
               getTargetExpression() == other.getTargetExpression();
    }
};

/** Forall aggregation */
class RamForall : public RamOperation {
protected:
    /** Domain relation */
    std::unique_ptr<RamRelation> domRelation;

    /** Values to connect to each domain-relation argument */
    std::vector<std::unique_ptr<RamValue>> args;

    /** Forall-domain variables (the rest of the relation columns
     * indicate forall instance) */
    SearchColumns domVars;

    /** Nested operation */
    std::unique_ptr<RamOperation> nested;

public:
    RamForall(std::unique_ptr<RamRelation> domRelation, std::unique_ptr<RamOperation> nested)
            : RamOperation(RN_Forall, nested->getLevel() - 1), domRelation(std::move(domRelation)),
              domVars(0), nested(std::move(nested)) {}

    void addArg(std::unique_ptr<RamValue> arg) {
	args.push_back(std::move(arg));
    }

    void addDomVar(size_t idx) {
	domVars |= (1L << idx);
    }

    RamRelation* getDomRelation() const {
	return domRelation.get();
    }

    std::vector<RamValue*> getArgs() const {
	return toPtrVector(args);
    }

    RamOperation* getNested() const {
	return nested.get();
    }

    const SearchColumns getDomVarColumns() const {
	return domVars;
    }

    const SearchColumns getKeyColumns() const {
	size_t arity = domRelation->getArity();
	return ((1L << arity) - 1) ^ domVars;
    }

    /** Get depth of query */
    size_t getDepth() const override {
        return 1 + nested->getDepth();
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        auto res = RamOperation::getChildNodes();
        res.push_back(domRelation.get());
        for (const auto& cur : args) {
            res.push_back(cur.get());
        }
	res.push_back(nested.get());
        return res;
    }

    /** Create clone */
    RamForall* clone() const override {
        RamForall* res = new RamForall(std::unique_ptr<RamRelation>(domRelation->clone()),
				       std::unique_ptr<RamOperation>(nested->clone()));
        for (auto& cur : args) {
            res->args.push_back(std::unique_ptr<RamValue>(cur->clone()));
        }
	res->domVars = domVars;
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamOperation::apply(map);
        domRelation = map(std::move(domRelation));
        for (auto& cur : args) {
            cur = map(std::move(cur));
        }
	nested = map(std::move(nested));
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamForall*>(&node));
        const RamForall& other = static_cast<const RamForall&>(node);
	return *domRelation.get() == *other.domRelation.get() &&
	    domVars == other.domVars &&
	    equal_targets(args, other.args) &&
	    *nested.get() == *other.nested.get();
    }
};

/** Duplicate detection */
class RamFindDuplicate : public RamOperation {
protected:
    /** Column numbers defining the tuples on which we look for duplicates,
     * per "given" col */
    std::vector<int> dup;
    /** Column numbers separating the input tuple stream into separate domains
     * in which we look for duplicates */
    std::vector<int> given;

    /** Relation over which we look for duplicates */
    std::unique_ptr<RamRelation> srcRelation;

    /** Nested operation */
    std::unique_ptr<RamOperation> nested;

public:
    RamFindDuplicate(std::unique_ptr<RamOperation> nested, std::unique_ptr<RamRelation> src)
            : RamOperation(RN_FindDuplicate, nested->getLevel() - 1),
	      srcRelation(std::move(src)),
	      nested(std::move(nested)) {}

    void addDupVar(int col) {
	dup.push_back(col);
    }

    void addGivenVar(int col) {
	given.push_back(col);
    }

    std::vector<int> getDupVars() const {
	return dup;
    }

    std::vector<int> getGivenVars() const {
	return given;
    }

    RamOperation* getNested() const {
	return nested.get();
    }

    RamRelation* getSrcRelation() const {
	return srcRelation.get();
    }

    /** Get depth of query */
    size_t getDepth() const override {
        return 1 + nested->getDepth();
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        auto res = RamOperation::getChildNodes();
	res.push_back(nested.get());
	res.push_back(srcRelation.get());
        return res;
    }

    /** Create clone */
    RamFindDuplicate* clone() const override {
        RamFindDuplicate* res = new RamFindDuplicate(
	    std::unique_ptr<RamOperation>(nested->clone()),
	    std::unique_ptr<RamRelation>(srcRelation->clone()));
        for (auto cur : dup) {
            res->dup.push_back(cur);
        }
	for (auto cur : given) {
            res->given.push_back(cur);
        }
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamOperation::apply(map);
	nested = map(std::move(nested));
	srcRelation = map(std::move(srcRelation));
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamFindDuplicate*>(&node));
        const RamFindDuplicate& other = static_cast<const RamFindDuplicate&>(node);
	return
	    dup == other.dup && given == other.given &&
	    *nested.get() == *other.nested.get() &&
	    *srcRelation.get() == *other.srcRelation.get();
    }
};

/** Projection */
class RamProject : public RamOperation {
protected:
    /** Relation */
    std::unique_ptr<RamRelation> relation;

    /** Relation to check whether does not exist */
    // TODO (#541): rename
    std::unique_ptr<RamRelation> filter;

    /* Values for projection */
    std::vector<std::unique_ptr<RamValue>> values;

    /** Projection allocates new "hypothetical" variable on all tuples
     * created? */
    bool hypothetical;

public:
    RamProject(std::unique_ptr<RamRelation> rel, size_t level)
	    : RamOperation(RN_Project, level), relation(std::move(rel)), filter(nullptr), hypothetical(false) {}

    RamProject(std::unique_ptr<RamRelation> rel, const RamRelation& filter, size_t level)
            : RamOperation(RN_Project, level), relation(std::move(rel)),
              filter(std::make_unique<RamRelation>(filter)), hypothetical(false) {}

    /** Add value for a column */
    void addArg(std::unique_ptr<RamValue> v) {
        values.push_back(std::move(v));
    }

    /** Add condition to project, needed for different level check */
    void addCondition(std::unique_ptr<RamCondition> c, const RamOperation& root) override;

    void setHypothetical(bool h) {
	hypothetical = h;
    }

    bool getHypothetical() const {
	return hypothetical;
    }

    /** Get relation */
    const RamRelation& getRelation() const {
        return *relation;
    }

    /** Check filter */
    bool hasFilter() const {
        return (bool)filter;
    }

    /** Get filter */
    const RamRelation& getFilter() const {
        assert(hasFilter());
        return *filter;
    }

    /** Get values */
    std::vector<RamValue*> getValues() const {
        return toPtrVector(values);
    }

    /** Get depth */
    size_t getDepth() const override {
        return 1;
    }

    /** Print */
    void print(std::ostream& os, int tabpos) const override;

    /** Obtain list of child nodes */
    std::vector<const RamNode*> getChildNodes() const override {
        auto res = RamOperation::getChildNodes();
        res.push_back(relation.get());
        for (const auto& cur : values) {
            res.push_back(cur.get());
        }
        return res;
    }

    /** Create clone */
    RamProject* clone() const override {
        RamProject* res = new RamProject(std::unique_ptr<RamRelation>(relation->clone()), level);
        if (filter != nullptr) {
            res->filter = std::make_unique<RamRelation>(*filter);
        }
        for (auto& cur : values) {
            res->values.push_back(std::unique_ptr<RamValue>(cur->clone()));
        }
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamOperation::apply(map);
        relation = map(std::move(relation));
        for (auto& cur : values) {
            cur = map(std::move(cur));
        }
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamProject*>(&node));
        const RamProject& other = static_cast<const RamProject&>(node);
        bool isFilterEqual = false;
        if (filter == nullptr && other.filter == nullptr) {
            isFilterEqual = true;
        } else if (filter != nullptr && other.filter != nullptr) {
            isFilterEqual = (*filter == *other.filter);
        }
        return RamOperation::equal(other) && getRelation() == other.getRelation() &&
               equal_targets(values, other.values) && isFilterEqual;
    }
};

/** A statement for returning from a ram subroutine */
class RamReturn : public RamOperation {
protected:
    std::vector<std::unique_ptr<RamValue>> values;

public:
    RamReturn(size_t level) : RamOperation(RN_Return, level) {}

    void print(std::ostream& out, int tabpos) const override;

    size_t getDepth() const override {
        return 1;
    }

    void addValue(std::unique_ptr<RamValue> val) {
        values.push_back(std::move(val));
    }

    std::vector<RamValue*> getValues() const {
        return toPtrVector(values);
    }

    /** Get value */
    RamValue& getValue(size_t i) const {
        assert(i < values.size() && "value index out of range");
        return *values[i];
    }

    /** Create clone */
    RamReturn* clone() const override {
        RamReturn* res = new RamReturn(level);
        for (auto& cur : values) {
            res->values.push_back(std::unique_ptr<RamValue>(cur->clone()));
        }
        return res;
    }

    /** Apply mapper */
    void apply(const RamNodeMapper& map) override {
        RamOperation::apply(map);
        for (auto& cur : values) {
            cur = map(std::move(cur));
        }
    }

protected:
    /** Check equality */
    bool equal(const RamNode& node) const override {
        assert(dynamic_cast<const RamReturn*>(&node));
        const RamReturn& other = static_cast<const RamReturn&>(node);
        return RamOperation::equal(other) && equal_targets(values, other.values);
    }
};

}  // namespace souffle
