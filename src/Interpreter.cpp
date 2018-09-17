/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file Interpreter.cpp
 *
 * Implementation of the RAM interpreter.
 *
 ***********************************************************************/

#include "Interpreter.h"
#include "AstLogStatement.h"
#include "AstRelation.h"
#include "AstTranslator.h"
#include "AstVisitor.h"
#include "BinaryConstraintOps.h"
#include "BinaryFunctorOps.h"
#include "Global.h"
#include "IOSystem.h"
#include "InterpreterRecords.h"
#include "Logger.h"
#include "Macro.h"
#include "RamVisitor.h"
#include "RuleScheduler.h"
#include "SignalHandler.h"
#include "TypeSystem.h"
#include "UnaryFunctorOps.h"
#include "Util.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <memory>
#include <regex>
#include <set>
#include <utility>

#include <unistd.h>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace souffle {

class EvalContext {
    std::vector<const RamDomain*> data;
    std::vector<RamDomain>* returnValues = nullptr;
    std::vector<bool>* returnErrors = nullptr;
    const std::vector<RamDomain>* args = nullptr;
    std::vector<BDDValue> preds;  // predicate for each level of evaluation

public:
    // Foralls: only one forall is active at a time.

    // - Per-key (forall-instance) state:
    //
    // When predicates are not enabled: simply store sets of tuples
    // for domain and value inputs, and when counts are equal, the
    // forall is satisfied.
    struct ForallState {
	bool init;
	std::set<std::vector<RamDomain>> domVals;
	std::set<std::vector<RamDomain>> seenVals;

	ForallState() : init(false) {}
    };

private:
    std::map<std::vector<RamDomain>, ForallState> forallState;


public:
    EvalContext(size_t size = 0) : data(size) {
	BDDValue t = BDD::TRUE();
	for (size_t i = 0; i < size; i++) {
	    preds.push_back(t);
	}
    }

    const RamDomain*& operator[](size_t index) {
        return data[index];
    }

    const RamDomain* const& operator[](size_t index) const {
        return data[index];
    }

    std::vector<RamDomain>& getReturnValues() const {
        return *returnValues;
    }

    void setReturnValues(std::vector<RamDomain>& retVals) {
        returnValues = &retVals;
    }

    void addReturnValue(RamDomain val, bool err = false) {
        assert(returnValues != nullptr && returnErrors != nullptr);
        returnValues->push_back(val);
        returnErrors->push_back(err);
    }

    std::vector<bool>& getReturnErrors() const {
        return *returnErrors;
    }

    void setReturnErrors(std::vector<bool>& retErrs) {
        returnErrors = &retErrs;
    }

    const std::vector<RamDomain>& getArguments() const {
        return *args;
    }

    void setArguments(const std::vector<RamDomain>& a) {
        args = &a;
    }

    RamDomain getArgument(size_t i) const {
        assert(args != nullptr && i < args->size() && "argument out of range");
        return (*args)[i];
    }


    ForallState& getForallState(std::vector<RamDomain> key) {
	auto it = forallState.find(key);
	if (it == forallState.end()) {
	    ForallState newState;
	    return forallState.insert(
		it, std::make_pair(std::move(key), newState))->second;
	} else {
	    return it->second;
	}
    }

    BDDValue& pred(size_t i) {
	return preds[i];
    }

    const BDDValue& pred(size_t i) const {
	return preds[i];
    }
};

namespace {

RamDomain eval(const RamValue& value, InterpreterEnvironment& env, const EvalContext& ctxt = EvalContext()) {
    class Evaluator : public RamVisitor<RamDomain> {
        InterpreterEnvironment& env;
        const EvalContext& ctxt;

    public:
        Evaluator(InterpreterEnvironment& env, const EvalContext& ctxt) : env(env), ctxt(ctxt) {}

        // -- basics --
        RamDomain visitNumber(const RamNumber& num) override {
            return num.getConstant();
        }

        RamDomain visitElementAccess(const RamElementAccess& access) override {
            return ctxt[access.getLevel()][access.getElement()];
        }

        RamDomain visitAutoIncrement(const RamAutoIncrement& /*inc*/) override {
            return env.incCounter();
        }

        // unary functions

        RamDomain visitUnaryOperator(const RamUnaryOperator& op) override {
            switch (op.getOperator()) {
                case UnaryOp::NEG:
                    return -visit(op.getValue());
                case UnaryOp::BNOT:
                    return ~visit(op.getValue());
                case UnaryOp::LNOT:
                    return !visit(op.getValue());
                case UnaryOp::ORD:
                    return visit(op.getValue());
                case UnaryOp::STRLEN:
                    return strlen(env.getSymbolTable().resolve(visit(op.getValue())));
                case UnaryOp::SIN:
                    return sin(visit(op.getValue()));
                case UnaryOp::COS:
                    return cos(visit(op.getValue()));
                case UnaryOp::TAN:
                    return tan(visit(op.getValue()));
                case UnaryOp::ASIN:
                    return asin(visit(op.getValue()));
                case UnaryOp::ACOS:
                    return acos(visit(op.getValue()));
                case UnaryOp::ATAN:
                    return atan(visit(op.getValue()));
                case UnaryOp::SINH:
                    return sinh(visit(op.getValue()));
                case UnaryOp::COSH:
                    return cosh(visit(op.getValue()));
                case UnaryOp::TANH:
                    return tanh(visit(op.getValue()));
                case UnaryOp::ASINH:
                    return asinh(visit(op.getValue()));
                case UnaryOp::ACOSH:
                    return acosh(visit(op.getValue()));
                case UnaryOp::ATANH:
                    return atanh(visit(op.getValue()));
                case UnaryOp::LOG:
                    return log(visit(op.getValue()));
                case UnaryOp::EXP:
                    return exp(visit(op.getValue()));
                default:
                    assert(0 && "unsupported operator");
                    return 0;
            }
        }

        // binary functions

        RamDomain visitBinaryOperator(const RamBinaryOperator& op) override {
            switch (op.getOperator()) {
                // arithmetic
                case BinaryOp::ADD: {
                    return visit(op.getLHS()) + visit(op.getRHS());
                }
                case BinaryOp::SUB: {
                    return visit(op.getLHS()) - visit(op.getRHS());
                }
                case BinaryOp::MUL: {
                    return visit(op.getLHS()) * visit(op.getRHS());
                }
                case BinaryOp::DIV: {
                    RamDomain rhs = visit(op.getRHS());
                    return visit(op.getLHS()) / rhs;
                }
                case BinaryOp::EXP: {
                    return std::pow(visit(op.getLHS()), visit(op.getRHS()));
                }
                case BinaryOp::MOD: {
                    RamDomain rhs = visit(op.getRHS());
                    return visit(op.getLHS()) % rhs;
                }
                case BinaryOp::BAND: {
                    return visit(op.getLHS()) & visit(op.getRHS());
                }
                case BinaryOp::BOR: {
                    return visit(op.getLHS()) | visit(op.getRHS());
                }
                case BinaryOp::BXOR: {
                    return visit(op.getLHS()) ^ visit(op.getRHS());
                }
                case BinaryOp::LAND: {
                    return visit(op.getLHS()) && visit(op.getRHS());
                }
                case BinaryOp::LOR: {
                    return visit(op.getLHS()) || visit(op.getRHS());
                }
                case BinaryOp::MAX: {
                    return std::max(visit(op.getLHS()), visit(op.getRHS()));
                }
                case BinaryOp::MIN: {
                    return std::min(visit(op.getLHS()), visit(op.getRHS()));
                }

                // strings
                case BinaryOp::CAT: {
                    return env.getSymbolTable().lookup((
                            std::string(env.getSymbolTable().resolve(visit(op.getLHS()))) +
                            std::string(env.getSymbolTable().resolve(
                                    visit(op.getRHS())))).c_str());
                }
                default:
                    assert(0 && "unsupported operator");
                    return 0;
            }
        }

        // ternary functions

        RamDomain visitTernaryOperator(const RamTernaryOperator& op) override {
            switch (op.getOperator()) {
                case TernaryOp::SUBSTR: {
                    auto symbol = visit(op.getArg(0));
                    std::string str = env.getSymbolTable().resolve(symbol);
                    auto idx = visit(op.getArg(1));
                    auto len = visit(op.getArg(2));
                    std::string sub_str;
                    try {
                        sub_str = str.substr(idx, len);
                    } catch (...) {
                        std::cerr << "warning: wrong index position provided by substr(\"";
                        std::cerr << str << "\"," << (int32_t)idx << "," << (int32_t)len << ") functor.\n";
                    }
                    return env.getSymbolTable().lookup(sub_str.c_str());
                }
                default:
                    assert(0 && "unsupported operator");
                    return 0;
            }
        }

        // -- records --

        RamDomain visitPack(const RamPack& op) override {
            auto values = op.getValues();
            auto arity = values.size();
            RamDomain data[arity];
            for (size_t i = 0; i < arity; ++i) {
                data[i] = visit(values[i]);
            }
            return pack(data, arity);
        }

        // -- subroutine argument

        RamDomain visitArgument(const RamArgument& arg) override {
            return ctxt.getArgument(arg.getArgNumber());
        }

        // -- safety net --

        RamDomain visitNode(const RamNode& node) override {
            std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
            assert(false && "Unsupported Node Type!");
            return 0;
        }
    };

    // create and run evaluator
    return Evaluator(env, ctxt)(value);
}

RamDomain eval(const RamValue* value, InterpreterEnvironment& env, const EvalContext& ctxt = EvalContext()) {
    return eval(*value, env, ctxt);
}

// Returns predicate (rather than bool) as the condition may be predicated
// on hypothetical tuple presence/absence.
BDDValue eval(const RamCondition& cond, InterpreterEnvironment& env, const EvalContext& ctxt = EvalContext()) {
    class Evaluator : public RamVisitor<BDDValue> {
        InterpreterEnvironment& env;
        const EvalContext& ctxt;
	BDD& bdd;

    public:
        Evaluator(InterpreterEnvironment& env, const EvalContext& ctxt)
	    : env(env), ctxt(ctxt), bdd(env.getBDD()) {}

        // -- connectors operators --

        BDDValue visitAnd(const RamAnd& a) override {
            return bdd.make_and(visit(a.getLHS()), visit(a.getRHS()));
        }

        // -- relation operations --

        BDDValue visitEmpty(const RamEmpty& empty) override {
            return env.getRelation(empty.getRelation()).empty() ? BDD::TRUE() : BDD::FALSE();
        }

        BDDValue visitNotExists(const RamNotExists& ne) override {
            const InterpreterRelation& rel = env.getRelation(ne.getRelation());
	    BDDValue pred = ctxt.pred(ne.getLevel());

            // construct the pattern tuple
            auto arity = rel.getArity();
            auto values = ne.getValues();

            // for total we use the exists test
            if (ne.isTotal()) {
                RamDomain tuple[arity];
                for (size_t i = 0; i < arity; i++) {
                    tuple[i] = (values[i]) ? eval(values[i], env, ctxt) : MIN_RAM_DOMAIN;
                }

                return bdd.make_not(rel.exists(tuple, pred, BDD::NO_VAR()));
            }

            // for partial we search for lower and upper boundaries
            RamDomain low[arity];
            RamDomain high[arity];
            for (size_t i = 0; i < arity; i++) {
                low[i] = (values[i]) ? eval(values[i], env, ctxt) : MIN_RAM_DOMAIN;
                high[i] = (values[i]) ? low[i] : MAX_RAM_DOMAIN;
            }

            // obtain index
            auto idx = rel.getIndex(ne.getKey());
            auto range = idx->lowerUpperBound(low, high);
	    if (env.getEnableHypotheses()) {
		BDDValue exists = BDD::FALSE();
		for (auto it = range.first; it != range.second; ++it) {
		    const auto* tuple = *it;
		    BDDValue thisPred = BDDValue::from_domain(tuple[rel.getArity()]);
		    BDDVar thisPredVar = BDDVar::from_domain(tuple[rel.getArity() + 1]);
		    thisPred = thisPredVar != BDD::NO_VAR() ? bdd.make_and(thisPred, bdd.make_var(thisPredVar)) : thisPred;
		    exists = bdd.make_or(exists, thisPred);
		}
		return bdd.make_not(exists);
	    } else {
		// if there are none => done
		return range.first == range.second ? BDD::TRUE() : BDD::FALSE();
	    }
        }

        // -- comparison operators --

        BDDValue visitBinaryRelation(const RamBinaryRelation& relOp) override {
            switch (relOp.getOperator()) {
                // comparison operators
                case BinaryConstraintOp::EQ:
                    return eval(relOp.getLHS(), env, ctxt) == eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                              : BDD::FALSE();
                case BinaryConstraintOp::NE:
                    return eval(relOp.getLHS(), env, ctxt) != eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                              : BDD::FALSE();
                case BinaryConstraintOp::LT:
                    return eval(relOp.getLHS(), env, ctxt) < eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                             : BDD::FALSE();
                case BinaryConstraintOp::LE:
                    return eval(relOp.getLHS(), env, ctxt) <= eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                              : BDD::FALSE();
                case BinaryConstraintOp::GT:
                    return eval(relOp.getLHS(), env, ctxt) > eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                             : BDD::FALSE();
                case BinaryConstraintOp::GE:
                    return eval(relOp.getLHS(), env, ctxt) >= eval(relOp.getRHS(), env, ctxt) ? BDD::TRUE()
                                                                                              : BDD::FALSE();

                // strings
                case BinaryConstraintOp::MATCH: {
                    RamDomain l = eval(relOp.getLHS(), env, ctxt);
                    RamDomain r = eval(relOp.getRHS(), env, ctxt);
                    const std::string& pattern = env.getSymbolTable().resolve(l);
                    const std::string& text = env.getSymbolTable().resolve(r);
                    bool result = false;
                    try {
                        result = std::regex_match(text, std::regex(pattern));
                    } catch (...) {
                        std::cerr << "warning: wrong pattern provided for match(\"" << pattern << "\",\""
                                  << text << "\")\n";
                    }
                    return result ? BDD::TRUE() : BDD::FALSE();
                }
                case BinaryConstraintOp::NOT_MATCH: {
                    RamDomain l = eval(relOp.getLHS(), env, ctxt);
                    RamDomain r = eval(relOp.getRHS(), env, ctxt);
                    const std::string& pattern = env.getSymbolTable().resolve(l);
                    const std::string& text = env.getSymbolTable().resolve(r);
                    bool result = false;
                    try {
                        result = !std::regex_match(text, std::regex(pattern));
                    } catch (...) {
                        std::cerr << "warning: wrong pattern provided for !match(\"" << pattern << "\",\""
                                  << text << "\")\n";
                    }
                    return result ? BDD::TRUE() : BDD::FALSE();
                }
                case BinaryConstraintOp::CONTAINS: {
                    RamDomain l = eval(relOp.getLHS(), env, ctxt);
                    RamDomain r = eval(relOp.getRHS(), env, ctxt);
                    const std::string& pattern = env.getSymbolTable().resolve(l);
                    const std::string& text = env.getSymbolTable().resolve(r);
                    return text.find(pattern) != std::string::npos ? BDD::TRUE() : BDD::FALSE();
                }
                case BinaryConstraintOp::NOT_CONTAINS: {
                    RamDomain l = eval(relOp.getLHS(), env, ctxt);
                    RamDomain r = eval(relOp.getRHS(), env, ctxt);
                    const std::string& pattern = env.getSymbolTable().resolve(l);
                    const std::string& text = env.getSymbolTable().resolve(r);
                    return text.find(pattern) == std::string::npos ? BDD::TRUE() : BDD::FALSE();
                }
                default:
                    assert(0 && "unsupported operator");
                    return BDD::FALSE();
            }
        }

        // -- safety net --

        BDDValue visitNode(const RamNode& node) override {
            std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
            assert(false && "Unsupported Node Type!");
            return BDD::FALSE();
        }
    };

    // run evaluator
    return Evaluator(env, ctxt)(cond);
}

void apply(const RamOperation& op, InterpreterEnvironment& env, const EvalContext& args = EvalContext()) {
    class Interpreter : public RamVisitor<void> {
        InterpreterEnvironment& env;
        EvalContext& ctxt;
	BDD& bdd;

    public:
        Interpreter(InterpreterEnvironment& env, EvalContext& ctxt) : env(env), ctxt(ctxt), bdd(env.getBDD()) {}

        // -- Operations -----------------------------

        void visitSearch(const RamSearch& search) override {
            // check condition
            auto condition = search.getCondition();
	    BDDValue thisPred = ctxt.pred(search.getLevel());
            if (condition) {
		thisPred = bdd.make_and(thisPred, eval(*condition, env, ctxt));
	    }

	    if (thisPred == BDD::FALSE()) {
                return;  // condition not valid => skip nested
            }

            // process nested
	    ctxt.pred(search.getLevel() + 1) = thisPred;
            visit(*search.getNestedOperation());
        }

        void visitScan(const RamScan& scan) override {
            // get the targeted relation
            const InterpreterRelation& rel = env.getRelation(scan.getRelation());

	    BDDValue thisPred = ctxt.pred(scan.getLevel());

            // process full scan if no index is given
            if (scan.getRangeQueryColumns() == 0) {
                // if scan is not binding anything => check for emptiness
                if (scan.isPureExistenceCheck()) {
		    ctxt.pred(scan.getLevel()) = bdd.make_not(rel.empty(thisPred));
		    if (!scan.isHypFilter() || ctxt.pred(scan.getLevel()) == BDD::TRUE()) {
			visitSearch(scan);
		    }
                    return;
                }

                // if scan is unrestricted => use simple iterator
                for (const RamDomain* cur : rel) {
                    ctxt[scan.getLevel()] = cur;
		    BDDValue tuplePred = BDD::TRUE();
		    if (env.getEnableHypotheses()) {
			tuplePred = BDDValue::from_domain(cur[rel.getArity()]);
			BDDVar tuplePredVar = BDDVar::from_domain(cur[rel.getArity() + 1]);
			tuplePred = tuplePredVar != BDD::NO_VAR() ?
			    bdd.make_and(tuplePred, bdd.make_var(tuplePredVar)) :
			    tuplePred;
			ctxt.pred(scan.getLevel()) = bdd.make_and(thisPred, tuplePred);

			if (scan.isHypFilter() && ctxt.pred(scan.getLevel()) != BDD::TRUE()) {
			    continue;
			}
		    }
                    visitSearch(scan);
                }
                return;
            }

            // create pattern tuple for range query
            auto arity = rel.getArity();
            RamDomain low[arity];
            RamDomain hig[arity];
            auto pattern = scan.getRangePattern();
            for (size_t i = 0; i < arity; i++) {
                if (pattern[i] != nullptr) {
                    low[i] = eval(pattern[i], env, ctxt);
                    hig[i] = low[i];
                } else {
                    low[i] = MIN_RAM_DOMAIN;
                    hig[i] = MAX_RAM_DOMAIN;
                }
            }

            // obtain index
            auto idx = rel.getIndex(scan.getRangeQueryColumns(), nullptr);

            // get iterator range
            auto range = idx->lowerUpperBound(low, hig);

            // if this scan is not binding anything ...
            if (scan.isPureExistenceCheck()) {
		if (env.getEnableHypotheses()) {
		    BDDValue pred = BDD::FALSE();
		    for (auto it = range.first; it != range.second; ++it) {
			const auto* tuple = *it;
			BDDValue tuplePred = BDDValue::from_domain(tuple[rel.getArity()]);
			BDDVar tupleVar = BDDVar::from_domain(tuple[rel.getArity() + 1]);
			tuplePred = tupleVar != BDD::NO_VAR() ?
			    bdd.make_and(tuplePred, bdd.make_var(tupleVar)) :
			    tuplePred;
			pred = bdd.make_or(pred, tuplePred);
		    }
		    ctxt.pred(scan.getLevel()) = bdd.make_and(thisPred, pred);
		    if (ctxt.pred(scan.getLevel()) != BDD::FALSE()) {
			visitSearch(scan);
		    }
		} else {
		    // just an existence check, so any non-empty range
		    // will match the query.
		    if (range.first != range.second) {
			visitSearch(scan);
		    }
		}
                return;
            }

	    // conduct range query
	    for (auto ip = range.first; ip != range.second; ++ip) {
		const RamDomain* data = *(ip);
		ctxt[scan.getLevel()] = data;
		BDDValue tuplePred = BDD::TRUE();
		if (env.getEnableHypotheses()) {
		    tuplePred = BDDValue::from_domain(data[rel.getArity()]);
		    BDDVar tupleVar = BDDVar::from_domain(data[rel.getArity() + 1]);
		    tuplePred = tupleVar != BDD::NO_VAR() ?
			bdd.make_and(tuplePred, bdd.make_var(tupleVar)) :
			tuplePred;
		    tuplePred = bdd.make_and(thisPred, tuplePred);
		}
		ctxt.pred(scan.getLevel()) = tuplePred;
		visitSearch(scan);
	    }
        }

        void visitLookup(const RamLookup& lookup) override {
            // get reference
            RamDomain ref = ctxt[lookup.getReferenceLevel()][lookup.getReferencePosition()];

	    // no predicate handling required: handled in
	    // `visitSearch()`, called below.

            // check for null
            if (isNull(ref)) {
                return;
            }

            // update environment variable
            auto arity = lookup.getArity();
            const RamDomain* tuple = unpack(ref, arity);

            // save reference to temporary value
            ctxt[lookup.getLevel()] = tuple;

            // run nested part - using base class visitor
            visitSearch(lookup);
        }

	void visitForall(const RamForall& forall) override {
	    size_t arity = forall.getArgs().size();

	    if (ctxt.pred(forall.getLevel()) != BDD::TRUE()) {
		return;
	    }

	    // construct the tuple
            const auto& args = forall.getArgs();
	    std::vector<RamDomain> tuple;
            for (size_t i = 0; i < arity; i++) {
                tuple.push_back(eval(args[i], env, ctxt));
            }

	    // construct the key and val from the tuple
	    std::vector<RamDomain> low;
	    std::vector<RamDomain> high;
	    SearchColumns valCols = forall.getDomVarColumns();
	    SearchColumns keyCols = ((1L << arity) - 1) ^ valCols;
	    for (size_t i = 0; i < arity; i++) {
		bool isVal = valCols & (1L << i);
		low.push_back(isVal ? MIN_RAM_DOMAIN : tuple[i]);
		high.push_back(isVal ? MAX_RAM_DOMAIN : tuple[i]);
	    }

	    // get the state for this forall instance
	    auto& state = ctxt.getForallState(low);

	    // initialize the state if needed (count domain vals)
	    if (!state.init) {
		// get the domain relation
		const InterpreterRelation& domRel = env.getRelation(*forall.getDomRelation());

		if (!keyCols) {
		    for (auto t : domRel) {
			std::vector<RamDomain> domVal(t, t + arity);
			state.domVals.insert(std::move(domVal));
		    }
		} else {
		    auto* index = domRel.getIndex(keyCols);
		    auto p = index->lowerUpperBound(low.data(), high.data());
		    for (auto i = p.first; i != p.second; ++i) {
			const RamDomain* t = *i;
			std::vector<RamDomain> domVal(t, t + arity);
			state.domVals.insert(std::move(domVal));
		    }
		}
		state.init = true;
	    }

	    // if we haven't seen this value before, record it and bump count
	    auto it = state.seenVals.find(tuple);
	    if (it == state.seenVals.end()) {
		auto domIt = state.domVals.find(tuple);
		if (domIt != state.domVals.end()) {
		    state.seenVals.insert(it, std::move(tuple));
		    if (state.seenVals.size() >= state.domVals.size()) {
			// Reached all domain values -- forall satisified!
			visit(forall.getNested());
		    }
		}
	    }
	}

        void visitAggregate(const RamAggregate& aggregate) override {
	    // get the targeted relation
            const InterpreterRelation& rel = env.getRelation(aggregate.getRelation());

	    // predicates not supported for aggregates.

	    // initialize result
            RamDomain res = 0;
            switch (aggregate.getFunction()) {
                case RamAggregate::MIN:
                    res = MAX_RAM_DOMAIN;
                    break;
                case RamAggregate::MAX:
                    res = MIN_RAM_DOMAIN;
                    break;
                case RamAggregate::COUNT:
                    res = 0;
                    break;
                case RamAggregate::SUM:
                    res = 0;
                    break;
            }

            // init temporary tuple for this level
            auto arity = rel.getArity();

            // get lower and upper boundaries for iteration
            const auto& pattern = aggregate.getPattern();
            RamDomain low[arity];
            RamDomain hig[arity];

            for (size_t i = 0; i < arity; i++) {
                if (pattern[i] != nullptr) {
                    low[i] = eval(pattern[i], env, ctxt);
                    hig[i] = low[i];
                } else {
                    low[i] = MIN_RAM_DOMAIN;
                    hig[i] = MAX_RAM_DOMAIN;
                }
            }

            // obtain index
            auto idx = rel.getIndex(aggregate.getRangeQueryColumns());

            // get iterator range
            auto range = idx->lowerUpperBound(low, hig);

            // check for emptiness
            if (aggregate.getFunction() != RamAggregate::COUNT) {
                if (range.first == range.second) {
                    return;  // no elements => no min/max
                }
            }

            // iterate through values
            for (auto ip = range.first; ip != range.second; ++ip) {
                // link tuple
                const RamDomain* data = *(ip);
                ctxt[aggregate.getLevel()] = data;

                // count is easy
                if (aggregate.getFunction() == RamAggregate::COUNT) {
                    res++;
                    continue;
                }

                // aggregation is a bit more difficult

                // eval target expression
                RamDomain cur = eval(aggregate.getTargetExpression(), env, ctxt);

                switch (aggregate.getFunction()) {
                    case RamAggregate::MIN:
                        res = std::min(res, cur);
                        break;
                    case RamAggregate::MAX:
                        res = std::max(res, cur);
                        break;
                    case RamAggregate::COUNT:
                        res = 0;
                        break;
                    case RamAggregate::SUM:
                        res += cur;
                        break;
                }
            }

            // write result to environment
            RamDomain tuple[1];
            tuple[0] = res;
            ctxt[aggregate.getLevel()] = tuple;

            // check whether result is used in a condition
            auto condition = aggregate.getCondition();
            if (condition && eval(*condition, env, ctxt) == BDD::FALSE()) {
                return;  // condition not valid => skip nested
            }

            // run nested part - using base class visitor
            visitSearch(aggregate);
        }

        void visitProject(const RamProject& project) override {
            // check constraints
            RamCondition* condition = project.getCondition();
	    BDDValue thisPred = ctxt.pred(project.getLevel());
            if (condition) {
		thisPred = bdd.make_and(thisPred, eval(*condition, env, ctxt));
	    }

	    if (thisPred == BDD::FALSE()) {
                return;  // condition violated => skip insert
            }

            // create a tuple of the proper arity (also supports arity 0)
            auto arity = project.getRelation().getArity();
            const auto& values = project.getValues();
            RamDomain tuple[arity];
            for (size_t i = 0; i < arity; i++) {
                tuple[i] = eval(values[i], env, ctxt);
            }
	    BDDVar thisVar = BDD::NO_VAR();
	    if (env.getEnableHypotheses() && project.getHypothetical()) {
		thisVar = bdd.alloc_var();
	    }

            // check filter relation
            if (project.hasFilter() &&
		env.getRelation(project.getFilter()).exists(tuple, thisPred, thisVar) == BDD::TRUE()) {
                return;
            }

            // insert in target relation
            env.getRelation(project.getRelation()).insert(tuple, thisPred, thisVar);
        }

        // -- return from subroutine --
        void visitReturn(const RamReturn& ret) override {
            for (auto val : ret.getValues()) {
                if (val == nullptr) {
                    ctxt.addReturnValue(0, true);
                } else {
                    ctxt.addReturnValue(eval(val, env, ctxt));
                }
            }
        }

        // -- safety net --
        void visitNode(const RamNode& node) override {
            std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
            assert(false && "Unsupported Node Type!");
        }
    };

    // create and run interpreter
    EvalContext ctxt(op.getDepth());
    ctxt.setReturnValues(args.getReturnValues());
    ctxt.setReturnErrors(args.getReturnErrors());
    ctxt.setArguments(args.getArguments());
    Interpreter(env, ctxt).visit(op);
}

void run(const QueryExecutionStrategy& strategy, std::ostream* report, std::ostream* profile,
        const RamStatement& stmt, InterpreterEnvironment& env) {
    class Interpreter : public RamVisitor<bool> {
        InterpreterEnvironment& env;
        const QueryExecutionStrategy& queryExecutor;
        std::ostream* report;
        std::ostream* profile;

    public:
        Interpreter(InterpreterEnvironment& env, const QueryExecutionStrategy& strategy, std::ostream* report,
                std::ostream* profile)
                : env(env), queryExecutor(strategy), report(report), profile(profile) {}

        // -- Statements -----------------------------

        bool visitSequence(const RamSequence& seq) override {
            // process all statements in sequence
            for (const auto& cur : seq.getStatements()) {
                if (!visit(cur)) {
                    return false;
                }
            }

            // all processed successfully
            return true;
        }

        bool visitParallel(const RamParallel& parallel) override {
            // get statements to be processed in parallel
            const auto& stmts = parallel.getStatements();

            // special case: empty
            if (stmts.empty()) {
                return true;
            }

            // special handling for a single child
            if (stmts.size() == 1) {
                return visit(stmts[0]);
            }

            // parallel execution
            bool cond = true;
//#pragma omp parallel for reduction(&& : cond)
            for (size_t i = 0; i < stmts.size(); i++) {
                cond = cond && visit(stmts[i]);
            }
            return cond;
        }

        bool visitLoop(const RamLoop& loop) override {
            while (visit(loop.getBody())) {
            }
            return true;
        }

        bool visitExit(const RamExit& exit) override {
            return !(eval(exit.getCondition(), env) == BDD::TRUE());
        }

        bool visitLogTimer(const RamLogTimer& timer) override {
            Logger logger(
                    timer.getMessage().c_str(), *profile, fileExtension(Global::config().get("profile")));
            return visit(timer.getStatement());
        }

        bool visitDebugInfo(const RamDebugInfo& dbg) override {
            SignalHandler::instance()->setMsg(dbg.getMessage().c_str());
            return visit(dbg.getStatement());
        }

        bool visitCreate(const RamCreate& create) override {
            env.getRelation(create.getRelation());
            return true;
        }

        bool visitClear(const RamClear& clear) override {
            env.getRelation(clear.getRelation()).purge();
            return true;
        }

        bool visitDrop(const RamDrop& drop) override {
            env.dropRelation(drop.getRelation());
            return true;
        }

        bool visitPrintSize(const RamPrintSize& print) override {
            auto lease = getOutputLock().acquire();
            (void)lease;
            std::cout << print.getMessage() << env.getRelation(print.getRelation()).size() << "\n";
            return true;
        }

        bool visitLogSize(const RamLogSize& print) override {
            auto lease = getOutputLock().acquire();
            (void)lease;
            *profile << print.getMessage() << env.getRelation(print.getRelation()).size();
            const std::string ext = fileExtension(Global::config().get("profile"));
            if (ext == "json") {
                *profile << "},";
            }
            *profile << "\n";
            return true;
        }

        bool visitLoad(const RamLoad& load) override {
            try {
                InterpreterRelation& relation = env.getRelation(load.getRelation());
                std::unique_ptr<ReadStream> reader = IOSystem::getInstance().getReader(
                        load.getRelation().getSymbolMask(), env.getSymbolTable(), load.getIODirectives(),
                        Global::config().has("provenance"), Global::config().has("predicated"));
                reader->readAll(relation);
            } catch (std::exception& e) {
                std::cerr << e.what();
                return false;
            }
            return true;
        }

        bool visitStore(const RamStore& store) override {
            for (IODirectives ioDirectives : store.getIODirectives()) {
                try {
                    IOSystem::getInstance()
                            .getWriter(store.getRelation().getSymbolMask(), env.getSymbolTable(),
				       ioDirectives, Global::config().has("provenance"),
				       env.getEnableHypotheses())
                            ->writeAll(env.getRelation(store.getRelation()));
                } catch (std::exception& e) {
                    std::cerr << e.what();
                    exit(1);
                }
            }
            return true;
        }

        bool visitFact(const RamFact& fact) override {
            auto arity = fact.getRelation().getArity();
            RamDomain tuple[arity];
            auto values = fact.getValues();

            for (size_t i = 0; i < arity; ++i) {
                tuple[i] = eval(values[i], env);
            }

            env.getRelation(fact.getRelation()).insert(tuple);
            return true;
        }

        bool visitInsert(const RamInsert& insert) override {
            // run generic query executor
            queryExecutor(insert, env, report);
            return true;
        }

        bool visitMerge(const RamMerge& merge) override {
            // get involved relation
            InterpreterRelation& src = env.getRelation(merge.getSourceRelation());
            InterpreterRelation& trg = env.getRelation(merge.getTargetRelation());

            if (dynamic_cast<InterpreterEqRelation*>(&trg)) {
                // expand src with the new knowledge generated by insertion.
                src.extend(trg);
            }
            // merge in all elements
            trg.insert(src);

            // done
            return true;
        }

        bool visitSwap(const RamSwap& swap) override {
            std::swap(env.getRelation(swap.getFirstRelation()), env.getRelation(swap.getSecondRelation()));
            return true;
        }

	bool visitForallContext(const RamForallContext& fctx) override {
	    return visit(*fctx.getNested());
	}

        // -- safety net --

        bool visitNode(const RamNode& node) override {
            auto lease = getOutputLock().acquire();
            (void)lease;
            std::cerr << "Unsupported node type: " << typeid(node).name() << "\n";
            assert(false && "Unsupported Node Type!");
            return false;
        }
    };

    // create and run interpreter
    Interpreter(env, strategy, report, profile).visit(stmt);
}
}  // namespace

void Interpreter::invoke(const RamProgram& prog, InterpreterEnvironment& env) const {
    SignalHandler::instance()->set();

    std::string recordsInFilepath = Global::getRecordInFilepath();
    if (fileExists(recordsInFilepath)) {
	std::map<std::string, std::string> readIODirectivesMap = {
	    {"IO", "file"},
	    {"filename", recordsInFilepath},
	    {"name", "souffle_records"}
	};
	IODirectives readIODirectives(readIODirectivesMap);
	try {
	    std::unique_ptr<RecordReadStream> reader = IOSystem::getInstance()
		.getRecordReader(env.getSymbolTable(), readIODirectives);
	    auto records = reader->readAllRecords();
	    for (auto r_it = records->begin(); r_it != records->end(); ++r_it) {
		for (RamDomain* record: r_it->second) {
		    pack(record, r_it->first);
		}
	    }
	} catch (std::exception& e) {
	    std::cerr << e.what();
	}
    }

    if (Global::config().has("profile")) {
        std::string fname = Global::config().get("profile");
        // open output stream
        std::ofstream os(fname);
        if (!os.is_open()) {
            throw std::invalid_argument("Cannot open profile log file <" + fname + ">");
        }
        os << AstLogStatement::startDebug() << std::endl;
        run(queryStrategy, report, &os, *(prog.getMain()), env);
    } else {
        run(queryStrategy, report, nullptr, *(prog.getMain()), env);
    }


    // std::cout << "Printing records now\n";
    std::string recordsOutFilepath = Global::getRecordOutFilepath();
    std::string symtabOutFilepath = Global::getSymtabOutFilepath();
    std::map<std::string, std::string> writeIODirectivesMap = {
	{"IO", "file"},
	{"filename", recordsOutFilepath},
	{"symtabfilename", symtabOutFilepath},
	{"name", "souffle_records"}
    };
    IODirectives writeIODirectives(writeIODirectivesMap);
    try {
	printRecords(IOSystem::getInstance()
		     .getRecordWriter(env.getSymbolTable(), writeIODirectives));
    } catch (std::exception& e) {
	std::cerr << e.what();
	exit(1);
    }

    if (env.getEnableHypotheses()) {
	try {
	    env.getBDD().writeFile(Global::getBDDNodesOutFilepath());
	} catch (std::exception& e) {
	    std::cerr << e.what();
	    exit(1);
	}
    }
    
    SignalHandler::instance()->reset();
}

/**
 * Runs a subroutine of a RamProgram
 */
void Interpreter::executeSubroutine(InterpreterEnvironment& env, const RamStatement& stmt,
        const std::vector<RamDomain>& arguments, std::vector<RamDomain>& returnValues,
        std::vector<bool>& returnErrors) const {
    EvalContext ctxt;
    ctxt.setReturnValues(returnValues);
    ctxt.setReturnErrors(returnErrors);
    ctxt.setArguments(arguments);

    // run subroutine
    const RamOperation& op = static_cast<const RamInsert&>(stmt).getOperation();
    apply(op, env, ctxt);
}

namespace {

using namespace scheduler;

Order scheduleByModel(AstClause& clause, InterpreterEnvironment& env, std::ostream* report) {
    assert(!clause.isFact());

    // check whether schedule is fixed
    if (clause.hasFixedExecutionPlan()) {
        if (report) {
            *report << "   Skipped due to fixed execution plan!\n";
        }
        return Order::getIdentity(clause.getAtoms().size());
    }

    // check whether there is actually something to schedule
    if (clause.getAtoms().size() < 2) {
        return Order::getIdentity(clause.getAtoms().size());
    }

    // TODO: provide alternative scheduling approach for larger rules
    //  8 atoms require   ~200ms to schedule
    //  9 atoms require  ~2400ms to schedule
    // 10 atoms require ~29000ms to schedule
    // 11 atoms => out of memory
    if (clause.getAtoms().size() > 8) {
        return Order::getIdentity(clause.getAtoms().size());
    }

    // get atom list
    std::vector<AstAtom*> atoms = clause.getAtoms();

    // a utility for mapping variable names to ids
    std::map<std::string, int> varIDs;
    auto getID = [&](const AstVariable& var) -> int {
        auto pos = varIDs.find(var.getName());
        if (pos != varIDs.end()) {
            return pos->second;
        }
        int id = varIDs.size();
        varIDs[var.getName()] = id;
        return id;
    };

    // fix scheduling strategy
    typedef Problem<SimpleComputationalCostModel> Problem;
    typedef typename Problem::atom_type Atom;

    // create an optimization problem
    Problem p;

    // create atoms
    for (unsigned i = 0; i < atoms.size(); i++) {
        // convert pattern of arguments
        std::vector<Argument> args;

        for (const AstArgument* arg : atoms[i]->getArguments()) {
            if (const AstVariable* var = dynamic_cast<const AstVariable*>(arg)) {
                args.push_back(Argument::createVar(getID(*var)));
            } else if (dynamic_cast<const AstUnnamedVariable*>(arg)) {
                args.push_back(Argument::createUnderscore());
            } else if (dynamic_cast<const AstConstant*>(arg)) {
                args.push_back(Argument::createConst());
            } else {
                args.push_back(Argument::createOther());
            }
        }

        // add new atom
        AstTranslator translator;
        p.addAtom(
                Atom(i, args, env.getRelation(translator.translateRelationName(atoms[i]->getName())).size()));
    }

    // solve the optimization problem
    auto schedule = p.solve();

    // log problem and solution
    if (report) {
        *report << "Scheduling Problem: " << p << "\n";
        *report << "          Schedule: " << schedule << "\n";
    }

    // extract order
    Order res;
    for (const auto& cur : schedule) {
        res.append(cur.getID());
    }

    // re-order atoms
    clause.reorderAtoms(res.getOrder());

    // done
    return res;
}
}  // namespace

/** With this strategy queries will be processed as they are stated by the user */
const QueryExecutionStrategy DirectExecution = [](
        const RamInsert& insert, InterpreterEnvironment& env, std::ostream*) -> ExecutionSummary {
    // measure the time
    auto start = now();

    // simplest strategy of all - just apply the nested operation
    apply(insert.getOperation(), env);

    // create report
    auto end = now();
    return ExecutionSummary(
            {Order::getIdentity(insert.getOrigin().getAtoms().size()), duration_in_ms(start, end)});
};

/** With this strategy queries will be dynamically rescheduled before each execution */
const QueryExecutionStrategy ScheduledExecution = [](
        const RamInsert& insert, InterpreterEnvironment& env, std::ostream* report) -> ExecutionSummary {

    // Report scheduling
    // TODO: only re-schedule atoms (avoid cloning entire clause)
    std::unique_ptr<AstClause> clause(insert.getOrigin().clone());

    Order order;

    // (re-)schedule clause
    if (report) {
        *report << "\nScheduling clause @ " << clause->getSrcLoc() << "\n";
    }
    {
        auto start = now();
        order = scheduleByModel(*clause, env, report);
        auto end = now();
        if (report) {
            *report << "    Original Query: " << insert.getOrigin() << "\n";
        }
        if (report) {
            *report << "       Rescheduled: " << *clause << "\n";
        }
        if (!equal_targets(insert.getOrigin().getAtoms(), clause->getAtoms())) {
            if (report) {
                *report << "            Order has Changed!\n";
            }
        }
        if (report) {
            *report << "   Scheduling Time: " << duration_in_ms(start, end) << "ms\n";
        }
    }

    // create operation
    std::unique_ptr<RamStatement> stmt = AstTranslator().translateClause(*clause, nullptr, nullptr);
    assert(dynamic_cast<RamInsert*>(stmt.get()));

    // run rescheduled node
    auto start = now();
    apply(static_cast<RamInsert*>(stmt.get())->getOperation(), env);
    auto end = now();
    auto runtime = duration_in_ms(start, end);
    if (report) {
        *report << "           Runtime: " << runtime << "ms\n";
    }

    return ExecutionSummary({order, runtime});
};

RelationStats RelationStats::extractFrom(const InterpreterRelation& rel, uint32_t sample_size) {
    // write each column in its own set
    std::vector<btree_set<RamDomain>> columns(rel.getArity());

    // analyze sample
    uint32_t count = 0;
    for (auto it = rel.begin(); it != rel.end() && count < sample_size; ++it, ++count) {
        const RamDomain* tuple = *it;

        // compute cardinality of columns
        for (std::size_t i = 0; i < rel.getArity(); ++i) {
            columns[i].insert(tuple[i]);
        }
    }

    // create the resulting statistics object
    RelationStats stats;

    stats.arity = rel.getArity();
    stats.size = rel.size();
    stats.sample_size = count;

    for (std::size_t i = 0; i < rel.getArity(); i++) {
        // estimate the cardinality of the columns
        uint64_t card = 0;
        if (count > 0) {
            // based on the observed probability
            uint64_t cur = columns[i].size();
            double p = ((double)cur / (double)count);

            // obtain an estimate of the overall cardinality
            card = (uint64_t)(p * rel.size());

            // make sure that it is at least what you have seen
            if (card < cur) {
                card = cur;
            }
        }

        // add result
        stats.cardinalities.push_back(card);
    }

    // done
    return stats;
}

}  // end of namespace souffle
