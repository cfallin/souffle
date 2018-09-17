#pragma once

#include "BDD.h"
#include "CompiledRelation.h"
#include "CompiledTuple.h"

#include <unordered_map>

namespace souffle {

template <typename rel_type1, typename rel_type2>
static void predHelperMergeWithPredicates(BDD& bdd, rel_type1* to, rel_type2* from) {
    auto ctxt = to->createContext();
    for (const auto& tuple : *from) {
        predHelperInsert(bdd, to, tuple, ctxt);
    }
}

template <typename tuple_type>
static BDDValue predHelperTuple(BDD& bdd, BDDValue parent_pred, const tuple_type& tuple) {
    BDDValue tuplePred = BDDValue::from_domain(tuple[tuple_type::arity - 2]);
    BDDVar tupleVar = BDDVar::from_domain(tuple[tuple_type::arity - 1]);
    BDD::SubFrame sf(bdd);
    if (tupleVar != BDD::NO_VAR()) {
        tuplePred = bdd.make_and(tuplePred, bdd.make_var(tupleVar));
    }
    return sf.ret(bdd.make_and(parent_pred, tuplePred));
}

template <typename Range>
static BDDValue predHelperRangeEmpty(BDD& bdd, const Range& range, BDDValue parent_pred) {
    if (range.empty()) {
        return BDD::TRUE();
    }
    BDD::SubFrame sf(bdd);
    BDDValue ret = BDD::TRUE();
    for (const auto& tuple : range) {
        BDDValue this_pred = predHelperTuple(bdd, parent_pred, tuple);
        BDDValue not_present = bdd.make_not(this_pred);
        ret = bdd.make_and(ret, not_present);
	if (ret == BDD::FALSE()) {
	    break;
	}
    }
    return sf.ret(ret);
}

template <typename Relation>
static BDDValue predHelperEmpty(BDD& bdd, const Relation& relation, BDDValue parent_pred) {
    if (relation.empty()) {
        return BDD::TRUE();
    }
    BDD::SubFrame sf(bdd);
    BDDValue ret = parent_pred;
    for (const auto& tuple : relation) {
        BDDValue this_pred = predHelperTuple(bdd, BDD::TRUE(), tuple);
        BDDValue not_present = bdd.make_not(this_pred);
        ret = bdd.make_and(ret, not_present);
	if (ret == BDD::FALSE()) {
	    break;
	}
    }
    return sf.ret(ret);
}

template <typename rel_type, typename tuple_type, typename ctxt_type>
static BDDValue predHelperNotExists(
    BDD& bdd, const rel_type& rel, const tuple_type& tuple, BDDValue pred, ctxt_type& ctxt) {
    enum { arity = tuple_type::arity - 2 };

    auto range = rel.template equalRange<typename ram::index_utils::get_full_index<arity>::type>(
	tuple, ctxt);
    if (range.begin() == range.end()) {
        return pred;
    } else {
	BDD::SubFrame sf(bdd);
        BDDValue not_exists = pred;
        for (const auto& t : range) {
	    BDDValue this_pred = predHelperTuple(bdd, BDD::TRUE(), t);
	    not_exists = bdd.make_and(not_exists, bdd.make_not(this_pred));
	    if (not_exists == BDD::FALSE()) {
		break;
	    }
        }
        return sf.ret(not_exists);
    }
}

template <typename rel_type, typename tuple_type, typename ctxt_type>
static BDDValue predHelperContains(
    BDD& bdd, const rel_type& rel, const tuple_type& tuple, BDDValue pred, ctxt_type& ctxt) {
    return bdd.make_not(predHelperNotExists(bdd, rel, tuple, pred, ctxt));
}

template <typename rel_type, typename tuple_type, typename ctxt_type>
static void predHelperInsert(BDD& bdd, rel_type* rel, const tuple_type& tuple, ctxt_type& ctxt) {
    rel->insert(tuple, ctxt);
}

}  // namespace souffle
