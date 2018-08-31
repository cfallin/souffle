#pragma once

#include "BDD.h"
#include "CompiledTuple.h"
#include "CompiledRelation.h"

namespace souffle {

template<unsigned arity, typename Derived1, typename Derived2>
static void predHelperMergeWithPredicates(BDD& bdd,
					  ram::detail::RelationBase<arity, Derived1>* to,
					  ram::detail::RelationBase<arity, Derived2>* from) {
    // TODO(cfallin)
}

template<typename tuple_type>
static BDDValue predHelperTuple(BDD& bdd, BDDValue parent_pred, const tuple_type& tuple) {
    BDDValue tuplePred = static_cast<BDDValue>(tuple[tuple_type::arity - 2]);
    BDDVar tupleVar = static_cast<BDDVar>(tuple[tuple_type::arity - 1]);
    if (tupleVar != 0) {
	tuplePred = bdd.make_and(tuplePred, bdd.make_var(tupleVar));
    }
    return bdd.make_and(parent_pred, tuplePred);
}

template<typename Range>
static BDDValue predHelperRangeEmpty(BDD& bdd, const Range& range, BDDValue parent_pred) {
    // TODO(cfallin)
    return parent_pred;
}

template<typename rel_type, typename tuple_type>
static BDDValue predHelperNotExists(BDD& bdd,
				    const rel_type& rel,
				    const tuple_type& tuple,
				    BDDValue parent_pred) {
    // TODO(cfallin)
    return BDD::FALSE;
}

template<typename rel_type, typename tuple_type>
static void predHelperInsert(rel_type* rel, const tuple_type& tuple) {
    // TODO(cfallin)
}
    
template<unsigned keyArity, unsigned valueArity>
class PredHelperForallContext {
    enum { arity = keyArity + valueArity };

    BDD& bdd;
public:
    PredHelperForallContext(BDD& bdd) : bdd(bdd) {}

    template<typename key_tuple_type, typename value_tuple_type, typename dom_rel_type>
    BDDValue value(const key_tuple_type& key,
		   const value_tuple_type& value,
		   const dom_rel_type& domainRel,
		   BDDValue pred) {
	// TODO(cfallin)
	return BDD::FALSE;
    }
};
    
}  // namespace souffle
