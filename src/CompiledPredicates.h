#pragma once

#include "BDD.h"
#include "CompiledTuple.h"
#include "CompiledRelation.h"

namespace souffle {

template<unsigned arity, typename Derived1, typename Derived2>
static void predHelperMergeWithPredicates(BDD& bdd,
					  RelationBase<arity, Derived1>* to,
					  RelationBase<arity, Derived2>* from) {
    // TODO(cfallin)
}

template<typename Domain, unsigned arity>
static BDDValue predHelperTuple(BDD& bdd, BDDValue parent_pred, const Tuple<Domain, arity>& tuple) {
    BDDValue tuplePred = tuple[arity - 2];
    BDDVar tupleVar = tuple[arity - 1];
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

template<typename Domain, unsigned keyArity, unsigned valueArity>
class PredHelperForallContext {
    BDD& bdd;
public:
    PredHelperForallContext(BDD& bdd) : bdd(bdd) {}

    template<typename Derived>
    BDDValue value(const Tuple<Domain, keyArity>& key, const Tuple<Domain, valueArity>& value,
		   const RelationBase<arity, Derived>* domainRel) {
	// TODO(cfallin)
	return BDD::FALSE;
    }
};
    
}  // namespace souffle
