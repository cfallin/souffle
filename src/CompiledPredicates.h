#pragma once

#include "BDD.h"
#include "CompiledRelation.h"
#include "CompiledTuple.h"

#include <unordered_map>

namespace souffle {

template <typename rel_type1, typename rel_type2>
static void predHelperMergeWithPredicates(BDD& bdd, rel_type1* to, rel_type2* from) {
    for (const auto& tuple : *from) {
        predHelperInsert(bdd, to, tuple);
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
    }
    return sf.ret(ret);
}

template <typename rel_type, typename tuple_type>
static BDDValue predHelperNotExists(
        BDD& bdd, const rel_type& rel, const tuple_type& tuple, BDDValue parent_pred) {
    enum { arity = tuple_type::arity - 2 };

    auto range = rel->template equalRange<typename ram::index_utils::get_full_index<arity>::type>(tuple);
    if (range.begin() == range.end()) {
        return BDD::TRUE();
    } else {
	BDD::SubFrame sf(bdd);
        BDDValue exists = BDD::FALSE();
        for (const auto& t : range) {
            BDDValue thisPred = predHelperTuple(bdd, BDD::TRUE(), t);
            exists = bdd.make_or(exists, thisPred);
        }
        return sf.ret(bdd.make_not(exists));
    }
}

template <typename rel_type, typename tuple_type>
static void predHelperInsert(BDD& bdd, rel_type* rel, const tuple_type& tuple) {
    enum { arity = tuple_type::arity - 2 };

    auto range = rel->template equalRange<typename ram::index_utils::get_full_index<arity>::type>(tuple);
    if (range.begin() == range.end()) {
        rel->insert(tuple);
    } else {
        auto& to_tuple = *range.begin();
        if (to_tuple[arity + 1] != 0 || tuple[arity + 1] != 0) {
            // if predvar != 0, can't merge
            rel->insert(tuple);
        } else {
            const_cast<RamDomain&>(to_tuple[arity]) =
		bdd.make_or(BDDValue::from_domain(to_tuple[arity]),
			    BDDValue::from_domain(tuple[arity])).as_domain();
        }
    }
}

template<unsigned arity, unsigned... keycols>
struct PredHelperForall {
    BDD& bdd;

    PredHelperForall(BDD& bdd) : bdd(bdd) {}

    struct Hasher {
	size_t operator()(const RamDomain* const& t) const {
	    size_t h = 1;
	    for (size_t i = 0; i < arity; i++) {
		h = (h * 97303) + t[i];
	    }
	    return h;
	}
    };
    struct Equal {
	bool operator()(const RamDomain* const& t1, const RamDomain* const& t2) const {
	    for (size_t i = 0; i < arity; i++) {
		if (t1[i] != t2[i]) {
		    return false;
		}
	    }
	    return true;
	}
    };

    template<typename TupleType, typename DomRel, typename ValRel>
    BDDValue computeOneKey(const DomRel& domain,
			   const ValRel& values,
			   const TupleType& key) {
	auto domRange = domain.template equalRange<keycols...>(key);
	auto valRange = values.template equalRange<keycols...>(key);

	std::unordered_map<const RamDomain*, BDDValue, Hasher, Equal> valPreds;

	for (const auto& valTuple : valRange) {
	    BDDValue p = predHelperTuple(bdd, BDD::TRUE(), valTuple);
	    const RamDomain* key = &valTuple[0];
	    valPreds.insert(std::make_pair(key, p));
	}

	BDD::SubFrame sf(bdd);
	BDDValue ret = BDD::TRUE();
	for (const auto& domTuple : domRange) {
	    BDDValue dom = predHelperTuple(bdd, BDD::TRUE(), domTuple);
	    const RamDomain* key = &domTuple[0];
	    auto it = valPreds.find(key);
	    if (it == valPreds.end()) {
		return BDD::FALSE();
	    }
	    BDDValue val = it->second;
	    BDDValue valOrNotDom = bdd.make_or(val, bdd.make_not(dom));
	    ret = bdd.make_and(ret, valOrNotDom);
	}
	return sf.ret(ret);
    }
};

}  // namespace souffle
