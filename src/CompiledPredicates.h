#pragma once

#include "BDD.h"
#include "CompiledRelation.h"
#include "CompiledTuple.h"

#include <unordered_map>

namespace souffle {

template <typename rel_type1, typename rel_type2>
static void predHelperMergeWithPredicates(BDD& bdd, rel_type1* to, rel_type2* from) {
    for (const auto& tuple : *from) {
	std::cout << "merge: tuple " << tuple << "\n";
        predHelperInsert(bdd, to, tuple);
    }

    std::cout << "---- now:\n";
    for (const auto& tuple : *to) {
	std::cout << tuple << "\n";
    }
    std::cout << "-----\n";
}

template <typename tuple_type>
static BDDValue predHelperTuple(BDD& bdd, BDDValue parent_pred, const tuple_type& tuple) {
    BDDValue tuplePred = static_cast<BDDValue>(tuple[tuple_type::arity - 2]);
    BDDVar tupleVar = static_cast<BDDVar>(tuple[tuple_type::arity - 1]);
    if (tupleVar != 0) {
        tuplePred = bdd.make_and(tuplePred, bdd.make_var(tupleVar));
    }
    return bdd.make_and(parent_pred, tuplePred);
}

template <typename Range>
static BDDValue predHelperRangeEmpty(BDD& bdd, const Range& range, BDDValue parent_pred) {
    if (range.empty()) {
        return BDD::TRUE;
    }
    BDDValue ret = BDD::TRUE;
    for (const auto& tuple : range) {
        BDDValue this_pred = predHelperTuple(bdd, parent_pred, tuple);
	std::cout << "** RangeEmpty: tuple " << tuple << " this_pred " << this_pred << "\n";
        BDDValue not_present = bdd.make_not(this_pred);
	std::cout << "** not present: " << not_present << "\n";
        ret = bdd.make_and(ret, not_present);
	std::cout << "** running AND: " << ret << "\n";
    }
    std::cout << "*** answer: " << ret << "\n";
    return ret;
}

template <typename rel_type, typename tuple_type>
static BDDValue predHelperNotExists(
        BDD& bdd, const rel_type& rel, const tuple_type& tuple, BDDValue parent_pred) {
    enum { arity = tuple_type::arity - 2 };

    auto range = rel->template equalRange<typename ram::index_utils::get_full_index<arity>::type>(tuple);
    if (range.begin() == range.end()) {
        return BDD::TRUE;
    } else {
        BDDValue exists = BDD::FALSE;
        for (const auto& t : range) {
            BDDValue thisPred = predHelperTuple(bdd, BDD::TRUE, t);
            exists = bdd.make_or(exists, thisPred);
        }
        return bdd.make_not(exists);
    }
}

template <typename rel_type, typename tuple_type>
static void predHelperInsert(BDD& bdd, rel_type* rel, const tuple_type& tuple) {
    enum { arity = tuple_type::arity - 2 };

    std::cout << "predHelperInsert: tuple " << tuple << "\n";

    auto range = rel->template equalRange<typename ram::index_utils::get_full_index<arity>::type>(tuple);
    if (range.begin() == range.end()) {
	std::cout << " -> inserting new: " << tuple << "\n";
        rel->insert(tuple);
    } else {
        auto& to_tuple = *range.begin();
	std::cout << " -> already exists: " << to_tuple << "\n";
        if (to_tuple[arity + 1] != 0 || tuple[arity + 1] != 0) {
	    std::cout << "   -> predvar so can't merge; inserting new\n";
            // if predvar != 0, can't merge
            rel->insert(tuple);
        } else {
	    std::cout << "    -> merging pred\n";
            const_cast<RamDomain&>(to_tuple[arity]) = bdd.make_or(to_tuple[arity], tuple[arity]);
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

	std::cout << "computeOneKey: val tuple " << key << "\n";

	std::unordered_map<const RamDomain*, BDDValue, Hasher, Equal> valPreds;

	for (const auto& valTuple : valRange) {
	    std::cout << " -> val tuple (with same key) " << valTuple << "\n";
	    BDDValue p = predHelperTuple(bdd, BDD::TRUE, valTuple);
	    std::cout << "    -> pred " << p << "\n";
	    const RamDomain* key = &valTuple[0];
	    valPreds.insert(std::make_pair(key, p));
	}

	BDDValue ret = BDD::TRUE;
	for (const auto& domTuple : domRange) {
	    std::cout << " -> dom tuple " << domTuple << "\n";
	    BDDValue p = predHelperTuple(bdd, BDD::TRUE, domTuple);
	    std::cout << "    -> pred " << p << "\n";
	    const RamDomain* key = &domTuple[0];
	    auto it = valPreds.find(key);
	    if (it == valPreds.end()) {
		return BDD::FALSE;
	    }
	    BDDValue dom = it->second;
	    BDDValue valOrNotDom = bdd.make_or(p, bdd.make_not(dom));
	    ret = bdd.make_and(ret, valOrNotDom);
	}
	return ret;
    }
};

}  // namespace souffle
