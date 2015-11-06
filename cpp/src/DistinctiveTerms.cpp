#include "DistinctiveTerms.h"

void DistinctiveTerms::add(DistinctiveTerm term) {
    if (terms.size() < DistinctiveTerms::SIZE) {
        terms.push(term);
    } else if (term.second > terms.top().second) {
        terms.pop();
        terms.push(term);
    }
}

DistinctiveTerm DistinctiveTerms::top() {
    return terms.top();
}

void DistinctiveTerms::pop() {
    terms.pop();
}

bool DistinctiveTerms::empty() {
    return terms.empty();
}
