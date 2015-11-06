#ifndef DISTINCTIVE_TERMS_H
#define DISTINCTIVE_TERMS_H

#include <queue>
#include <vector>

typedef std::pair<unsigned long, int> DistinctiveTerm;

class DistinctiveTermsCompare {
    public:
        bool operator()(DistinctiveTerm n1, DistinctiveTerm n2) {
            return n1.second > n2.second;
        }
};

class DistinctiveTerms {
    public:
        void add(DistinctiveTerm term);
        DistinctiveTerm top();
        void pop();
        bool empty();

    private:
        static const int SIZE = 10;
        std::priority_queue<DistinctiveTerm, std::vector<DistinctiveTerm>, DistinctiveTermsCompare> terms;
};

#endif
