#ifndef TERMS_H
#define TERMS_H

#include <string>
#include <map>
#include <vector>
#include "Users.h"

typedef std::map<unsigned long, std::string> TermsMap;
typedef std::map<int, std::map<unsigned long, int>> UsersTermsMap;
typedef std::map<int, int> UsersContentMap;

class Terms {
    public:
        Terms(const char *filename, const Users &users);
        void serialise(const char *es_index, const char *es_content) const;
        const int count() const;

    private:
        void tokenise(const char *str, int user_id);

        static const char *KEY_USER;
        static const char *KEY_CONTENT;
        static const char *PREFIX_HTTP;
        static const char *PREFIX_HTTPS;
        static const std::vector<std::string> STOP_WORDS;
        const Users &users;
        TermsMap terms;
        UsersTermsMap users_terms;
        UsersContentMap users_content;
};

#endif
