#ifndef USERS_H
#define USERS_H

#include <string>
#include <map>

class Users {
    public:
        Users(const char *filename);
        int count() const;
        bool exists(int id) const;
        std::string get(int id) const;

    private:
        std::map<int, std::string> users;
};

#endif
