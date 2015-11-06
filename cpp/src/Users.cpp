#include <fstream>
#include <sstream>
#include "Log.h"
#include "Users.h"

/**
 * Read in user name to user ID map
 */
Users::Users(const char *filename) {
    std::ifstream file(filename);
    if (!file) {
        throw std::runtime_error(std::string("Failed to open users file: ") + filename);
    }

    log("Reading users file");
    std::string line, name;
    int id;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        if (iss >> id >> name) {
            users[id] = name;
        }
    }
}

/**
 * Get count of users
 */
int Users::count() const {
    return users.size();
}

/**
 * Check user ID exists
 */
bool Users::exists(int id) const {
    return (users.find(id) != users.end());
}

/**
 * Get user ID from name
 */
std::string Users::get(int id) const {
    if (users.find(id) == users.end()) {
        return "";
    }
    return users.at(id);
}
