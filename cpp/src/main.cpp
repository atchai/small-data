#include <iostream>
#include <chrono>
#include "Log.h"
#include "Users.h"
#include "Terms.h"

/**
 * Run
 */
int main(int argc, char *argv[]) {
    const auto start_time = std::chrono::system_clock::now();

    // Check arguments
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <users.txt> <comments.bson> <es-index> <es-content>\n";
        return 1;
    }

    // Read users
    Users users(argv[1]);

    // Read terms
    Terms terms(argv[2], users);

    // Get most frequent terms for users and serialise
    terms.serialise(argv[3], argv[4]);

    // Log stats
    log(
        std::string("Processed ") +
        std::to_string(users.count()) + " users and " +
        std::to_string(terms.count()) + " terms"
    );

    // Log elapsed time
    const auto elapsed = std::chrono::system_clock::now() - start_time;
    const auto mins = std::chrono::duration_cast<std::chrono::minutes>(elapsed);
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(elapsed - mins);
    log(std::string("Finished in ") + std::to_string(mins.count()) + "m" + std::to_string(secs.count()) + "s");


    return 0;
}
