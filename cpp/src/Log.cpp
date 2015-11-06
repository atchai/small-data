#include <iostream>
#include <iomanip>
#include <ctime>
#include <sys/time.h>
#include <sys/resource.h>
#include "Log.h"

/**
 * Print log message with time and memory usage
 */
void log(std::string msg, bool erase) {
    // Get timestamp
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now).count();

    // Get memory usage
    struct rusage usage;
    const auto mem = (0 == getrusage(RUSAGE_SELF, &usage)) ? usage.ru_maxrss / 1024 / 1024 : 0;

    // Log to stderr
    std::cerr << "[" << timestamp << "][" << mem << "MB" << "] " << msg << (erase ? '\r' : '\n');
}
