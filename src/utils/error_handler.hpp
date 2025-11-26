#pragma once

#include <string>
#include <stdexcept>
#include <iostream>

namespace ErrorHandler {

// Global flag to control error behavior
inline bool log_errors_instead_of_throw = false;

// Set the error handling mode
inline void SetLogErrorsMode(bool should_log) {
    log_errors_instead_of_throw = should_log;
}

// Get the current error handling mode
inline bool IsLogErrorsMode() {
    return log_errors_instead_of_throw;
}

// Handle a runtime error - either log it or throw it
inline void HandleRuntimeError(const std::string& message) {
    if (log_errors_instead_of_throw) {
        std::cerr << "[ERROR] " << message << std::endl;
    } else {
        throw std::runtime_error(message);
    }
}

// Handle a logic error - either log it or throw it
inline void HandleLogicError(const std::string& message) {
    if (log_errors_instead_of_throw) {
        std::cerr << "[ERROR] " << message << std::endl;
    } else {
        throw std::logic_error(message);
    }
}

// Handle an invalid argument error - either log it or throw it
inline void HandleInvalidArgumentError(const std::string& message) {
    if (log_errors_instead_of_throw) {
        std::cerr << "[ERROR] " << message << std::endl;
    } else {
        throw std::invalid_argument(message);
    }
}

// Handle an out of range error - either log it or throw it
inline void HandleOutOfRangeError(const std::string& message) {
    if (log_errors_instead_of_throw) {
        std::cerr << "[ERROR] " << message << std::endl;
    } else {
        throw std::out_of_range(message);
    }
}

} // namespace ErrorHandler
