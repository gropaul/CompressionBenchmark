#pragma once

#include <string>

static std::string CSVEscape(std::string_view s) {
    const bool needs_quotes = s.find_first_of(",\"\n\r") != std::string_view::npos;
    std::string out;
    if (!needs_quotes) return std::string(s);
    out.reserve(s.size() + 2);
    out.push_back('"');
    for (char c : s) {
        if (c == '"') out.push_back('"'); // escape " by doubling
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}
