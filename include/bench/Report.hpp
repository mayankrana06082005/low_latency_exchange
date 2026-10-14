#pragma once

#include <string>

#include "core/LatencyRecorder.hpp"

namespace bench {

void printLatencyHeader(const char* first_column);
void printLatencyRow(const std::string& label, const core::LatencySummary& s);
// Log-scale ASCII histogram of the samples (microsecond buckets).
void printHistogram(const std::string& title, const core::LatencyRecorder& r);
// One sample per line, in microseconds. Creates the directory if needed.
bool writeCsv(const std::string& dir, const std::string& name, const core::LatencyRecorder& r);

}  // namespace bench
