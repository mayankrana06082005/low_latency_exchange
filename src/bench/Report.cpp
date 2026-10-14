#include "bench/Report.hpp"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <vector>

namespace bench {

void printLatencyHeader(const char* first_column) {
    std::printf("  %-22s %9s %9s %9s %9s %9s %9s %9s\n", first_column, "count", "mean", "p50",
                "p90", "p99", "p99.9", "max");
}

void printLatencyRow(const std::string& label, const core::LatencySummary& s) {
    std::printf("  %-22s %9zu %9.1f %9.1f %9.1f %9.1f %9.1f %9.1f\n", label.c_str(), s.count,
                s.mean_us, s.p50_us, s.p90_us, s.p99_us, s.p999_us, s.max_us);
}

void printHistogram(const std::string& title, const core::LatencyRecorder& r) {
    if (r.empty()) return;
    // Bucket b holds samples in [2^(b-1), 2^b) microseconds; bucket 0 is < 1 us.
    std::vector<size_t> buckets(24, 0);
    for (uint64_t ns : r.samples()) {
        uint64_t us = ns / 1000;
        size_t b = 0;
        while (us > 0 && b + 1 < buckets.size()) {
            us >>= 1;
            ++b;
        }
        ++buckets[b];
    }
    const size_t total = r.samples().size();
    const size_t peak = *std::max_element(buckets.begin(), buckets.end());
    size_t first = 0;
    size_t last = buckets.size() - 1;
    while (buckets[first] == 0) ++first;
    while (buckets[last] == 0) --last;

    std::printf("\n  %s (microseconds, log scale)\n", title.c_str());
    for (size_t b = first; b <= last; ++b) {
        const unsigned long long lo = b == 0 ? 0ULL : 1ULL << (b - 1);
        const unsigned long long hi = 1ULL << b;
        const int width = static_cast<int>(50.0 * static_cast<double>(buckets[b]) / static_cast<double>(peak));
        std::printf("  %7llu - %-7llu |%-50s| %6.2f%%\n", lo, hi, std::string(static_cast<size_t>(width), '#').c_str(),
                    100.0 * static_cast<double>(buckets[b]) / static_cast<double>(total));
    }
}

bool writeCsv(const std::string& dir, const std::string& name, const core::LatencyRecorder& r) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    std::ofstream out(std::filesystem::path(dir) / (name + ".csv"));
    if (!out) return false;
    out << "latency_us\n";
    for (uint64_t ns : r.samples()) out << static_cast<double>(ns) / 1000.0 << "\n";
    return true;
}

}  // namespace bench
