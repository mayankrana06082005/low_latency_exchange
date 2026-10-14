#pragma once

namespace core {

// Exponentially weighted moving average: value += alpha * (sample - value).
// The first sample primes it. alpha = 1/8 matches TCP's smoothed RTT.
class Ewma {
public:
    explicit Ewma(double alpha = 0.125) : alpha_(alpha) {}

    double update(double sample) {
        value_ = primed_ ? value_ + alpha_ * (sample - value_) : sample;
        primed_ = true;
        return value_;
    }

    double value() const { return value_; }
    bool primed() const { return primed_; }

private:
    double alpha_;
    double value_ = 0;
    bool primed_ = false;
};

}  // namespace core
