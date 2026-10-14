#include <doctest/doctest.h>

#include <random>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "net/FrameReader.hpp"

using namespace wire;
using net::FrameReader;
using net::FrameStatus;

namespace {

std::vector<uint8_t> threeFrames() {
    std::vector<uint8_t> out;
    codec::BinaryCodec::encode(makeMessage(MsgType::Bid, 1, 9, OrderRequest{1, 100, 5}), out);
    codec::BinaryCodec::encode(makeMessage(MsgType::ListFeed, 2, 9, Empty{}), out);
    codec::BinaryCodec::encode(makeMessage(MsgType::Cancel, 3, 9, CancelRequest{77}), out);
    return out;
}

// Feeds `bytes` in chunks split at the given points, collecting every frame.
std::vector<uint32_t> readAll(const std::vector<uint8_t>& bytes, const std::vector<size_t>& cuts) {
    FrameReader reader;
    std::vector<uint32_t> req_ids;
    size_t prev = 0;
    std::vector<size_t> points = cuts;
    points.push_back(bytes.size());
    for (size_t cut : points) {
        reader.feed(bytes.data() + prev, cut - prev);
        prev = cut;
        Message m;
        while (reader.next(m) == FrameStatus::Frame) req_ids.push_back(m.header.req_id);
    }
    return req_ids;
}

}  // namespace

TEST_CASE("FrameReader: several frames in one read") {
    CHECK(readAll(threeFrames(), {}) == std::vector<uint32_t>{1, 2, 3});
}

TEST_CASE("FrameReader: one byte at a time") {
    std::vector<uint8_t> bytes = threeFrames();
    std::vector<size_t> cuts;
    for (size_t i = 1; i < bytes.size(); ++i) cuts.push_back(i);
    CHECK(readAll(bytes, cuts) == std::vector<uint32_t>{1, 2, 3});
}

TEST_CASE("FrameReader: random split points") {
    std::vector<uint8_t> bytes = threeFrames();
    std::mt19937 rng(42);
    for (int round = 0; round < 500; ++round) {
        std::vector<size_t> cuts;
        for (size_t i = 1; i < bytes.size(); ++i) {
            if (rng() % 5 == 0) cuts.push_back(i);
        }
        REQUIRE(readAll(bytes, cuts) == std::vector<uint32_t>{1, 2, 3});
    }
}

TEST_CASE("FrameReader: oversize length is fatal before the payload arrives") {
    FrameReader reader(1024);
    std::vector<uint8_t> frame;
    codec::BinaryCodec::encode(makeMessage(MsgType::ListFeed, 1, 0, Empty{}), frame);
    frame[1] = 0x01;  // length 65536 > 1024
    reader.feed(frame.data(), frame.size());
    Message m;
    CHECK(reader.next(m) == FrameStatus::Fatal);
}

TEST_CASE("FrameReader: bad version is fatal, unknown type is recoverable") {
    std::vector<uint8_t> bytes = threeFrames();
    Message m;
    SUBCASE("version") {
        bytes[19] = 7;
        FrameReader reader;
        reader.feed(bytes.data(), bytes.size());
        CHECK(reader.next(m) == FrameStatus::Fatal);
        CHECK(reader.lastError() == codec::DecodeStatus::UnsupportedVersion);
    }
    SUBCASE("type") {
        bytes[18] = 0x7E;
        FrameReader reader;
        reader.feed(bytes.data(), bytes.size());
        CHECK(reader.next(m) == FrameStatus::BadFrame);
        CHECK(m.header.req_id == 1);
        CHECK(reader.next(m) == FrameStatus::Frame);  // stream stays in sync
        CHECK(m.header.req_id == 2);
    }
}

TEST_CASE("FrameReader: random byte mutations never crash or over-read") {
    const std::vector<uint8_t> clean = threeFrames();
    std::mt19937 rng(7);
    for (int round = 0; round < 20000; ++round) {
        std::vector<uint8_t> bytes = clean;
        const int flips = 1 + static_cast<int>(rng() % 4);
        for (int i = 0; i < flips; ++i) bytes[rng() % bytes.size()] = static_cast<uint8_t>(rng());
        FrameReader reader;
        reader.feed(bytes.data(), bytes.size());
        Message m;
        for (int i = 0; i < 4; ++i) {
            FrameStatus st = reader.next(m);
            if (st == FrameStatus::NeedMore || st == FrameStatus::Fatal) break;
        }
    }
    CHECK(true);  // the sanitizer builds are the real assertion here
}
