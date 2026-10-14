#include <doctest/doctest.h>

#include <vector>

#include "wire/ByteReader.hpp"
#include "wire/ByteWriter.hpp"

using wire::ByteReader;
using wire::ByteWriter;

TEST_CASE("ByteWriter writes big-endian") {
    std::vector<uint8_t> buf;
    ByteWriter w(buf);
    w.u8(0xAB);
    w.u16(0x0102);
    w.u32(0x03040506);
    w.u64(0x0708090A0B0C0D0EULL);

    const std::vector<uint8_t> expected = {0xAB, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                                           0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E};
    CHECK(buf == expected);
}

TEST_CASE("ByteWriter appends to existing data and patches in place") {
    std::vector<uint8_t> buf = {0xFF};
    ByteWriter w(buf);
    w.u32(0);
    w.patchU32(1, 0xDEADBEEF);
    CHECK(buf == std::vector<uint8_t>{0xFF, 0xDE, 0xAD, 0xBE, 0xEF});
}

TEST_CASE("ByteReader round-trips every width") {
    std::vector<uint8_t> buf;
    ByteWriter w(buf);
    w.u8(200);
    w.u16(65000);
    w.u32(4000000000u);
    w.u64(0xFFFFFFFFFFFFFFFFULL);

    ByteReader r(buf.data(), buf.size());
    uint8_t a = 0;
    uint16_t b = 0;
    uint32_t c = 0;
    uint64_t d = 0;
    REQUIRE((r.u8(a) && r.u16(b) && r.u32(c) && r.u64(d)));
    CHECK(a == 200);
    CHECK(b == 65000);
    CHECK(c == 4000000000u);
    CHECK(d == 0xFFFFFFFFFFFFFFFFULL);
    CHECK(r.atEnd());
}

TEST_CASE("ByteReader refuses to read past the end and consumes nothing") {
    const uint8_t buf[3] = {1, 2, 3};
    ByteReader r(buf, sizeof buf);
    uint32_t v = 0;
    CHECK_FALSE(r.u32(v));
    CHECK(r.remaining() == 3);
    uint16_t s = 0;
    CHECK(r.u16(s));
    CHECK(s == 0x0102);
    CHECK_FALSE(r.skip(2));
    CHECK(r.skip(1));
    CHECK(r.atEnd());
}
