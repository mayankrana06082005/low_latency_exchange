#include <doctest/doctest.h>

#include <thread>

#include "core/OrderBook.hpp"
#include "core/SpscQueue.hpp"
#include "core/Worker.hpp"
#include "net/Mailbox.hpp"

using namespace core;
using namespace wire;

TEST_CASE("SpscQueue: FIFO, capacity, failed push leaves value intact") {
    SpscQueue<int> q(3);  // rounds up to 4
    CHECK(q.capacity() == 4);
    for (int i = 0; i < 4; ++i) {
        int v = i;
        CHECK(q.tryPush(std::move(v)));
    }
    int extra = 99;
    CHECK_FALSE(q.tryPush(std::move(extra)));
    CHECK(extra == 99);
    int out = -1;
    for (int i = 0; i < 4; ++i) {
        REQUIRE(q.tryPop(out));
        CHECK(out == i);
    }
    CHECK_FALSE(q.tryPop(out));
}

TEST_CASE("SpscQueue: two-thread stress keeps order (run under TSan)") {
    SpscQueue<uint64_t> q(1024);
    constexpr uint64_t kCount = 1000000;
    std::thread producer([&q] {
        for (uint64_t i = 1; i <= kCount; ++i) {
            uint64_t v = i;
            while (!q.tryPush(std::move(v))) std::this_thread::yield();
        }
    });
    uint64_t expected = 1;
    bool in_order = true;
    while (expected <= kCount) {
        uint64_t v = 0;
        if (!q.tryPop(v)) continue;
        in_order = in_order && v == expected;
        ++expected;
    }
    producer.join();
    CHECK(in_order);
}

TEST_CASE("OrderBook: price-time matching at the resting price") {
    OrderBook book;
    CHECK(book.submit(1, Side::Buy, 100, 10).fills.empty());
    CHECK(book.submit(2, Side::Buy, 101, 5).fills.empty());
    CHECK(book.submit(3, Side::Buy, 100, 7).fills.empty());

    MatchResult r = book.submit(4, Side::Sell, 100, 12);
    CHECK(r.remaining == 0);
    REQUIRE(r.fills.size() == 2);
    CHECK(r.fills[0].resting_order_id == 2);  // best price first
    CHECK(r.fills[0].price == 101);
    CHECK(r.fills[1].resting_order_id == 1);  // then time priority at 100
    CHECK(r.fills[1].vol == 7);
    CHECK(r.fills[1].resting_remaining == 3);

    std::array<Level, BOOK_LEVELS> levels{};
    CHECK(book.topLevels(Side::Buy, levels) == 1);
    CHECK(levels[0].price == 100);
    CHECK(levels[0].vol == 10);  // 3 left on order 1 + 7 on order 3

    Volume cancelled = 0;
    CHECK(book.cancel(3, cancelled));
    CHECK(cancelled == 7);
    CHECK_FALSE(book.cancel(3, cancelled));
    CHECK_FALSE(book.cancel(2, cancelled));  // already filled
}

TEST_CASE("OrderBook: a non-crossing order rests with its full volume") {
    OrderBook book;
    book.submit(1, Side::Sell, 105, 4);
    MatchResult r = book.submit(2, Side::Buy, 104, 4);
    CHECK(r.fills.empty());
    CHECK(r.remaining == 4);
    CHECK(book.restingCount() == 2);
}

TEST_CASE("Mailbox: coalesces per order, keeps first-insertion order, never drops") {
    net::Mailbox box;
    box.merge(OrderAck{10, OrderStatus::PartialFill, 6, {Trade{1, 50, 3, 99}}});
    box.merge(OrderAck{20, OrderStatus::Cancelled, 0, {}});
    box.merge(OrderAck{10, OrderStatus::PartialFill, 3, {Trade{2, 50, 3, 98}}});
    box.merge(OrderAck{10, OrderStatus::Filled, 0, {Trade{3, 50, 3, 97}}});
    CHECK(box.size() == 2);

    std::vector<OrderAck> all = box.takeAll();
    CHECK(box.empty());
    REQUIRE(all.size() == 2);
    CHECK(all[0].order_id == 10);
    CHECK(all[0].status == OrderStatus::Filled);
    CHECK(all[0].remaining_vol == 0);
    REQUIRE(all[0].trades.size() == 3);
    CHECK(all[0].trades[2].trade_id == 3);
    CHECK(all[1].order_id == 20);
}

TEST_CASE("Worker: order ids carry the worker id; fills produce ORDER_UPDATE") {
    SpscQueue<WorkItem> in(16);
    SpscQueue<WorkerOutput> out(16);
    WorkerConfig cfg;
    cfg.worker_id = 3;
    cfg.groups = {GroupConfig{0, "g", {SymbolConfig{1, "AAA"}}}};
    Worker worker(cfg, in, out);

    WorkItem bid;
    bid.type = MsgType::Bid;
    bid.client_id = 111;
    bid.req_id = 5;
    bid.symbol_id = 1;
    bid.price = 10;
    bid.volume = 4;
    worker.process(bid);

    Message m;
    REQUIRE(out.tryPop(m));
    CHECK(m.header.type == MsgType::OrderAck);
    CHECK(m.header.client_id == 111);
    CHECK(m.header.req_id == 5);
    const uint64_t resting_id = std::get<OrderAck>(m.payload).order_id;
    CHECK((resting_id >> 56) == 3);

    WorkItem ask = bid;
    ask.type = MsgType::Ask;
    ask.client_id = 222;
    ask.volume = 1;
    worker.process(ask);
    REQUIRE(out.tryPop(m));
    CHECK(m.header.type == MsgType::OrderAck);
    CHECK(std::get<OrderAck>(m.payload).status == OrderStatus::Filled);
    REQUIRE(out.tryPop(m));
    CHECK(m.header.type == MsgType::OrderUpdate);
    CHECK(m.header.client_id == 0);  // worker does not know the owner
    CHECK(std::get<OrderAck>(m.payload).order_id == resting_id);
    CHECK(std::get<OrderAck>(m.payload).remaining_vol == 3);
}
