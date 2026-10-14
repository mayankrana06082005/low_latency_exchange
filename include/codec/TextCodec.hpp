#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "codec/BinaryCodec.hpp"
#include "codec/SymbolTable.hpp"
#include "wire/Messages.hpp"

namespace codec {

// HTTP/1.1 + JSON encoding of the same logical messages (Section 5): the
// measured baseline the binary protocol is compared against.
//
//   BID          POST   /order/bid          {"max_price","ticker","type","volume"}
//   ASK          POST   /order/ask          {"min_price","ticker","type","volume"}
//   CANCEL       DELETE /order/cancel       header Order-ID
//   LIST_FEED    GET    /feed
//   SUBSCRIBE    POST   /feed/subscribe     {"groups":[..],"udp_port":n}
//   UNSUBSCRIBE  POST   /feed/unsubscribe   {"groups":[..]}
//   responses    HTTP/1.1 <status> ...      JSON body; ERROR uses 4xx + {"error","code"}
//   ORDER_UPDATE POST   /order/update       pushed by the server, no response expected
//   DISCONNECT   POST   /session/disconnect pushed by the server
//
// Headers: Client-ID, Callback-Port and Request-ID carry the binary header
// fields. Request-ID is echoed on the response because replies from different
// workers can complete out of order, so HTTP's positional pairing is not enough.
class TextCodec {
public:
    static constexpr size_t MAX_HEADER_BYTES = 8192;

    static void encode(const wire::Message& msg, const SymbolTable& symbols,
                       std::vector<uint8_t>& out);

    // Decodes one HTTP message from the front of [data, data+len).
    // Ok and UnknownType set `consumed` to the message's size; UnknownType means
    // the framing was valid but the method/path is not part of the protocol.
    static DecodeStatus decode(const uint8_t* data, size_t len, const SymbolTable& symbols,
                               uint32_t max_body, wire::Message& out, size_t& consumed);
};

}  // namespace codec
