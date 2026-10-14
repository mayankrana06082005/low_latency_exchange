#!/usr/bin/env bash
# Runs a command while capturing loopback traffic, then prints a packet summary.
#
#   tools/capture.sh <output.pcapng> <command...>
#   tools/capture.sh build/sim.pcapng ./build/debug/simulation --quiet
#
# Open the result with the protocol decoder:
#   wireshark -X lua_script:tools/exchange.lua build/sim.pcapng
set -euo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $0 <output.pcapng> <command...>" >&2
    exit 2
fi

out="$1"
shift
here="$(cd "$(dirname "$0")" && pwd)"
lua="$here/exchange.lua"
mkdir -p "$(dirname "$out")"

# Capture as the wireshark group if this login session does not have it yet.
# The inner shell records tshark's own PID so it can be stopped cleanly.
pidfile="$(mktemp)"
capture_cmd="echo \$\$ > '$pidfile'; exec tshark -q -i lo -f 'tcp or udp' -w '$out'"
if id -nG | grep -qw wireshark; then
    sh -c "$capture_cmd" 2>/dev/null &
else
    sg wireshark -c "$capture_cmd" 2>/dev/null &
fi
launcher=$!
sleep 1.5  # let the capture start before traffic flows

status=0
"$@" || status=$?

sleep 1
kill -INT "$(cat "$pidfile")" 2>/dev/null || true
wait "$launcher" 2>/dev/null || true
rm -f "$pidfile"

ts() { tshark -r "$out" -X lua_script:"$lua" "$@" 2>/dev/null; }
count() { ts -Y "$1" | wc -l; }
bytes() { ts -Y "$1" -T fields -e frame.len | awk '{s += $1} END {print s + 0}'; }

echo
echo "== capture: $out"
printf "  %-44s %10s %12s\n" "" "packets" "wire bytes"
printf "  %-44s %10s %12s\n" "all TCP + UDP on loopback" "$(count 'tcp or udp')" "$(bytes 'tcp or udp')"
printf "  %-44s %10s %12s\n" "custom protocol segments (exchange)" "$(count exchange)" "$(bytes exchange)"
printf "  %-44s %10s %12s\n" "HTTP/1.1 + JSON segments (http)" "$(count http)" "$(bytes http)"
printf "  %-44s %10s %12s\n" "UDP market data (exchange_md)" "$(count exchange_md)" "$(bytes exchange_md)"
printf "  %-44s %10s\n" "TCP connections opened (SYN)" "$(count 'tcp.flags.syn==1 && tcp.flags.ack==0')"
printf "  %-44s %10s\n" "TCP connections closed (FIN)" "$(count 'tcp.flags.fin==1')"
echo
echo "  messages by type (custom protocol):"
ts -Y exchange -T fields -e exchange.type | tr ',' '\n' | sed '/^$/d' | sort | uniq -c | sort -rn |
    while read -r n t; do
        name=$(awk -v t="$t" 'BEGIN {
            split("0x01 BID 0x02 ASK 0x03 CANCEL 0x04 LIST_FEED 0x05 SUBSCRIBE 0x06 UNSUBSCRIBE 0x81 ORDER_ACK 0x83 CANCEL_ACK 0x84 LIST_FEED_RSP 0x85 SUBSCRIBE_ACK 0x86 UNSUBSCRIBE_ACK 0x8f ERROR 0xc1 ORDER_UPDATE 0xc2 DISCONNECT", a, " ")
            for (i = 1; i < 28; i += 2) if (a[i] == tolower(t)) print a[i + 1] }')
        printf "    %-18s %8s\n" "${name:-$t}" "$n"
    done
echo
echo "  open in Wireshark:  wireshark -X lua_script:$lua $out"
exit "$status"
