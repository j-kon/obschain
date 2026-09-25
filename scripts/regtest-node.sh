#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATADIR="${WORKSPACE_ROOT}/.regtest"
CONF_FILE="${DATADIR}/bitcoin.conf"

mkdir -p "${DATADIR}"

if [ ! -f "${CONF_FILE}" ]; then
    cat <<EOF > "${CONF_FILE}"
regtest=1
server=1
txindex=1
fallbackfee=0.0001

[regtest]
rpcbind=127.0.0.1
rpcport=18443
rpcallowip=127.0.0.1
zmqpubrawtx=tcp://127.0.0.1:28332
zmqpubrawblock=tcp://127.0.0.1:28333
zmqpubsequence=tcp://127.0.0.1:28334
EOF
    echo "Created regtest config at ${CONF_FILE}"
fi

ACTION="${1:-status}"
shift || true

case "${ACTION}" in
    start)
        echo "Starting bitcoind regtest daemon with datadir ${DATADIR}..."
        bitcoind -datadir="${DATADIR}" -daemon
        echo "Waiting for bitcoind to warm up..."
        until bitcoin-cli -datadir="${DATADIR}" getblockchaininfo >/dev/null 2>&1; do
            sleep 0.5
        done
        echo "Bitcoin Core regtest is ready on RPC port 18443 and ZMQ 28332-28334"
        bitcoin-cli -datadir="${DATADIR}" getblockchaininfo
        ;;
    stop)
        echo "Stopping bitcoind regtest daemon..."
        bitcoin-cli -datadir="${DATADIR}" stop || true
        ;;
    status)
        bitcoin-cli -datadir="${DATADIR}" getblockchaininfo
        ;;
    cli)
        bitcoin-cli -datadir="${DATADIR}" "$@"
        ;;
    mine)
        BLOCKS="${1:-1}"
        WALLET="${2:-obschain_wallet}"
        # Ensure a wallet exists
        if ! bitcoin-cli -datadir="${DATADIR}" listwallets | grep -q "${WALLET}"; then
            bitcoin-cli -datadir="${DATADIR}" createwallet "${WALLET}" >/dev/null 2>&1 || \
            bitcoin-cli -datadir="${DATADIR}" loadwallet "${WALLET}" >/dev/null 2>&1 || true
        fi
        ADDR=$(bitcoin-cli -datadir="${DATADIR}" -rpcwallet="${WALLET}" getnewaddress)
        bitcoin-cli -datadir="${DATADIR}" generatetoaddress "${BLOCKS}" "${ADDR}"
        ;;
    clean)
        echo "Stopping bitcoind if running..."
        bitcoin-cli -datadir="${DATADIR}" stop >/dev/null 2>&1 || true
        sleep 1
        echo "Removing ${DATADIR}..."
        rm -rf "${DATADIR}"
        echo "Cleaned regtest state."
        ;;
    *)
        echo "Usage: $0 {start|stop|status|cli <args>|mine <blocks>|clean}"
        exit 1
        ;;
esac
