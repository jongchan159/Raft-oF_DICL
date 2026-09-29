#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <node-id>"
    echo "example: $0 5"
    exit 1
fi

ID="$1"

case "$ID" in
    4|5|6|7) ;;
    *)
        echo "invalid node id: $ID"
        echo "valid ids: 4, 5, 6, 7"
        exit 1
        ;;
esac

META_DIR="$(metadata_dir_for_id "$ID")"

is_live_raft_node() {
    local pid stat

    for pid in $(pgrep -f '[r]aft_node' || true); do
        stat="$(ps -o stat= -p "$pid" 2>/dev/null | xargs || true)"

        # Z = zombie/defunct. 이미 죽은 프로세스이므로 live로 보지 않는다.
        if [[ -n "$stat" && "$stat" != Z* ]]; then
            return 0
        fi
    done

    return 1
}

live_raft_pids() {
    local pid stat

    for pid in $(pgrep -f '[r]aft_node' || true); do
        stat="$(ps -o stat= -p "$pid" 2>/dev/null | xargs || true)"

        if [[ -n "$stat" && "$stat" != Z* ]]; then
            echo "$pid"
        fi
    done
}

echo "========================================"
echo "Stop + Clean Raft node"
echo "========================================"
echo "id       : $ID"
echo "metadata : $META_DIR"
echo "========================================"

#
# 1. Stop raft_node
#
if is_live_raft_node; then
    echo
    echo "[1/2] stopping raft_node..."

    PIDS="$(live_raft_pids)"

    echo "live raft_node:"
    ps -fp $PIDS || true

    echo
    echo "sending SIGTERM..."
    sudo kill -TERM $PIDS 2>/dev/null || true

    # 최대 10초 기다림
    for _ in $(seq 1 100); do
        if ! is_live_raft_node; then
            break
        fi
        sleep 0.1
    done

    if is_live_raft_node; then
        echo "raft_node did not exit; sending SIGKILL..."

        PIDS="$(live_raft_pids)"
        sudo kill -KILL $PIDS 2>/dev/null || true

        sleep 0.5
    fi
else
    echo
    echo "[1/2] no live raft_node found."
fi

if is_live_raft_node; then
    echo
    echo "ERROR: live raft_node still exists:"
    PIDS="$(live_raft_pids)"
    ps -fp $PIDS || true
    exit 1
fi

# zombie는 참고용으로만 출력
ZOMBIES="$(
    for pid in $(pgrep -f '[r]aft_node' || true); do
        stat="$(ps -o stat= -p "$pid" 2>/dev/null | xargs || true)"
        if [[ "$stat" == Z* ]]; then
            echo "$pid"
        fi
    done
)"

if [[ -n "$ZOMBIES" ]]; then
    echo
    echo "note: zombie raft_node remains (already exited):"
    ps -o pid,ppid,stat,cmd -p $(echo "$ZOMBIES" | tr '\n' ' ') || true
fi

#
# 2. Clean metadata
#
echo
echo "[2/2] cleaning metadata..."

if ! mountpoint -q /mnt/raftvol; then
    echo "ERROR: /mnt/raftvol is not mounted."
    exit 1
fi

# 잘못된 경로 삭제 방지
case "$META_DIR" in
    /mnt/raftvol/node5|/mnt/raftvol/node6|/mnt/raftvol/node7)
        ;;
    *)
        echo "ERROR: refusing to clean unexpected path:"
        echo "  $META_DIR"
        exit 1
        ;;
esac

sudo mkdir -p "$META_DIR"

sudo find "$META_DIR" \
    -mindepth 1 \
    -maxdepth 1 \
    -exec rm -rf -- {} +

echo
echo "clean complete:"
sudo ls -la "$META_DIR"

echo
echo "========================================"
echo "DONE"
echo "node $ID stopped and cleaned"
echo "========================================"