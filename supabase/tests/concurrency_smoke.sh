#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-nafaz_test}"
export PGHOST PGPORT PGUSER PGDATABASE

psql -X -v ON_ERROR_STOP=1 -q -c "
  delete from public.execution_capacity_reservations
   where account_scope = 'capacity-concurrency-smoke';
  delete from public.execution_capacity_config
   where account_scope = 'capacity-concurrency-smoke';
  delete from public.execution_coordinator_leases
   where scope = 'coordinator-concurrency-smoke';
  select public.set_execution_capacity_config(
    'capacity-concurrency-smoke', 'indodax', 1, 100
  );
"

tmpdir="$(mktemp -d)"
cleanup() {
  psql -X -v ON_ERROR_STOP=1 -q -c "
    delete from public.execution_capacity_reservations
     where account_scope = 'capacity-concurrency-smoke';
    delete from public.execution_capacity_config
     where account_scope = 'capacity-concurrency-smoke';
    delete from public.execution_coordinator_leases
     where scope = 'coordinator-concurrency-smoke';
  " >/dev/null
  rm -rf "$tmpdir"
}
trap cleanup EXIT

# Both sessions leave the same delay window together, then contend for the
# one available slot and the same capital budget using different pair keys.
psql -X -Atq -v ON_ERROR_STOP=1 -c "
  select pg_sleep(0.5);
  select accepted
    from public.reserve_execution_capacity(
      'capacity-concurrency-smoke', 'indodax',
      'indodax:pair_a', 'capacity-cid-a', 60, 100
    );
" >"$tmpdir/capacity-a" &
pid_a=$!
psql -X -Atq -v ON_ERROR_STOP=1 -c "
  select pg_sleep(0.5);
  select accepted
    from public.reserve_execution_capacity(
      'capacity-concurrency-smoke', 'indodax',
      'indodax:pair_b', 'capacity-cid-b', 60, 100
    );
" >"$tmpdir/capacity-b" &
pid_b=$!
wait "$pid_a"
wait "$pid_b"

capacity_accepted="$(cat "$tmpdir/capacity-a" "$tmpdir/capacity-b" | awk '$0 == "t" { n++ } END { print n+0 }')"
capacity_rejected="$(cat "$tmpdir/capacity-a" "$tmpdir/capacity-b" | awk '$0 == "f" { n++ } END { print n+0 }')"
if [[ "$capacity_accepted" != "1" || "$capacity_rejected" != "1" ]]; then
  echo "capacity race failed: accepted=$capacity_accepted rejected=$capacity_rejected"
  cat "$tmpdir/capacity-a" "$tmpdir/capacity-b"
  exit 1
fi

# Two independent owners racing for one expired/absent coordinator scope must
# never both acquire it; a subsequent owner must see a monotonic fencing token.
psql -X -Atq -v ON_ERROR_STOP=1 -c "
  select pg_sleep(0.5);
  select acquired
    from public.acquire_execution_coordinator_lease(
      'coordinator-concurrency-smoke',
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 30
    );
" >"$tmpdir/coordinator-a" &
pid_a=$!
psql -X -Atq -v ON_ERROR_STOP=1 -c "
  select pg_sleep(0.5);
  select acquired
    from public.acquire_execution_coordinator_lease(
      'coordinator-concurrency-smoke',
      'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 30
    );
" >"$tmpdir/coordinator-b" &
pid_b=$!
wait "$pid_a"
wait "$pid_b"

coordinator_accepted="$(cat "$tmpdir/coordinator-a" "$tmpdir/coordinator-b" | awk '$0 == "t" { n++ } END { print n+0 }')"
coordinator_rejected="$(cat "$tmpdir/coordinator-a" "$tmpdir/coordinator-b" | awk '$0 == "f" { n++ } END { print n+0 }')"
if [[ "$coordinator_accepted" != "1" || "$coordinator_rejected" != "1" ]]; then
  echo "coordinator race failed: acquired=$coordinator_accepted rejected=$coordinator_rejected"
  cat "$tmpdir/coordinator-a" "$tmpdir/coordinator-b"
  exit 1
fi

echo "Postgres cross-connection capacity and coordinator concurrency checks passed."
