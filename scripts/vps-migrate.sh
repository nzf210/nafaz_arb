#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Read a simple KEY=VALUE .env file without sourcing/executing shell code.
# Explicit process environment always takes precedence over .env.
ENV_FILE="${NAFAZ_ENV_FILE:-$ROOT_DIR/.env}"
if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    line="${line#export }"
    [[ "$line" == *=* ]] || continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key//[[:space:]]/}"
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    case "$key" in
      NAFAZ_MIGRATION_TARGET|MIGRATION_BACKUP_DIR|EXPECTED_SUPABASE_PROJECT_REF|PGHOST|PGPORT|PGDATABASE|PGUSER|PGPASSWORD|DATABASE_URL|POSTGRES_DSN|BACKUP_DATABASE_PROJECT_REF) ;;
      *) continue ;;
    esac
    if [[ ! -v "$key" ]]; then
      value="${value#\"}"; value="${value%\"}"
      value="${value#\'}"; value="${value%\'}"
      printf -v "$key" '%s' "$value"
      export "$key"
    fi
  done < "$ENV_FILE"
fi

TARGET="${NAFAZ_MIGRATION_TARGET:-local}"
MODE=dry-run
BACKUP_DIR="${MIGRATION_BACKUP_DIR:-/var/backups/nafaz-db-migrations}"
EXPECTED_REF="${EXPECTED_SUPABASE_PROJECT_REF:-}"

usage() {
  cat <<'USAGE'
Usage:
  bash scripts/vps-migrate.sh [--target local|supabase] [--dry-run|--apply]

Default target is the PostgreSQL server on this VPS (127.0.0.1:5432).
Supabase remote is opt-in: --target supabase.

Local PostgreSQL environment:
  PGHOST=127.0.0.1       PGPORT=5432
  PGDATABASE=nafaz       PGUSER=postgres
  PGPASSWORD=...         (prefer .pgpass / protected secret injection)
  MIGRATION_BACKUP_DIR=/var/backups/nafaz-db-migrations

Supabase remote environment:
  EXPECTED_SUPABASE_PROJECT_REF=<project-ref>
  DATABASE_URL=<direct PostgreSQL URL used only for pre-migration backup>
  BACKUP_DATABASE_PROJECT_REF=<same project-ref, explicit confirmation>

Dry-run is the default. --apply creates and verifies a full backup first.
USAGE
}

while (($#)); do
  case "$1" in
    --target)
      (($# >= 2)) || { echo "--target requires local or supabase" >&2; exit 2; }
      TARGET="$2"; shift ;;
    --target=*) TARGET="${1#*=}" ;;
    --dry-run) MODE=dry-run ;;
    --apply) MODE=apply ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

die(){ echo "ERROR: $*" >&2; exit 1; }
log(){ printf '\n[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }
[[ "$TARGET" == local || "$TARGET" == supabase ]] || die "Target must be local or supabase."
[[ -d supabase/migrations ]] || die "supabase/migrations directory missing."
compgen -G 'supabase/migrations/*.sql' >/dev/null || die "No SQL migrations found."
command -v flock >/dev/null || die "flock is required."

declare -A versions=()
for file in supabase/migrations/*.sql; do
  base="$(basename "$file")"
  version="${base%%_*}"
  [[ "$version" =~ ^[0-9]{14}$ ]] || die "Invalid migration version: $base"
  [[ -z "${versions[$version]:-}" ]] || die "Duplicate migration version: $version"
  versions[$version]=1
done

LOCK_KEY="local"
if [[ "$TARGET" == local ]]; then
  PGHOST="${PGHOST:-127.0.0.1}"
  PGPORT="${PGPORT:-5432}"
  PGDATABASE="${PGDATABASE:-nafaz}"
  PGUSER="${PGUSER:-postgres}"
  export PGHOST PGPORT PGDATABASE PGUSER
  LOCK_KEY="$PGHOST-$PGPORT-$PGDATABASE"
else
  command -v supabase >/dev/null || die "Supabase CLI missing."
  [[ -f supabase/config.toml && -f supabase/.temp/project-ref ]] || die "Supabase project is not linked."
  PROJECT_REF="$(tr -d '[:space:]' < supabase/.temp/project-ref)"
  [[ -n "$PROJECT_REF" ]] || die "Linked project ref is empty."
  [[ -z "$EXPECTED_REF" || "$PROJECT_REF" == "$EXPECTED_REF" ]] || die "Project ref mismatch."
  LOCK_KEY="$PROJECT_REF"
fi

exec 9>"${TMPDIR:-/tmp}/nafaz-migration-$LOCK_KEY.lock"
flock -n 9 || die "Another migration runner is active for $LOCK_KEY."

if [[ "$TARGET" == local ]]; then
  command -v psql >/dev/null || die "psql missing."
  command -v pg_isready >/dev/null || die "pg_isready missing."
  log "Checking local PostgreSQL at $PGHOST:$PGPORT database=$PGDATABASE user=$PGUSER"
  pg_isready -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" >/dev/null || die "Local PostgreSQL is not accepting connections. No remote fallback is attempted."
  psql -X -v ON_ERROR_STOP=1 -Atqc 'select current_database(), current_user, inet_server_addr(), inet_server_port()' |
    while IFS='|' read -r db usr host port; do
      log "Connected database=$db user=$usr server_addr=${host:-local-socket} port=${port:-local}"
    done

  # The migration set grants backend access to Supabase-compatible roles.
  # Do not silently create or weaken database roles on an existing VPS.
  for role in anon authenticated service_role; do
    exists="$(psql -X -v ON_ERROR_STOP=1 -Atqc "select count(*) from pg_roles where rolname='$role'")"
    [[ "$exists" == "1" ]] || die "Required PostgreSQL role '$role' is missing. Create/review it explicitly before applying these migrations."
  done

  ledger_exists="$(psql -X -v ON_ERROR_STOP=1 -Atqc "select to_regclass('public.nafaz_schema_migrations') is not null")"
  if [[ "$ledger_exists" != "t" ]]; then
    legacy_tables="$(psql -X -v ON_ERROR_STOP=1 -Atqc "select count(*) from pg_tables where schemaname='public' and tablename in ('positions','orders','scanner_opportunities','executor_global_control')")"
    [[ "$legacy_tables" == "0" ]] || die "Existing NAFAZ tables found but migration ledger is absent. Refusing to replay baseline automatically; inspect and baseline-adopt the current schema first."
  fi

  pending=()
  for file in supabase/migrations/*.sql; do
    base="$(basename "$file")"
    version="${base%%_*}"
    name="${base#${version}_}"
    checksum="$(sha256sum "$file" | awk '{print $1}')"
    row="$(psql -X -v ON_ERROR_STOP=1 -Atqc "select sha256 from public.nafaz_schema_migrations where version='$version'")"
    if [[ -n "$row" ]]; then
      [[ "$row" == "$checksum" ]] || die "Applied migration $version has changed checksum. Stop and investigate; no migration applied."
      log "Already applied: $base"
    else
      pending+=("$file")
      log "Pending: $base"
    fi
  done

  if (("${#pending[@]}" == 0)); then
    log "Local database is already up to date."
    exit 0
  fi
  if [[ "$MODE" == dry-run ]]; then
    log "DRY-RUN: ${#pending[@]} migration(s) would be applied to local PostgreSQL. No schema changes made."
    exit 0
  fi

  command -v pg_dump >/dev/null || die "pg_dump missing."
  command -v pg_restore >/dev/null || die "pg_restore missing."
  umask 077
  mkdir -p "$BACKUP_DIR"; chmod 700 "$BACKUP_DIR"
  stamp="$(date -u +'%Y%m%dT%H%M%SZ')"
  backup="$BACKUP_DIR/nafaz-local-$PGHOST-$PGPORT-$PGDATABASE-$stamp.dump"
  tmp_backup="$backup.partial"
  tmp_sql="$(mktemp "${TMPDIR:-/tmp}/nafaz-migration.XXXXXX.sql")"
  cleanup(){ rm -f "$tmp_sql" "$tmp_backup"; }
  trap cleanup EXIT

  log "Creating full backup of local PostgreSQL before migration."
  pg_dump --format=custom --no-owner --no-acl --file="$tmp_backup"
  [[ -s "$tmp_backup" ]] || die "Backup is empty."
  pg_restore --list "$tmp_backup" >/dev/null || die "Backup archive verification failed."
  mv "$tmp_backup" "$backup"; chmod 600 "$backup"
  log "Verified backup: $backup"

  psql -X -v ON_ERROR_STOP=1 -c "
    create table if not exists public.nafaz_schema_migrations (
      version text primary key,
      name text not null,
      sha256 text not null,
      applied_at timestamptz not null default now()
    );" >/dev/null

  for file in "${pending[@]}"; do
    base="$(basename "$file")"
    version="${base%%_*}"
    name="${base#${version}_}"
    checksum="$(sha256sum "$file" | awk '{print $1}')"
    log "Applying locally: $base"
    {
      cat "$file"
      printf '\nINSERT INTO public.nafaz_schema_migrations(version,name,sha256) VALUES (%s,%s,%s);\n' \
        "'$version'" "'${name%.sql}'" "'$checksum'"
    } > "$tmp_sql"
    # Schema change and history record commit atomically for each migration.
    psql -X -v ON_ERROR_STOP=1 --single-transaction --file="$tmp_sql"
    : > "$tmp_sql"
  done

  log "Verifying local migration ledger."
  psql -X -v ON_ERROR_STOP=1 -c "select version,name,applied_at from public.nafaz_schema_migrations order by version;"
  log "Local PostgreSQL migrations completed. No services were restarted."
  exit 0
fi

# Explicit Supabase remote mode; never selected automatically.
log "Target Supabase remote project: $PROJECT_REF"
supabase migration list --remote
supabase db push --remote --dry-run
if [[ "$MODE" == dry-run ]]; then
  log "DRY-RUN complete; no remote database changes made."
  exit 0
fi
DB_URL="${DATABASE_URL:-${POSTGRES_DSN:-}}"
[[ -n "$DB_URL" ]] || die "Supabase --apply requires DATABASE_URL for a full backup."
[[ "${BACKUP_DATABASE_PROJECT_REF:-}" == "$PROJECT_REF" ]] || die "Set BACKUP_DATABASE_PROJECT_REF after verifying the backup DSN targets this project."
command -v pg_dump >/dev/null || die "pg_dump missing."
command -v pg_restore >/dev/null || die "pg_restore missing."
umask 077
mkdir -p "$BACKUP_DIR"; chmod 700 "$BACKUP_DIR"
stamp="$(date -u +'%Y%m%dT%H%M%SZ')"
backup="$BACKUP_DIR/nafaz-$PROJECT_REF-$stamp.dump"
tmp_backup="$backup.partial"
cleanup(){ [[ ! -e "$tmp_backup" ]] || rm -f "$tmp_backup"; }
trap cleanup EXIT
pg_dump --format=custom --no-owner --no-acl --file="$tmp_backup" "$DB_URL"
[[ -s "$tmp_backup" ]] || die "Backup is empty."
pg_restore --list "$tmp_backup" >/dev/null || die "Backup archive verification failed."
mv "$tmp_backup" "$backup"; chmod 600 "$backup"
log "Verified backup: $backup"
supabase db push --remote --yes
supabase migration list --remote
log "Supabase remote migrations completed. No services were restarted."
