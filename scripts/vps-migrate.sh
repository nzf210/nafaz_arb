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
      NAFAZ_MIGRATION_TARGET|NAFAZ_PG_CONTAINER|MIGRATION_BACKUP_DIR|EXPECTED_SUPABASE_PROJECT_REF|PGHOST|PGPORT|PGDATABASE|PGUSER|PGPASSWORD|DATABASE_URL|POSTGRES_DSN|BACKUP_DATABASE_PROJECT_REF) ;;
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

Local PostgreSQL container environment:
  NAFAZ_PG_CONTAINER=wch-stg-postgres
  PGDATABASE=nafaz       PGUSER=<POSTGRES_USER inside container>
  MIGRATION_BACKUP_DIR=/var/backups/nafaz-db-migrations

Local mode runs psql, pg_dump, pg_restore, and pg_isready inside Docker.
No PostgreSQL client binaries are required on the VPS host.

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
  PG_CONTAINER="${NAFAZ_PG_CONTAINER:-wch-stg-postgres}"
  PGDATABASE="${PGDATABASE:-nafaz}"
  command -v docker >/dev/null || die "docker CLI missing; local migrations require Docker access."
  docker inspect "$PG_CONTAINER" >/dev/null 2>&1 || die "PostgreSQL container '$PG_CONTAINER' not found or inaccessible."
  running="$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null || true)"
  [[ "$running" == "true" ]] || die "PostgreSQL container '$PG_CONTAINER' is not running."
  if [[ -z "${PGUSER:-}" ]]; then
    PGUSER="$(docker exec "$PG_CONTAINER" sh -lc 'printf "%s" "${POSTGRES_USER:-postgres}"')"
  fi
  [[ -n "$PGUSER" ]] || die "Could not determine PostgreSQL user; set PGUSER explicitly."
  export PGDATABASE PGUSER
  LOCK_KEY="$(printf '%s' "$PG_CONTAINER-$PGDATABASE" | sha256sum | awk '{print $1}')"
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
  # PostgreSQL client tools execute inside the selected Docker container.
  # Migration SQL and backup archives remain on the VPS host.
  db_exec() { docker exec -i "$PG_CONTAINER" "$@"; }
  db_psql() { db_exec psql -X -U "$PGUSER" -d "$PGDATABASE" -v ON_ERROR_STOP=1 "$@"; }

  log "Checking Docker PostgreSQL container=$PG_CONTAINER database=$PGDATABASE user=$PGUSER"
  db_exec pg_isready -h /var/run/postgresql -p 5432 -d "$PGDATABASE" -U "$PGUSER" >/dev/null ||
    die "PostgreSQL in container '$PG_CONTAINER' is not accepting connections."

  identity="$(db_psql -Atqc 'select current_database(), current_user, inet_server_addr(), inet_server_port()')"
  IFS='|' read -r connected_db connected_user server_addr server_port <<< "$identity"
  [[ "$connected_db" == "$PGDATABASE" ]] || die "Connected database mismatch: expected '$PGDATABASE', got '$connected_db'."
  [[ "$connected_user" == "$PGUSER" ]] || die "Connected user mismatch: expected '$PGUSER', got '$connected_user'."
  log "Connected database=$connected_db user=$connected_user server_addr=${server_addr:-container-local} port=${server_port:-5432}"

  # Required application roles are checked, never created or weakened here.
  for role in anon authenticated service_role; do
    exists="$(db_psql -Atqc "select count(*) from pg_roles where rolname='$role'")"
    [[ "$exists" == "1" ]] || die "Required PostgreSQL role '$role' is missing. Review/create it explicitly before applying migrations."
  done

  ledger_exists="$(db_psql -Atqc "select to_regclass('public.nafaz_schema_migrations') is not null")"
  fresh_database=false
  if [[ "$ledger_exists" != "t" ]]; then
    existing_public_tables="$(db_psql -Atqc "select count(*) from pg_tables where schemaname='public'")"
    [[ "$existing_public_tables" == "0" ]] ||
      die "Existing public tables found but migration ledger is absent. Refusing to replay baseline automatically; inspect and baseline-adopt the existing schema first."
    fresh_database=true
    log "No migration ledger or public tables found; treating database as a fresh installation."
  fi

  pending=()
  for file in supabase/migrations/*.sql; do
    base="$(basename "$file")"
    version="${base%%_*}"
    name="${base#${version}_}"
    checksum="$(sha256sum "$file" | awk '{print $1}')"
    row=""
    if [[ "$fresh_database" == false ]]; then
      row="$(db_psql -Atqc "select sha256 from public.nafaz_schema_migrations where version='$version'")"
    fi
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
    log "DRY-RUN: ${#pending[@]} migration(s) would be applied to Docker PostgreSQL. No schema changes made."
    exit 0
  fi

  umask 077
  mkdir -p "$BACKUP_DIR"; chmod 700 "$BACKUP_DIR"
  stamp="$(date -u +'%Y%m%dT%H%M%SZ')"
  backup_label="$(printf '%s-%s' "$PG_CONTAINER" "$PGDATABASE" | tr -c 'A-Za-z0-9_.-' '_')"
  backup="$BACKUP_DIR/nafaz-$backup_label-$stamp.dump"
  tmp_backup="$backup.partial"
  tmp_sql="$(mktemp "${TMPDIR:-/tmp}/nafaz-migration.XXXXXX.sql")"
  container_backup="/tmp/$(basename "$tmp_backup")"
  cleanup(){
    rm -f "$tmp_sql" "$tmp_backup"
    docker exec "$PG_CONTAINER" rm -f "$container_backup" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT

  log "Creating full backup of database=$PGDATABASE from container=$PG_CONTAINER."
  db_exec pg_dump -U "$PGUSER" -d "$PGDATABASE" --format=custom --no-owner --no-acl > "$tmp_backup"
  [[ -s "$tmp_backup" ]] || die "Backup is empty."
  docker cp "$tmp_backup" "$PG_CONTAINER:$container_backup" >/dev/null ||
    die "Could not copy backup archive into PostgreSQL container for verification."
  docker exec "$PG_CONTAINER" pg_restore --list "$container_backup" >/dev/null ||
    die "Backup archive verification failed."
  docker exec "$PG_CONTAINER" rm -f "$container_backup"
  mv "$tmp_backup" "$backup"; chmod 600 "$backup"
  log "Verified backup on VPS host: $backup"

  db_psql -c "
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
    db_psql --single-transaction --file=- < "$tmp_sql"
    : > "$tmp_sql"
  done

  log "Verifying migration ledger in database=$PGDATABASE."
  db_psql -c "select version,name,applied_at from public.nafaz_schema_migrations order by version;"
  log "Docker PostgreSQL migrations completed. No services were restarted."
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
