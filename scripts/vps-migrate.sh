#!/usr/bin/env bash
set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
MODE=dry-run
BACKUP_DIR="${MIGRATION_BACKUP_DIR:-/var/backups/nafaz-db-migrations}"
EXPECTED_REF="${EXPECTED_SUPABASE_PROJECT_REF:-}"
usage(){ echo "Usage: bash scripts/vps-migrate.sh [--dry-run|--apply]"; }
while (($#)); do
  case "$1" in
    --dry-run) MODE=dry-run ;;
    --apply) MODE=apply ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done
die(){ echo "ERROR: $*" >&2; exit 1; }
log(){ printf '\n[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"; }
command -v supabase >/dev/null || die "Supabase CLI missing."
command -v flock >/dev/null || die "flock missing."
[[ -d supabase/migrations && -f supabase/config.toml ]] || die "Supabase project files missing."
compgen -G 'supabase/migrations/*.sql' >/dev/null || die "No SQL migrations found."
[[ -f supabase/.temp/project-ref ]] || die "Project not linked; run supabase link --project-ref <ref>."
PROJECT_REF="$(tr -d '[:space:]' < supabase/.temp/project-ref)"
[[ -n "$PROJECT_REF" ]] || die "Linked project ref is empty."
[[ -z "$EXPECTED_REF" || "$PROJECT_REF" == "$EXPECTED_REF" ]] || die "Project ref mismatch: linked=$PROJECT_REF expected=$EXPECTED_REF."
declare -A versions=()
for file in supabase/migrations/*.sql; do
  base="$(basename "$file")"; version="${base%%_*}"
  [[ "$version" =~ ^[0-9]{14}$ ]] || die "Invalid migration version: $base"
  [[ -z "${versions[$version]:-}" ]] || die "Duplicate migration version: $version"
  versions[$version]=1
done
exec 9>"${TMPDIR:-/tmp}/nafaz-supabase-migration-$PROJECT_REF.lock"
flock -n 9 || die "Another migration runner is active for $PROJECT_REF."
log "Target project: $PROJECT_REF; local migrations: ${#versions[@]}"
log "Inspecting remote migration history."
supabase migration list --remote
log "Calculating pending plan (dry-run)."
supabase db push --remote --dry-run
if [[ "$MODE" == dry-run ]]; then
  log "DRY-RUN complete; no database changes made."
  exit 0
fi
DB_URL="${DATABASE_URL:-${POSTGRES_DSN:-}}"
[[ -n "$DB_URL" ]] || die "--apply requires DATABASE_URL or POSTGRES_DSN for full backup."
[[ "${BACKUP_DATABASE_PROJECT_REF:-}" == "$PROJECT_REF" ]] || die "Set BACKUP_DATABASE_PROJECT_REF to the linked project ref after verifying that the backup DSN targets that exact project."
command -v pg_dump >/dev/null || die "pg_dump missing."
command -v pg_restore >/dev/null || die "pg_restore missing."
umask 077
mkdir -p "$BACKUP_DIR"; chmod 700 "$BACKUP_DIR"
STAMP="$(date -u +'%Y%m%dT%H%M%SZ')"
BACKUP="$BACKUP_DIR/nafaz-$PROJECT_REF-$STAMP.dump"
TMP="$BACKUP.partial"
cleanup(){ [[ ! -e "$TMP" ]] || rm -f "$TMP"; }
trap cleanup EXIT
log "Creating full PostgreSQL backup."
pg_dump --format=custom --no-owner --no-acl --file="$TMP" "$DB_URL"
[[ -s "$TMP" ]] || die "Backup is empty."
pg_restore --list "$TMP" >/dev/null || die "Backup archive verification failed."
mv "$TMP" "$BACKUP"; chmod 600 "$BACKUP"
log "Verified backup: $BACKUP"
log "Applying migrations to $PROJECT_REF."
supabase db push --remote --yes
log "Verifying remote migration history."
supabase migration list --remote
log "Migration command completed. No app/container restart or deployment performed."
