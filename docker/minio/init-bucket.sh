set -e

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

if mc ls local/data-lake >/dev/null 2>&1; then
  echo "'data-lake' bucketi artiq mövcuddur."
else
  mc mb local/data-lake
  echo "'data-lake' bucketi yaradildi."
fi
