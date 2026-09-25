Backfill job (engineered) - build & deploy

This documents how to build the backfill container, push it, and run as a Kubernetes CronJob or manual Job.

1) Build & push image (example using Docker Hub)

```bash
# from repo root
docker build -f cmd/migrate/Dockerfile -t your-registry/cex-backfill:latest .
docker push your-registry/cex-backfill:latest
```

2) Create k8s Secret for DB DSN

```bash
kubectl create secret generic db-credentials --from-literal=pg_dsn="postgres://user:pass@host:5432/db?sslmode=disable" -n default
```

3) Apply manifests

```bash
kubectl apply -f k8s/backfill-cronjob.yaml
# For manual run
kubectl apply -f k8s/backfill-job-manual.yaml
```

4) Notes & best practices
- Use `concurrencyPolicy: Forbid` to avoid overlapping runs.
- The program uses Postgres advisory lock (default key `123456789`) to avoid concurrent runs even if multiple replicas exist.
- The program persists a checkpoint row into `backfill_checkpoints` (configurable via `-checkpoint-table`) so interrupted runs resume.
- Always start with `-dry-run=true` in staging, then switch to `-dry-run=false` in production after validation.
- Use resource limits and `backoffLimit` to control retries.
