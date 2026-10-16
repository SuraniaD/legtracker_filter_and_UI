#!/bin/sh
# One image, several Railway services: the JOB variable picks what runs.
#   JOB=refresh (default)  daily ingestion refresh       (refresh.py)
#   JOB=filter             tracker filter, gates 1-3      (filter_job.py)
# Extra arguments go in JOB_ARGS, e.g. JOB_ARGS="--dry-run".
# Preflight runs first; the job only starts if it passes. `exec` makes the job
# the container's main process so Railway's SIGTERM reaches it.
set -e
case "${JOB:-refresh}" in
  refresh) python refresh.py --preflight;    exec python refresh.py ${JOB_ARGS} ;;
  filter)  python filter_job.py --preflight ${JOB_ARGS}; exec python filter_job.py ${JOB_ARGS} ;;
  *) echo "Unknown JOB='${JOB}'. Use refresh or filter."; exit 1 ;;
esac
