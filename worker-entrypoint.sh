#!/bin/sh
exec celery -A app.tasks.celery_app worker --loglevel=info --concurrency=${CELERY_CONCURRENCY:-1}
