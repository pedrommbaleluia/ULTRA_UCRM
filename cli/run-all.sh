#!/bin/bash
# UCRM combined cron runner

# Set working dir to project root
cd /var/www/ucrm.realtransfer.pt/cli || exit 1

# Recreate full environment (cron has none)
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export HOME=/var/www
export TZ=UTC
export GOOGLE_APPLICATION_CREDENTIALS=/etc/ucrm/fcm-service-account.json

# Optional: log to central file
LOGDIR=/var/log/ucrm
mkdir -p "$LOGDIR"

# Run scheduler first
runuser -u www-data -- /usr/bin/php scheduler.php --debug >>"$LOGDIR/cron-scheduler.log" 2>&1

# Then worker
runuser -u www-data -- /usr/bin/php worker.php --debug >>"$LOGDIR/cron-worker.log" 2>&1

