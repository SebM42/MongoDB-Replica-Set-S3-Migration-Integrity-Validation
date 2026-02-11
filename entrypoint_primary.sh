#!/bin/bash
set -e

MONGO_DATA=/data/db

if [ ! -d "$MONGO_DATA" ] || [ -z "$(ls -A $MONGO_DATA)" ]; then
  echo ">>> MongoDB data directory is empty, running full initialization"

	echo ">>> 1. Start MongoDB in standalone mode"
	mongod --dbpath $MONGO_DATA --bind_ip_all --fork --logpath /var/log/mongod.log

	echo ">>> 2. Wait for MongoDB to be ready"
	until mongosh --quiet --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
	  sleep 2
	done

	echo ">>> 3. Create admin user"
	mongosh <<EOF
use admin
db.createUser({user: "${MONGO_INITDB_ROOT_USERNAME}", pwd: "${MONGO_INITDB_ROOT_PASSWORD}", roles:["root"]})
EOF

	echo ">>> 4. Run migration script"
	python3 /migration.py \
		--aws-access-key $AWS_ACCESS_KEY_ID\
		--aws-secret-key $AWS_SECRET_ACCESS_KEY\
		--region $AWS_REGION\
		--bucket $BUCKET \
		--prefix $PREFIX \
		--mongo-uri "mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}@localhost:27017" \
		--mongo-db weather_records \

	echo ">>> 5. Stop MongoDB standalone"
	mongod --dbpath $MONGO_DATA --shutdown

	echo ">>> 6. Restart MongoDB with replicaSet + keyFile"
	mongod --dbpath $MONGO_DATA --replSet rs0 --keyFile /etc/mongo-keyfile --auth --bind_ip_all --fork --logpath /var/log/mongod-repl.log

	echo ">>> 7. Wait for replica mode to be ready"
	until mongosh -u "${MONGO_INITDB_ROOT_USERNAME}" -p "${MONGO_INITDB_ROOT_PASSWORD}" --authenticationDatabase admin --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
	  sleep 2
	done

	echo ">>> 8. Initiate replica set"
	mongosh -u "${MONGO_INITDB_ROOT_USERNAME}" -p "${MONGO_INITDB_ROOT_PASSWORD}" --authenticationDatabase admin /init-replica.js


else
	echo ">>> MongoDB data directory already exists, skipping initialization"
	echo ">>> 6. Start MongoDB with replicaSet + keyFile"
	mongod --dbpath $MONGO_DATA --replSet rs0 --keyFile /etc/mongo-keyfile --bind_ip_all --fork --logpath /var/log/mongod-repl.log
fi
# Garder le process vivant
tail -f /var/log/mongod-repl.log