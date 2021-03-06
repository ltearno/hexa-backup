.PHONY: build-image run-image

build-image:
	docker build . --build-arg UID=$(shell id -u) --build-arg GID=$(shell id -g) -t hexa-backup:latest

run-image:
	docker run -it --rm --name hexa-backup -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest

run-image-as-daemon:
	docker run -d --restart=always --name hexa-backup -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest

stop-and-remove-daemon:
	docker stop hexa-backup
	docker rm hexa-backup

run-image-as-daemon-for-xps15:
	docker run --name hexa-backup -d --restart=always \
		--network=host \
		-v /media/arnaud/0a2b2256-7384-4a26-be2f-59e291a975f85/hexa-backup:/hexa-backup/store \
		hexa-backup:latest

run-image-as-daemon-for-xps15-with-database:
	docker run --name hexa-backup -d --restart=always \
		--network=host \
		-v /media/arnaud/0a2b2256-7384-4a26-be2f-59e291a975f85/hexa-backup:/hexa-backup/store \
		hexa-backup:latest \
		-database postgres \
		-databaseHost localhost \
		-databasePort 5432 \
		-databaseUser postgres \
		-databasePassword hexa-backup

run-image-as-sync-agent-for-xps15:
	docker run --name hexa-backup -d --restart=always \
		-v /media/arnaud/0a2b2256-7384-4a26-be2f-59e291a975f85/hexa-backup:/hexa-backup/store \
		hexa-backup:latest

run-postresql:
	docker run -d --restart=always --name postgresql-hexa-backup -p 5432:5432 -e POSTGRES_PASSWORD=hexa-backup postgres

gen-certs:
	openssl req -new -x509 -sha256 -newkey rsa:2048 -nodes -keyout server.key -days 365 -out server.crt