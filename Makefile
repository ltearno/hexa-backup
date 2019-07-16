.PHONY: build-image run-image

build-image:
	docker build . --build-arg UID=$(shell id -u) --build-arg GID=$(shell id -g) -t hexa-backup:latest

run-image:
	docker run -it --rm --name hexa-backup -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest

run-image-as-daemon:
	docker run -d --restart=always --name hexa-backup -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest

gen-certs:
	openssl req -new -x509 -sha256 -newkey rsa:2048 -nodes -keyout key.pem -days 365 -out cert.pem