.PHONY: build-image run-image

build-image:
	docker build . --build-arg UID=$(shell id -u) --build-arg GID=$(shell id -g) -t hexa-backup:latest

run-image:
	docker run -it --rm --name hexa-backup -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest