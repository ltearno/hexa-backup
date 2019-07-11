build-image:
	docker build . --build-arg UID=$(shell id -u) --build-arg GID=$(shell id -g) -t hexa-backup:latest

run-image:
	docker run -it --rm -p 5005:5005 -v /tmp/hexa-backup-store:/hexa-backup/store hexa-backup:latest

#--name hexa-backup