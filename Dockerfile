FROM node:19 AS builder

WORKDIR /hexa-backup

RUN wget https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp_linux
RUN chmod +x yt-dlp_linux

ADD package.json ./
ADD tsconfig.json ./

RUN npm install

ADD src ./src
RUN npm run build || echo "yes"

FROM node:19

RUN apt update && apt install -y ffmpeg

ARG UID=1000
ARG GID=1000

RUN groupadd --gid ${GID} hexa-backup-group || echo "group ${GID} already exists"
RUN useradd --uid ${UID} --gid ${GID} hexa-backup-user || echo "user ${UID} already exists"

USER ${UID}

ADD --chown=1000:1000 server.crt /hexa-backup/server.crt
ADD --chown=1000:1000 server.key /hexa-backup/server.key
ADD --chown=1000:1000 static /hexa-backup/static

COPY --from=builder --chown=1000:1000 /hexa-backup/target /hexa-backup/target
COPY --from=builder --chown=1000:1000 /hexa-backup/node_modules/ /hexa-backup/node_modules/
COPY --from=builder --chown=1000:1000 /hexa-backup/yt-dlp_linux /usr/local/bin/yt-dlp

WORKDIR /hexa-backup

EXPOSE 5005

ENTRYPOINT ["node", "target/cli/hexa-backup.js", "store", "-storeDirectory", "/hexa-backup/store"]

# docker run -it --rm --name hexa-backup -p 5005:5005 -v $(pwd)/tmp/store:/hexa-backup/store hexa-backup:latest
