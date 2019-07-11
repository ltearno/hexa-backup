FROM node:10 AS builder

WORKDIR /hexa-backup

ADD src ./src
ADD package.json ./
ADD tsconfig.json ./
ADD yarn.lock ./

RUN npm install
RUN ./node_modules/.bin/tsc || echo "yes"

FROM node:10

ADD server.crt /hexa-backup/server.crt
ADD server.key /hexa-backup/server.key
ADD static /hexa-backup/static

COPY --from=builder /hexa-backup/target /hexa-backup/target
COPY --from=builder /hexa-backup/node_modules/ /hexa-backup/node_modules/

WORKDIR /hexa-backup

EXPOSE 5005

CMD ["node", "target/cli/hexa-backup.js", "store", "-storeDirectory", "/hexa-backup/store"]

# docker run -it --rm --name hexa-backup -p 5005:5005 -v $(pwd)/tmp/store:/hexa-backup/store hexa-backup:latest