FROM --platform=linux/amd64 oven/bun:1.3.13-slim AS packer
WORKDIR /w
ADD https://raw.githubusercontent.com/zanfranceschi/rinha-de-backend-2026/main/resources/references.json.gz /w/references.json.gz
COPY scripts/pack-references.ts /w/scripts/pack-references.ts
RUN bun /w/scripts/pack-references.ts

FROM --platform=linux/amd64 oven/bun:1.3.13-slim
WORKDIR /app
COPY package.json ./
RUN bun install
COPY src/server.ts ./src/
COPY --from=packer /w/references.bin /opt/references.bin
ENV REFERENCES_BIN=/opt/references.bin
ENV DATA_PATH=/data
ENV LISTEN=/tmp/api.sock
EXPOSE 9999
ENTRYPOINT ["bun", "run", "./src/server.ts"]
