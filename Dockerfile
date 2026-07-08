FROM golang:alpine AS build
ARG VERSION=dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags "-X main.version=${VERSION}" -o wavelog_worker .

FROM alpine:3.20
RUN adduser -D -u 1000 worker
WORKDIR /app
COPY --from=build /app/wavelog_worker .
USER worker
EXPOSE 9000 9001
CMD ["./wavelog_worker", "--config", "config.yaml"]
