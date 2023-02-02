FROM golang:1.15.3-alpine3.12 AS builder
WORKDIR /
COPY . .

RUN apk add --update git gcc musl-dev && apk upgrade --available
RUN CGO_ENABLED=1 go build -tags musl -installsuffix cgo -o /gravity-adapter-kafka ./cmd/gravity-adapter-kafka/gravity-adapter-kafka.go

FROM alpine:3.15.6
RUN apk update && apk upgrade --available
COPY --from=builder /gravity-adapter-kafka /gravity-adapter-kafka
COPY ./configs /configs
COPY ./settings /settings
COPY ./build/docker/startup.sh /startup.sh

RUN chmod 700 -R /settings && chown -R 1001  /settings
USER 1001

CMD ["sh", "/startup.sh"]
