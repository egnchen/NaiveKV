FROM golang:1.14-alpine as builder
WORKDIR /usr/src/app
ENV GOPROXY=https://goproxy.cn
ADD . /usr/src/app
RUN go build -ldflags "-s -w" -o master cmd/master/main.go

FROM scratch as runner
COPY --from=builder /usr/src/app/master /opt/app/
CMD ["/opt/app/server"]

