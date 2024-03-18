FROM golang:1.21

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY configs ./configs

RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc -o mc

RUN chmod +x ./mc

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /minio_api

EXPOSE 8000

CMD ["/minio_api"]
