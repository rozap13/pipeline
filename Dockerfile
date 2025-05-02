# Stage 1: Build
FROM golang:1.21 as builder
WORKDIR /app
COPY . .
RUN go build -o pipeline .

# Stage 2: Run
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/pipeline .
CMD ["./pipeline"]
