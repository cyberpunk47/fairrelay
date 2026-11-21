FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG APP
RUN CGO_ENABLED=0 go build -o /out/app ./cmd/${APP}

FROM alpine:3.20
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]