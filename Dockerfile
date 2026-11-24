# One Dockerfile builds every program in cmd/. Pick which with the APP build arg:
#   server | dispatcher | fakereceiver | bench

# ---- Stage 1: compile ----
FROM golang:1.27.4-alpine AS build
WORKDIR /src

# Download dependencies first, so Docker can cache this layer between builds
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG APP
RUN test -n "$APP" || (echo "build arg APP is required" && false)
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${APP}

# ---- Stage 2: small runtime image ----
FROM alpine:3.20
RUN adduser -D -u 10001 fairrelay && mkdir -p /work && chown fairrelay /work
WORKDIR /work
COPY --from=build /out/app /usr/local/bin/app
USER fairrelay
ENTRYPOINT ["/usr/local/bin/app"]
