# Multi-stage: compila estático y corre en distroless. El host NO necesita Go.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/onix-gateway ./cmd/onix-gateway
# Prepara el dir de uploads con dueño nonroot (65532): al inicializar el volumen
# Docker copia estos permisos, para que el gateway (distroless nonroot) pueda escribir.
RUN mkdir -p /data/uploads && chown -R 65532:65532 /data

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/onix-gateway /onix-gateway
COPY --from=build --chown=65532:65532 /data /data
EXPOSE 8080
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/onix-gateway", "-healthcheck"]
ENTRYPOINT ["/onix-gateway"]
