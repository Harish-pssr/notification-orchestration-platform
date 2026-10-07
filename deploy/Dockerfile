# One Dockerfile for every binary (D2b): docker build --build-arg SERVICE=relay .
FROM golang:1.27-alpine AS build
ARG SERVICE
WORKDIR /src

# Copy the dependency files first so this layer is cached until they change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off + static link = a binary that runs in an image with no libc at all.
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
