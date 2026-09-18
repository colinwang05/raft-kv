# Builds cmd/server (the only binary that needs to run inside a
# container — raftctl/raft-bench are meant to run on the host against
# the ports docker-compose.yml exposes; see README's "## Docker Compose
# demo"). Generated protobuf code is gitignored (see proto/raft.proto's
# comment), so the builder stage regenerates it from source rather than
# assuming it's present in the build context.
FROM golang:1.22-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

# Pin these to the versions README.md's "Generate protobuf/gRPC code"
# section installs, so a container build and a local `go build` produce
# equivalent generated code.
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2 \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
ENV PATH="/root/go/bin:${PATH}"

COPY . .
RUN protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/raft.proto

RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=builder /out/server /server
ENTRYPOINT ["/server"]
