# to-pgsql-go
Migrate a database to PostgreSQL

## Usage

to-pgsql -source [source database url] -target [postgreSQL database url]

## Build

- Host build: `make build` (outputs to `dist/to-pgsql`)
- Cross-compile Linux: `make build-linux` (outputs `dist/to-pgsql-linux-amd64` and `dist/to-pgsql-linux-arm64`)
- Cross-compile Windows: `make build-windows` (outputs `dist/to-pgsql-windows-amd64.exe` and `dist/to-pgsql-windows-arm64.exe`)
- Build all: `make build-all` (Linux + Windows targets)

