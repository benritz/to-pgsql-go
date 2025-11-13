# to-pgsql-go - Migrate a database to PostgreSQL

Migrate a MS SQL Server database to PostgreSQL using a script or directly. 

## Build

- Host: `make build` outputs to `dist/to-pgsql`
- Linux Arm64 + Amd64: `make build-linux` outputs `dist/to-pgsql-linux-amd64` and `dist/to-pgsql-linux-arm64`
- Windows Arm64 + Amd64: `make build-windows` outputs `dist/to-pgsql-windows-amd64.exe` and `dist/to-pgsql-windows-arm64.exe`
- Both Linux + Wndows: `make build-all` 

## Usage

to-pgsql -source [source database url] -target [postgreSQL database url]

