
## Installation

Install with

```bash
$ docker-compose up --build
```

## Usage

To start

```bash
$ docker exec -it golang_app bash
$ go run main.go
```


To check bd

```bash
$ docker exec -it golang_bd bash
$ mysql -h 127.0.0.1 -P 3306 -u docker -p

Pass: docker

$ use test_db;
$ select * from ticks;

```


