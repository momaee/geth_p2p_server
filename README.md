<!-- go build -ldflags="-extldflags=-static" -o main main.go
go build -ldflags "-linkmode external -extldflags '-static' -s -w" main.go
go build -ldflags="-extldflags=-static" -tags netgo,osusergo -o main main.go
go build -ldflags "-linkmode external -extldflags '-static' -s -w" -tags netgo,osusergo main.go

CC=/usr/local/musl/bin/musl-gcc go build --ldflags  '-extldflags "-static"' main.go
CC=/usr/local/musl/bin/musl-gcc go build -ldflags "-linkmode external -extldflags '-static' -s -w" main.go
CC=/usr/local/musl/bin/musl-gcc go build -ldflags "-linkmode external -extldflags '-static' -s -w" -tags netgo,osusergo main.go -->


go build -ldflags "-linkmode external -extldflags '-static' -s -w" -tags netgo,osusergo -o geth-p2p geth.go main.go
