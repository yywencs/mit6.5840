#!/bin/bash

rm ../wc.so
go build -race -buildmode=plugin ../../mrapps/wc.go
mv wc.so ..
go run -race ../mrworker.go ../wc.so
