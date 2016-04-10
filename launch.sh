#!/bin/bash
(cd kvstore  && go run *.go &) && \
(cd database && go run *.go localhost:3001 localhost:3000 &) && \
(cd storage  && go run *.go localhost:3002 localhost:3000 &) && \
(cd master   && go run *.go localhost:3003 localhost:3000 &) && \
(cd frontend && go run *.go localhost:3000 &) && \
(cd worker   && go run *.go localhost:3000 &)

