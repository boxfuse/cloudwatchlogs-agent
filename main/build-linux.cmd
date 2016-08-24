@Echo off

setlocal

SET GOOS=linux
SET GOARCH=amd64
go install -ldflags "-s"