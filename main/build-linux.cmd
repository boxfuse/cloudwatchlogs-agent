@Echo off

setlocal

SET GOOS=linux
SET GOARCH=amd64
go build -a -ldflags "-s"
mv main cloudwatchlogs-agent