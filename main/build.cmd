@Echo off

go build -ldflags "-s"
mv main cloudwatchlogs-agent.exe