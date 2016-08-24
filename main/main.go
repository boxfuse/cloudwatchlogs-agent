package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"
	"github.com/aws/aws-sdk-go/aws/session"
	".."
)

var (
	stderr = flag.Bool("stderr", false, "true if this logs messages from stderr instead of stdout")
)

func init() {
	flag.Parse()
}

func main() {
	instance, _ := os.Hostname()

	env := os.Getenv("BOXFUSE_ENV")
	if env == "" {
		log.Fatal("Missing BOXFUSE_ENV environment variable")
	}

	app := os.Getenv("BOXFUSE_APP")
	if app == "" {
		log.Fatal("Missing BOXFUSE_APP environment variable")
	}

	image := os.Getenv("BOXFUSE_IMAGE_COORDINATES")
	if image == "" {
		log.Fatal("Missing BOXFUSE_IMAGE_COORDINATES environment variable")
	}

	endpoint := os.Getenv("BOXFUSE_CLOUDWATCHLOGS_ENDPOINT")
	endpointMsg := "";
	if endpoint != "" {
		endpointMsg = " at " + endpoint;
	}

	level := "INFO"
	if *stderr {
		level = "ERROR"
	}

	log.Println("Redirecting " + level + " logs for " + image + " to CloudWatch Logs" + endpointMsg + " (group: " + env + ", stream: " + app + ") ...")

	logger, err := logger.NewLogger(session.New(nil), endpoint, env, app, level, time.Second, image, instance)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(logger, os.Stdin); err != nil {
		log.Println("copy err", err)
	}
	if err := logger.Close(); err != nil {
		log.Println(err)
	}
	log.Println("Exiting...")
	os.Exit(0)
}
