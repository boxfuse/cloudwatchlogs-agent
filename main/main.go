package main

import (
	"flag"
	"io"
	"log"
	"os"
	"time"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"strings"
	".."
)

var (
	stderr = flag.Bool("stderr", false, "true if this logs messages from stderr instead of stdout")
	x = flag.Bool("X", false, "show internal logs")
)

func init() {
	flag.Parse()
}

func main() {
	version := "1.0.2"

	instance, _ := os.Hostname()

	envVar := os.Getenv("BOXFUSE_ENV")
	if envVar == "" {
		log.Fatal("Missing BOXFUSE_ENV environment variable")
	}
	env := "boxfuse/" + envVar

	image := os.Getenv("BOXFUSE_IMAGE_COORDINATES")
	if image == "" {
		log.Fatal("Missing BOXFUSE_IMAGE_COORDINATES environment variable")
	}

	app := strings.Split(image, ":")[0]

	endpoint := os.Getenv("BOXFUSE_CLOUDWATCHLOGS_ENDPOINT")
	endpointMsg := "";
	var awsSession *session.Session
	if endpoint != "" {
		endpointMsg = " at " + endpoint;
		awsSession = session.New(&aws.Config{Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials("dummy", "dummy", "")})
	} else {
		awsSession = session.New()
	}

	level := "INFO"
	if *stderr {
		level = "ERROR"
	}

	log.Println("Boxfuse CloudWatch Logs Agent " + version + " redirecting " + level + " logs for " + image + " to CloudWatch Logs" + endpointMsg + " (group: " + env + ", stream: " + app + ") ...")

	logger1, err := logger.NewLogger(awsSession, endpoint, env, app, level, time.Second, image, instance, x)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := io.Copy(logger1, os.Stdin); err != nil {
		log.Println("copy err", err)
	}
	if err := logger1.Close(); err != nil {
		log.Println(err)
	}
	log.Println("Exiting...")
	os.Exit(0)
}
