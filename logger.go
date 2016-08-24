package logger

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

type (
	Logger struct {
		w io.Writer

		// Service exposed for direct actions
		Service *cloudwatchlogs.CloudWatchLogs

		// logging tokens
		group         *string
		stream        *string
		sequenceToken *string

		// internal
		sw   *ScannerWriter
		done chan struct{}
	}

	LogMessage struct {
		Instance *string `json:"instance"`
		Image *string `json:"image"`
		Level *string `json:"level"`
		Message *string `json:"message"`
	}
)

const (
	MaxMessageLength = 32 << 10
)

var (
	// this is how long a batch will continue to be retried, in the event CloudWatch is
	// not available.  At which point the batch is dumped to stderr
	MaxRetryTime = time.Hour

	// the buffer length of the log event channel
	EventLogBufferLength = 64 << 10

	// this occurs when the buffered channel receiving log writes blocks
	ErrStreamBackedUp = errors.New("stream backed up")
)

func NewLogger(sess *session.Session, endpoint, group, stream, level string, flushInterval time.Duration, image, instance string) (*Logger, error) {
	config := aws.NewConfig()
	config.Endpoint = &endpoint
	l := &Logger{
		Service: cloudwatchlogs.New(sess, config),
		group:   &group,
		stream:  &stream,
		done:    make(chan struct{}),
	}

	events := make(chan *cloudwatchlogs.InputLogEvent, EventLogBufferLength)

	go func() {

		flushTime := time.NewTicker(flushInterval)
		defer flushTime.Stop()

		var logEvents []*cloudwatchlogs.InputLogEvent

		for {
			func() {
				defer func() {
					if e := recover(); e != nil {
						fmt.Fprintln(os.Stderr, "panic:", e)
					}
				}()
				select {
				case e := <-events:
					logEvents = append(logEvents, e)
				case <-flushTime.C:
					if len(logEvents) > 0 {
						l.flush(logEvents)
						logEvents = nil
					}
				case <-l.done:
					for {
						select {
						case e := <-events:
							logEvents = append(logEvents, e)
						default:
							l.flush(logEvents)
							l.done <- struct{}{}
							close(l.done)
							runtime.Goexit()
						}
					}
				}
			}()
		}

	}()

	l.sw = NewScannerWriter(bufio.ScanLines, MaxMessageLength, func(token []byte) error {
		message := string(token)

		m := &LogMessage{
			Instance : &instance,
			Image    : &image,
			Level    : &level,
			Message  : &message}

		json, _ := json.Marshal(m)
		s := string(json)

		e := &cloudwatchlogs.InputLogEvent{
			Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
			Message:   aws.String(s),
		}

		select {
		case events <- e:
		default:
			// we're backed up, drop to stderr
			fmt.Fprintf(os.Stderr, "%#v\n", e)
			// this error will never be caught because
			// no one ever checks the return values of log.* calls
			// but return it anyway to be a good citizen
			return ErrStreamBackedUp
		}

		return nil

	})

	return l, nil

}

func eventLength(e *cloudwatchlogs.InputLogEvent) int {
	return len(*e.Message) + 26 // padding per spec
}

func (l *Logger) flush(logEvents []*cloudwatchlogs.InputLogEvent) {

	// The maximum rate of a PutLogEvents request is 5 requests per second per log stream.
	rate := NewRateLimiter(5, time.Second)
	defer rate.Close()

	for len(logEvents) > 0 && rate.Ready() {

		var (
			batchSize int
			batch     []*cloudwatchlogs.InputLogEvent
		)

		// None of the log events in the batch can be more than 2 hours in the future.
		// None of the log events in the batch can be older than 14 days or the retention period of the log group.
		// The log events in the batch must be in chronological ordered by their timestamp.
		const (
			// The maximum batch size is 1,048,576 bytes, and this size is calculated as the sum of all messages in UTF-8, plus 26 bytes for each log entry.
			MaxBatchSize = 1 << 20
			// The maximum number of log events in a batch is 10,000.
			MaxBatchCount = 10000
		)

		for batchSize < MaxBatchSize &&
			len(batch) < MaxBatchCount &&
			len(logEvents) > 0 {
			batch = append(batch, logEvents[0])
			batchSize += eventLength(logEvents[0])
			logEvents = logEvents[1:]
		}

		input := &cloudwatchlogs.PutLogEventsInput{
			LogEvents:     batch,
			LogGroupName:  l.group,
			LogStreamName: l.stream,
			SequenceToken: l.sequenceToken,
		}

		if err := NewTrier(MaxRetryTime).TryFunc(func() (error, bool) {

			//start := time.Now()
			resp, err := l.Service.PutLogEvents(input)
			//fmt.Println("PutLogEvents:", time.Since(start))

			if err != nil {
				if awsErr, ok := err.(awserr.Error); ok {
					switch awsErr.Code() {
					case "DataAlreadyAcceptedException":
						fmt.Fprintln(os.Stderr, "batch already added..")
						return nil, false
					case "ResourceNotFoundException":
						fmt.Fprintln(os.Stderr, "group or stream not found, creating...")
						if _, err := l.Service.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
							LogGroupName: l.group,
						}); err != nil {
							fmt.Fprintf(os.Stderr, "create group err: %v", err)
						}
						if _, err = l.Service.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
							LogGroupName:  l.group,
							LogStreamName: l.stream,
						}); err != nil {
							fmt.Fprintf(os.Stderr, "create stream err: %v", err)
						}
						return errors.New("retry"), true
					case "InvalidSequenceTokenException":
						// parse token from error (jank aws)
						// The given sequenceToken is invalid. The next expected sequenceToken is: 49540114571107725906840645449746451546762543407852177650
						msg := awsErr.Message()
						if i := strings.LastIndex(msg, " "); i > -1 {
							token := strings.TrimSpace(msg[i:])
							input.SequenceToken = &token
						}
						return err, true
					// Returned if a parameter of the request is incorrectly specified.
					case "InvalidParameterException":
						fmt.Fprintln(os.Stderr, "aws error", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
						return err, false
					}

					fmt.Fprintln(os.Stderr, "aws error", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
					fmt.Fprintln(os.Stderr, "retrying...")

					return err, true
				}

				// Generic AWS error with Code, Message, and original error (if any)
				if reqErr, ok := err.(awserr.RequestFailure); ok {
					// A Service error occurred
					fmt.Fprintln(os.Stderr, "aws fail", reqErr.Code(), reqErr.Message(), reqErr.OrigErr())
					return reqErr, false
				}

				// This case should never be hit, the SDK should always return an
				// error which satisfies the awserr.Error interface.
				fmt.Fprintf(os.Stderr, "unexpected err: %v\n", err)
				return err, false
			}

			l.sequenceToken = resp.NextSequenceToken

			return nil, false

		}); err != nil {
			failBatch(batch)
		}

	}

}

func failBatch(batch []*cloudwatchlogs.InputLogEvent) {
	// batch failed, drop it and move on
	fmt.Fprint(os.Stderr, "batch failed: ")
	if err := json.NewEncoder(os.Stderr).Encode(batch); err != nil {
		fmt.Fprintf(os.Stderr, "%#v\n", batch)
	}
}

func (l *Logger) Write(b []byte) (int, error) {
	return l.sw.Write(b)
}

func (l *Logger) WriteJSON(v interface{}) error {
	return json.NewEncoder(l).Encode(v)
}

func (l *Logger) WriteRoundTrip(resp *http.Response, duration time.Duration) error {
	type (
		Request struct {
			Method        string
			URL           *url.URL
			Header        http.Header
			ContentLength int64
		}
		Response struct {
			StatusCode    int
			Header        http.Header
			ContentLength int64
		}
		Payload struct {
			Type     string
			Request  Request
			Response Response
			Duration time.Duration
		}
	)
	return l.WriteJSON(&Payload{
		Type: "roundtrip",
		Request: Request{
			Method:        resp.Request.Method,
			URL:           resp.Request.URL,
			Header:        resp.Request.Header,
			ContentLength: resp.Request.ContentLength,
		},
		Response: Response{
			StatusCode:    resp.StatusCode,
			Header:        resp.Header,
			ContentLength: resp.ContentLength,
		},
		Duration: duration,
	})
}

func (l *Logger) WriteError(err error) error {
	type Payload struct {
		Type         string
		FunctionName string
		FileName     string
		Line         int
		Error        string
	}
	pc, fn, line, _ := runtime.Caller(1)
	return l.WriteJSON(&Payload{
		Type:         "error",
		FunctionName: runtime.FuncForPC(pc).Name(),
		FileName:     fn,
		Line:         line,
		Error:        err.Error(),
	})
}

func (l *Logger) Close() error {
	l.done <- struct{}{}
	<-l.done
	return nil
}
