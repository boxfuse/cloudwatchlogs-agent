package logger

import (
	"errors"
	"math/rand"
	"time"
)

type Trier struct {
	interval   time.Duration
	expiration time.Time
}

var (
	MaxSleepInterval = 10 * time.Second
	ErrMaxTries      = errors.New("max tries")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewTrier(maxTotalTime time.Duration) *Trier {
	return &Trier{
		interval:   time.Second,
		expiration: time.Now().Add(maxTotalTime),
	}
}

func (t *Trier) Try() bool {
	return time.Now().Before(t.expiration)
}

func (t *Trier) Wait() {

	// interval +- jitter
	w := t.interval + (t.interval / 2) - time.Duration(rand.Int63n(int64(t.interval)))

	if w > MaxSleepInterval {
		w = MaxSleepInterval
	}

	time.Sleep(w)

	t.interval *= 2 // exp growth

}

func (t *Trier) TryFunc(f func() (error, bool)) error {
	for ; t.Try(); t.Wait() {
		if err, retry := f(); err == nil || !retry {
			return err
		}
	}
	return ErrMaxTries
}
