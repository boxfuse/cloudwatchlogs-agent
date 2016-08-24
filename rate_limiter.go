package logger

import "time"

type RateLimiter struct {
	s chan struct{}
}

func NewRateLimiter(n int, d time.Duration) *RateLimiter {

	r := &RateLimiter{make(chan struct{}, n)}

	go func() {
		for {
		SLEEP:
			time.Sleep(d)
			for i := 0; i < n; i++ {
				select {
				case _, ok := <-r.s:
					if !ok {
						return
					}
				default:
					goto SLEEP
				}
			}
		}
	}()

	return r

}

func (r *RateLimiter) Ready() bool {
	r.s <- struct{}{}
	return true
}

func (r *RateLimiter) Close() {
	close(r.s)
}
