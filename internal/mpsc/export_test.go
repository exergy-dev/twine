package mpsc

// Cap returns the queue's capacity (the rounded value passed to New). Test
// helper.
func (q *Queue) Cap() int { return int(q.mask + 1) }
