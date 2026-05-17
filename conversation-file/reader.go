package conversationfile

import "context"

// Ported from exoclaw_conversation/_reader.py.
//
// Default SessionReader implementation: wraps a HistoryStore that doesn't
// yet provide a native streaming reader. Backends with on-disk storage
// (SessionManager) override HistoryStore.Reader with a true line-by-line
// streaming impl that never holds the full log in RAM. This default is a
// correctness fallback — it materialises via LoadRange so it works for
// any store but loses the memory-budget guarantee.

// defaultSessionReader is a generic streaming reader over a HistoryStore.
//
// Restartable: Stream re-reads on each call. Count and At go through the
// store directly so backends that maintain a cheap index can answer them
// without scanning the full log.
type defaultSessionReader struct {
	store HistoryStore
	key   string
}

// NewDefaultSessionReader returns the fallback streaming reader.
func NewDefaultSessionReader(store HistoryStore, key string) SessionReader {
	return &defaultSessionReader{store: store, key: key}
}

func (r *defaultSessionReader) Key() string { return r.key }

func (r *defaultSessionReader) Count(_ context.Context) (int, error) {
	msgs, err := r.store.LoadRange(r.key, 0, 1<<30)
	if err != nil {
		return 0, err
	}
	return len(msgs), nil
}

func (r *defaultSessionReader) Stream(ctx context.Context, start, end int) <-chan StreamMessage {
	out := make(chan StreamMessage)
	go func() {
		defer close(out)
		stop := end
		if stop < 0 {
			stop = 1 << 30
		}
		msgs, err := r.store.LoadRange(r.key, start, stop)
		if err != nil {
			select {
			case out <- StreamMessage{Err: err}:
			case <-ctx.Done():
			}
			return
		}
		for _, m := range msgs {
			select {
			case out <- StreamMessage{Message: m}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (r *defaultSessionReader) At(_ context.Context, index int) (map[string]any, error) {
	msgs, err := r.store.LoadRange(r.key, index, index+1)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return msgs[0], nil
}
