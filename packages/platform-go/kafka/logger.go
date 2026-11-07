package kafka

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// slogLogger forwards franz-go's warnings and errors (broker loss, rebalance
// trouble, failed produce batches) to slog.
type slogLogger struct{ l *slog.Logger }

func newLogger(l *slog.Logger) kgo.Logger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l.With("component", "franz-go")}
}

func (slogLogger) Level() kgo.LogLevel { return kgo.LogLevelWarn }

func (s slogLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := slog.LevelWarn
	if level == kgo.LogLevelError {
		lvl = slog.LevelError
	}
	s.l.Log(context.Background(), lvl, msg, keyvals...)
}
