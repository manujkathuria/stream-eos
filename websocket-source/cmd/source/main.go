package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"stream-eos/pkg/logger"
	"stream-eos/websocket-source/feed"
)

func main() {
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "configure the source in .env; CLI arguments are not supported")
		os.Exit(1)
	}
	cfg, err := loadConfig(".env")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := logger.New(logger.Config{Service: "websocket-source", Level: cfg.logLevel, Format: cfg.logFormat}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		logger.Error(ctx, "source stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	handler, err := feed.NewHandler(feed.Config{DisconnectAfter: cfg.disconnectAfter, Rate: cfg.rate, HistoryUpdates: cfg.historyUpdates, InstrumentCount: cfg.instrumentCount})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	logger.Info(ctx, "starting source: address=%s rate=%d", listener.Addr(), cfg.rate)
	return serve(ctx, server, listener, handler, cfg.shutdownTimeout)
}

func serve(ctx context.Context, server *http.Server, listener net.Listener, handler *feed.Handler, timeout time.Duration) error {
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-result:
	}
	logger.Info(ctx, "stopping source")
	// The signal context is canceled; cleanup needs its own bounded context.
	cleanup, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// Shutdown stops HTTP acceptance; feed shutdown also owns hijacked sessions.
	httpResult := make(chan error, 1)
	go func() { httpResult <- server.Shutdown(cleanup) }()
	feedErr := handler.Shutdown(cleanup)
	httpErr := <-httpResult
	if feedErr != nil || httpErr != nil {
		_ = server.Close()
	}
	if serveErr == nil {
		serveErr = <-result
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	err := errors.Join(serveErr, feedErr, httpErr)
	if err == nil {
		logger.Info(cleanup, "source stopped cleanly")
	}
	return err
}
