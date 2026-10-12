package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"
)

type child struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error // Written before done closes; read only after receiving done.
	logPath string
}

func startChild(ctx context.Context, binary, dir, logPath string, args ...string) (*child, error) {
	log, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Dir = dir
	// Explicit configuration: user .env files and service environment overrides
	// must not redirect demo traffic into unrelated topics or ports.
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "SYSTEMROOT"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	c := &child{cmd: cmd, done: make(chan struct{}), logPath: logPath}
	go func() { c.err = cmd.Wait(); log.Close(); close(c.done) }()
	return c, nil
}
func (c *child) failure(message string) error {
	raw, _ := os.ReadFile(c.logPath)
	return fmt.Errorf("%s: %v; log %s\n%s", message, c.err, c.logPath, raw)
}
func (c *child) wait(ctx context.Context) error {
	select {
	case <-c.done:
		if c.err != nil {
			return c.failure("service failed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *child) stop() error {
	select {
	case <-c.done:
		if c.err != nil {
			return c.failure("service failed")
		}
		return nil
	default:
	}
	_ = c.cmd.Process.Signal(os.Interrupt)
	select {
	case <-c.done:
		if c.err != nil {
			return c.failure("service shutdown failed")
		}
		return nil
	case <-time.After(5 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
		return c.failure("service required forced shutdown")
	}
}
