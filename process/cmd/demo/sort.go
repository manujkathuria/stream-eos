package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
)

// A fingerprint contains the full source event ID and SHA-256(key || JSON).
// Sorting uses fixed-size chunks and two-way disk merges. Only the current
// chunk and two buffered readers are resident, independent of total records.
type fingerprint [56]byte

const sortChunkRecords = 16384

type diskSorter struct {
	dir   string
	chunk []fingerprint
	runs  int
}

func newDiskSorter(parent, name string) (*diskSorter, error) {
	dir, err := os.MkdirTemp(parent, name)
	if err != nil {
		return nil, err
	}
	return &diskSorter{dir: dir, chunk: make([]fingerprint, 0, sortChunkRecords)}, nil
}
func (s *diskSorter) path(pass, run int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%d-%d", pass, run))
}
func (s *diskSorter) add(ctx context.Context, value fingerprint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.chunk = append(s.chunk, value)
	if len(s.chunk) == cap(s.chunk) {
		return s.flush()
	}
	return nil
}
func (s *diskSorter) flush() (err error) {
	if len(s.chunk) == 0 {
		return nil
	}
	slices.SortFunc(s.chunk, func(a, b fingerprint) int { return bytes.Compare(a[:], b[:]) })
	f, err := os.Create(s.path(0, s.runs))
	if err != nil {
		return err
	}
	defer func() {
		if e := f.Close(); err == nil {
			err = e
		}
	}()
	w := bufio.NewWriter(f)
	for i := range s.chunk {
		if _, err = w.Write(s.chunk[i][:]); err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	s.runs++
	s.chunk = s.chunk[:0]
	return nil
}
func (s *diskSorter) finish(ctx context.Context) (string, error) {
	if err := s.flush(); err != nil {
		return "", err
	}
	s.chunk = nil
	if s.runs == 0 {
		f, err := os.Create(s.path(0, 0))
		if err != nil {
			return "", err
		}
		return s.path(0, 0), f.Close()
	}
	pass, runs := 0, s.runs
	for runs > 1 {
		for run := 0; run < runs; run += 2 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			a, out := s.path(pass, run), s.path(pass+1, run/2)
			if run+1 == runs {
				if err := os.Rename(a, out); err != nil {
					return "", err
				}
			} else {
				b := s.path(pass, run+1)
				if err := mergeFiles(ctx, a, b, out); err != nil {
					return "", err
				}
				if err := os.Remove(a); err != nil {
					return "", err
				}
				if err := os.Remove(b); err != nil {
					return "", err
				}
			}
		}
		pass++
		runs = (runs + 1) / 2
	}
	return s.path(pass, 0), nil
}

type fingerprintReader struct {
	file   *os.File
	reader *bufio.Reader
}

func openFingerprints(path string) (*fingerprintReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fingerprintReader{f, bufio.NewReader(f)}, nil
}
func (r *fingerprintReader) next() (value fingerprint, err error) {
	_, err = io.ReadFull(r.reader, value[:])
	return
}
func mergeFiles(ctx context.Context, left, right, output string) (err error) {
	a, err := openFingerprints(left)
	if err != nil {
		return err
	}
	defer a.file.Close()
	b, err := openFingerprints(right)
	if err != nil {
		return err
	}
	defer b.file.Close()
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer func() {
		if e := f.Close(); err == nil {
			err = e
		}
	}()
	w := bufio.NewWriter(f)
	av, ae := a.next()
	bv, be := b.next()
	for ae != io.EOF || be != io.EOF {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ae != nil && ae != io.EOF {
			return ae
		}
		if be != nil && be != io.EOF {
			return be
		}
		if be == io.EOF || (ae == nil && bytes.Compare(av[:], bv[:]) <= 0) {
			if _, err = w.Write(av[:]); err != nil {
				return err
			}
			av, ae = a.next()
		} else {
			if _, err = w.Write(bv[:]); err != nil {
				return err
			}
			bv, be = b.next()
		}
	}
	return w.Flush()
}
