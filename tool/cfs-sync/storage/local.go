package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// LocalStorage is a Storage backend backed by the local filesystem.
type LocalStorage struct {
	root string
}

// NewLocal creates a LocalStorage rooted at root.
func NewLocal(root string) (*LocalStorage, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve path %s: %w", root, err)
	}
	return &LocalStorage{root: abs}, nil
}

func (l *LocalStorage) String() string { return l.root }

func (l *LocalStorage) fullPath(key string) string {
	return filepath.Join(l.root, filepath.FromSlash(key))
}

func (l *LocalStorage) List(ctx context.Context, prefix string) (<-chan *Object, <-chan error) {
	objects := make(chan *Object, 256)
	errc := make(chan error, 1)

	go func() {
		defer close(objects)
		defer close(errc)

		base := l.fullPath(prefix)
		var entries []string

		err := filepath.WalkDir(base, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			entries = append(entries, p)
			return nil
		})
		if err != nil {
			if !os.IsNotExist(err) {
				errc <- err
			}
			return
		}

		// sort for lexicographic order
		sort.Strings(entries)

		for _, p := range entries {
			info, serr := os.Stat(p)
			if serr != nil {
				continue
			}
			rel, rerr := filepath.Rel(l.root, p)
			if rerr != nil {
				continue
			}
			key := filepath.ToSlash(rel)
			if key == "." {
				if !info.IsDir() {
					// l.root itself is a plain file — emit as key="" for single-file sync.
					objects <- &Object{Key: "", Size: info.Size(), Mtime: info.ModTime()}
				}
				continue
			}
			obj := &Object{
				Key:   key,
				Size:  info.Size(),
				Mtime: info.ModTime(),
				IsDir: info.IsDir(),
			}
			if info.IsDir() {
				obj.Key += "/"
				obj.Size = 0
			}
			select {
			case <-ctx.Done():
				return
			case objects <- obj:
			}
		}
	}()

	return objects, errc
}

func (l *LocalStorage) Get(_ context.Context, key string, off, size int64) (io.ReadCloser, error) {
	f, err := os.Open(l.fullPath(key))
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err = f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	if size > 0 {
		return &limitedRC{f, io.LimitReader(f, size)}, nil
	}
	return f, nil
}

func (l *LocalStorage) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return l.PutWithMtime(ctx, key, r, size, time.Time{})
}

func (l *LocalStorage) PutWithMtime(_ context.Context, key string, r io.Reader, _ int64, mtime time.Time) error {
	dst := l.fullPath(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, r)
	if cerr := f.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if !mtime.IsZero() {
		_ = os.Chtimes(dst, mtime, mtime)
	}
	return nil
}

func (l *LocalStorage) Delete(_ context.Context, key string) error {
	return os.Remove(l.fullPath(key))
}

func (l *LocalStorage) MkdirAll(_ context.Context, key string) error {
	return os.MkdirAll(l.fullPath(key), 0o755)
}

// limitedRC wraps a file with an io.LimitReader so Close still works on the file.
type limitedRC struct {
	f *os.File
	r io.Reader
}

func (l *limitedRC) Read(p []byte) (int, error) { return l.r.Read(p) }
func (l *limitedRC) Close() error               { return l.f.Close() }
