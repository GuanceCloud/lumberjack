package lumberjack

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCompressionPoolLimits(t *testing.T) {
	for _, tt := range []struct {
		name      string
		workers   int
		files     int
		want      int
		instances int
	}{
		{name: "empty", instances: 1},
		{name: "default", files: 15, want: 6, instances: 1},
		{name: "negative", workers: -1, files: 15, want: 6, instances: 1},
		{name: "configured", workers: 3, files: 15, want: 3, instances: 1},
		{name: "serial", workers: 1, files: 15, want: 1, instances: 1},
		{name: "fewer files", workers: 6, files: 2, want: 2, instances: 1},
		{name: "independent loggers", files: 15, want: 6, instances: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "lumberjack-pool-")
			isNil(err, t)
			defer os.RemoveAll(dir)
			files := makeCompressionFiles(t, dir, tt.files)
			indexes := make(map[string]int)
			for i, file := range files {
				indexes[filepath.Join(dir, file.Name())] = i
			}

			started := make(chan int, tt.files*tt.instances)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			done := make(chan error, tt.instances)
			var mu sync.Mutex
			active := make([]int, tt.instances)
			peak := make([]int, tt.instances)
			calls := make([][]int, tt.instances)
			for instance := 0; instance < tt.instances; instance++ {
				calls[instance] = make([]int, tt.files)
				go func(instance int) {
					l := &Logger{Filename: logFile(dir), CompressWorkers: tt.workers}
					done <- l.compressLogFiles(files, func(src, dst string) error {
						index, ok := indexes[src]
						if !ok || dst != src+compressSuffix {
							return fmt.Errorf("unexpected compression paths: %q, %q", src, dst)
						}
						mu.Lock()
						calls[instance][index]++
						active[instance]++
						if active[instance] > peak[instance] {
							peak[instance] = active[instance]
						}
						mu.Unlock()
						started <- instance
						<-release
						mu.Lock()
						active[instance]--
						mu.Unlock()
						return nil
					})
				}(instance)
			}

			initial := make([]int, tt.instances)
			for i := 0; i < tt.want*tt.instances; i++ {
				select {
				case instance := <-started:
					initial[instance]++
				case err := <-done:
					t.Fatalf("pool returned before blocked jobs completed: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("workers did not start concurrently")
				}
			}
			for _, count := range initial {
				equals(tt.want, count, t)
			}
			if tt.files > 0 {
				select {
				case <-started:
					t.Fatal("pool exceeded its concurrency limit")
				case err := <-done:
					t.Fatalf("pool did not wait for blocked jobs: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
			}
			unblock()
			for i := 0; i < tt.instances; i++ {
				select {
				case err := <-done:
					isNil(err, t)
				case <-time.After(5 * time.Second):
					t.Fatal("pool did not finish")
				}
			}
			for instance := range calls {
				equals(tt.want, peak[instance], t)
				equals(0, active[instance], t)
				for _, count := range calls[instance] {
					equals(1, count, t)
				}
			}
		})
	}
}

func TestCompressionPoolErrorOrder(t *testing.T) {
	dir, err := ioutil.TempDir("", "lumberjack-pool-errors-")
	isNil(err, t)
	defer os.RemoveAll(dir)
	files := makeCompressionFiles(t, dir, 12)
	l := &Logger{Filename: logFile(dir), CompressWorkers: 2}
	firstErr := errors.New("first file failed")
	laterErr := errors.New("later file failed")
	laterFinished := make(chan struct{})
	calls := make(chan string, len(files))
	done := make(chan error, 1)
	go func() {
		done <- l.compressLogFiles(files, func(src, dst string) error {
			calls <- src
			switch filepath.Base(src) {
			case files[0].Name():
				<-laterFinished
				return firstErr
			case files[1].Name():
				return laterErr
			case files[2].Name():
				// With two workers and the first file blocked, reaching this
				// job means the second file's error has already been recorded.
				close(laterFinished)
				return nil
			default:
				return nil
			}
		})
	}()
	select {
	case err := <-done:
		if err != firstErr {
			t.Fatalf("expected first file error %v, got %v", firstErr, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pool stopped processing files after an error")
	}
	equals(len(files), len(calls), t)
}

func TestCompressionPoolRetainedFiles(t *testing.T) {
	for _, tt := range []struct {
		name     string
		compress bool
		maxAge   int
		retained int
	}{
		{name: "disabled", maxAge: 7, retained: 7},
		{name: "max backups", compress: true, retained: 10},
		{name: "max age", compress: true, maxAge: 7, retained: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "lumberjack-pool-retention-")
			isNil(err, t)
			defer os.RemoveAll(dir)
			files := makeCompressionFiles(t, dir, 20)
			l := &Logger{Filename: logFile(dir), Compress: tt.compress, MaxBackups: 10, MaxAge: tt.maxAge}
			live := []byte("current log\n")
			isNil(ioutil.WriteFile(l.Filename, live, 0600), t)
			unrelated := filepath.Join(dir, "unrelated.log")
			isNil(ioutil.WriteFile(unrelated, live, 0600), t)
			// An existing gzip file must not cause the same backup to be counted
			// twice. An incomplete gzip alongside its source is retried.
			retry := filepath.Join(dir, files[1].Name()) + compressSuffix
			isNil(ioutil.WriteFile(retry, []byte("incomplete"), 0600), t)
			alreadyCompressed := filepath.Join(dir, files[2].Name())
			isNil(compressLogFile(alreadyCompressed, alreadyCompressed+compressSuffix), t)

			isNil(l.millRunOnce(), t)
			for i, file := range files {
				path := filepath.Join(dir, file.Name())
				if i >= tt.retained {
					notExist(path, t)
					notExist(path+compressSuffix, t)
				} else if tt.compress || i == 2 {
					notExist(path, t)
					compressedContent(path+compressSuffix, compressionContent(i), t)
				} else {
					existsWithContent(path, compressionContent(i), t)
				}
			}
			existsWithContent(l.Filename, live, t)
			existsWithContent(unrelated, live, t)
			// A subsequent cleanup sees only completed gzip files.
			isNil(l.millRunOnce(), t)
		})
	}
}

func TestCompressionPoolFileError(t *testing.T) {
	dir, err := ioutil.TempDir("", "lumberjack-pool-file-error-")
	isNil(err, t)
	defer os.RemoveAll(dir)
	files := makeCompressionFiles(t, dir, 12)
	l := &Logger{Filename: logFile(dir), Compress: true}
	badSource := filepath.Join(dir, files[0].Name())
	// A directory at the output path causes compression to fail on every OS.
	isNil(os.Mkdir(badSource+compressSuffix, 0700), t)
	err = l.millRunOnce()
	if err == nil || !strings.Contains(err.Error(), badSource+compressSuffix) {
		t.Fatalf("expected compression error for %q, got %v", badSource, err)
	}
	existsWithContent(badSource, compressionContent(0), t)
	for i, file := range files[1:] {
		path := filepath.Join(dir, file.Name())
		notExist(path, t)
		compressedContent(path+compressSuffix, compressionContent(i+1), t)
	}
	// Failed sources remain available for the next cleanup to retry.
	isNil(os.Remove(badSource+compressSuffix), t)
	isNil(l.millRunOnce(), t)
	notExist(badSource, t)
	compressedContent(badSource+compressSuffix, compressionContent(0), t)
}

func makeCompressionFiles(t *testing.T, dir string, count int) []logInfo {
	t.Helper()
	files := make([]logInfo, 0, count)
	// Keep fixtures away from the exact MaxAge cutoff, including with a frozen clock.
	now := currentTime().UTC().Add(-time.Hour)
	for i := 0; i < count; i++ {
		stamp := now.Add(-time.Duration(i) * 24 * time.Hour)
		name := filepath.Join(dir, "foobar-"+stamp.Format(backupTimeFormat)+".log")
		isNil(ioutil.WriteFile(name, compressionContent(i), 0600), t)
		info, err := os.Stat(name)
		isNil(err, t)
		files = append(files, logInfo{timestamp: stamp, FileInfo: info})
	}
	return files
}

func compressionContent(index int) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("backup %d: log message\n", index)), 100)
}

func compressedContent(path string, want []byte, t *testing.T) {
	t.Helper()
	f, err := os.Open(path)
	isNil(err, t)
	defer f.Close()
	gz, err := gzip.NewReader(f)
	isNil(err, t)
	defer gz.Close()
	got, err := ioutil.ReadAll(gz)
	isNil(err, t)
	equals(want, got, t)
}
