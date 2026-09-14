package lumberjack

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStartCompressWithoutWrite(t *testing.T) {
	for _, name := range []string{"new logfile", "existing logfile"} {
		t.Run(name, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "lumberjack-start-")
			isNil(err, t)
			defer os.RemoveAll(dir)
			files := makeCompressionFiles(t, dir, 12)
			l := &Logger{Filename: logFile(dir), Compress: true}
			defer l.Close()
			live := []byte("existing log content\n")
			if name == "existing logfile" {
				isNil(ioutil.WriteFile(l.Filename, live, 0600), t)
			}
			// Recover a gzip file left incomplete by an earlier process.
			retry := filepath.Join(dir, files[0].Name()) + compressSuffix
			isNil(ioutil.WriteFile(retry, []byte("incomplete"), 0600), t)

			l.Start()
			waitForStartFiles(t, dir, files)
			for i, file := range files {
				compressedContent(filepath.Join(dir, file.Name())+compressSuffix, compressionContent(i), t)
			}
			isNil(l.file, t)
			if name == "existing logfile" {
				existsWithContent(l.Filename, live, t)
			} else {
				notExist(l.Filename, t)
			}
		})
	}
}

func TestStartCleanupWithoutCompression(t *testing.T) {
	dir, err := ioutil.TempDir("", "lumberjack-start-cleanup-")
	isNil(err, t)
	defer os.RemoveAll(dir)
	files := makeCompressionFiles(t, dir, 12)
	l := &Logger{Filename: logFile(dir), MaxBackups: 2}
	defer l.Close()

	l.Start()
	waitForStartFiles(t, dir, files[2:])
	for i, file := range files[:2] {
		existsWithContent(filepath.Join(dir, file.Name()), compressionContent(i), t)
	}
	for _, file := range files {
		notExist(filepath.Join(dir, file.Name())+compressSuffix, t)
	}
	notExist(l.Filename, t)
}

func TestStartConcurrentAndRescan(t *testing.T) {
	dir, err := ioutil.TempDir("", "lumberjack-start-concurrent-")
	isNil(err, t)
	defer os.RemoveAll(dir)
	files := makeCompressionFiles(t, dir, 12)
	l := &Logger{Filename: logFile(dir), Compress: true}
	defer l.Close()

	var callers sync.WaitGroup
	for i := 0; i < 32; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			l.Start()
		}()
	}
	done := make(chan struct{})
	go func() {
		callers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Start calls did not return")
	}
	waitForStartFiles(t, dir, files)
	for i, file := range files {
		compressedContent(filepath.Join(dir, file.Name())+compressSuffix, compressionContent(i), t)
	}

	// Publish another complete backup, then explicitly request another scan.
	// Reusing an already compressed name also exercises retrying its gzip file.
	path := filepath.Join(dir, files[0].Name())
	content := []byte("backup discovered after startup\n")
	isNil(ioutil.WriteFile(path+".tmp", content, 0600), t)
	isNil(os.Rename(path+".tmp", path), t)
	l.Start()
	waitForStartFiles(t, dir, files[:1])
	compressedContent(path+compressSuffix, content, t)

	// Starting maintenance must leave normal writes and rotations usable.
	live := []byte("new log content\n")
	n, err := l.Write(live)
	isNil(err, t)
	equals(len(live), n, t)
	existsWithContent(l.Filename, live, t)
	isNil(l.Rotate(), t)
	rotated, err := l.oldLogFiles()
	isNil(err, t)
	waitForStartFiles(t, dir, rotated[:1])
	rotatedPath := filepath.Join(dir, rotated[0].Name())
	// The rotation may already have been compressed before the directory scan.
	if filepath.Ext(rotatedPath) != compressSuffix {
		rotatedPath += compressSuffix
	}
	compressedContent(rotatedPath, live, t)
	existsWithContent(l.Filename, []byte{}, t)
}

// waitForStartFiles waits for source removal, which happens after a successful
// gzip close (or retention cleanup), without depending on a fixed sleep.
func waitForStartFiles(t *testing.T, dir string, files []logInfo) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for _, file := range files {
		path := filepath.Join(dir, strings.TrimSuffix(file.Name(), compressSuffix))
		for {
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				break
			}
			isNil(err, t)
			if time.Now().After(deadline) {
				t.Fatalf("background processing did not remove %q", path)
			}
			time.Sleep(time.Millisecond)
		}
	}
}
