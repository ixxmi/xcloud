package fileutil

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"xcloud/internal/syncmodel"
)

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func HashFile(path string, chunkSize int) (string, []syncmodel.ChunkRef, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", nil, 0, err
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, chunkSize)
	var chunks []syncmodel.ChunkRef
	var total int64
	index := 0
	for {
		n, readErr := io.ReadFull(f, buf)
		if readErr == io.EOF {
			break
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return "", nil, 0, readErr
		}
		part := buf[:n]
		chunkHash := HashBytes(part)
		chunks = append(chunks, syncmodel.ChunkRef{
			Index: index,
			Hash:  chunkHash,
			Size:  int64(n),
		})
		if _, err := h.Write(part); err != nil {
			return "", nil, 0, err
		}
		total += int64(n)
		index++
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	return hex.EncodeToString(h.Sum(nil)), chunks, total, nil
}

func WriteFileChunks(path string, chunkSize int, handle func(syncmodel.ChunkRef, []byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	index := 0
	for {
		n, readErr := io.ReadFull(f, buf)
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil && readErr != io.ErrUnexpectedEOF {
			return readErr
		}
		part := make([]byte, n)
		copy(part, buf[:n])
		ref := syncmodel.ChunkRef{
			Index: index,
			Hash:  HashBytes(part),
			Size:  int64(n),
		}
		if err := handle(ref, part); err != nil {
			return err
		}
		index++
		if readErr == io.ErrUnexpectedEOF {
			return nil
		}
	}
}

func SafeRel(root, path string) (string, error) {
	clean, err := CleanRel(path)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, filepath.FromSlash(clean))
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	fullAbs, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	if !PathWithin(rootAbs, fullAbs) {
		return "", fmt.Errorf("path escapes root: %s", path)
	}
	return filepath.ToSlash(clean), nil
}

func CleanRel(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty path")
	}
	normalized := strings.ReplaceAll(path, "\\", "/")
	if strings.HasPrefix(normalized, "/") || filepath.IsAbs(path) {
		return "", fmt.Errorf("absolute paths are not allowed: %s", path)
	}
	parts := strings.Split(normalized, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("unsafe path segment %q in %s", part, path)
		}
	}
	clean := filepath.ToSlash(filepath.Clean(normalized))
	if clean == "." || clean == "/" {
		return "", errors.New("invalid path")
	}
	return clean, nil
}

func PathWithin(rootAbs, pathAbs string) bool {
	rootAbs = filepath.Clean(rootAbs)
	pathAbs = filepath.Clean(pathAbs)
	if runtime.GOOS == "windows" {
		rootAbs = strings.ToLower(rootAbs)
		pathAbs = strings.ToLower(pathAbs)
	}
	if rootAbs == pathAbs {
		return true
	}
	rel, err := filepath.Rel(rootAbs, pathAbs)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func EnsureParent(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func AtomicWrite(path string, write func(*os.File) error) error {
	if err := EnsureParent(path); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.sync_tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := write(tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
