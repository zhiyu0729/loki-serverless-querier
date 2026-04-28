package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type Store interface {
	Put(ctx context.Context, keyHint string, body []byte, contentType string) (*protocol.ObjectRef, error)
	Get(ctx context.Context, ref protocol.ObjectRef) ([]byte, error)
}

type MemoryStore struct {
	mu     sync.Mutex
	bucket string
	data   map[string][]byte
}

func NewMemoryStore(bucket string) *MemoryStore {
	if bucket == "" {
		bucket = "memory"
	}
	return &MemoryStore{bucket: bucket, data: map[string][]byte{}}
}

func (s *MemoryStore) Put(ctx context.Context, keyHint string, body []byte, contentType string) (*protocol.ObjectRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	sum := sha256.Sum256(body)
	key := sanitizeKeyHint(keyHint)
	if key == "" {
		key = "object"
	}
	key = fmt.Sprintf("%s/%d-%s", key, time.Now().UnixNano(), hex.EncodeToString(sum[:8]))

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), body...)
	return &protocol.ObjectRef{
		Bucket:      s.bucket,
		Key:         key,
		ContentType: contentType,
		SizeBytes:   int64(len(body)),
	}, nil
}

func (s *MemoryStore) Get(ctx context.Context, ref protocol.ObjectRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ref.Bucket != s.bucket {
		return nil, fmt.Errorf("memory object bucket mismatch: got %q want %q", ref.Bucket, s.bucket)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.data[ref.Key]
	if !ok {
		return nil, errors.New("memory object not found")
	}
	return append([]byte(nil), body...), nil
}

func sanitizeKeyHint(in string) string {
	in = strings.TrimSpace(in)
	in = strings.Trim(in, "/")
	in = strings.ReplaceAll(in, "..", "")
	in = strings.ReplaceAll(in, "\\", "/")
	for strings.Contains(in, "//") {
		in = strings.ReplaceAll(in, "//", "/")
	}
	return in
}
