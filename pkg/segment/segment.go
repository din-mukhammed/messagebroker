package segment

import (
	"fmt"
	"os"
	"sync"

	log "github.com/mgutz/logxi/v1"

	"github.com/din-mukhammed/messagebroker/pkg/models"
)

type Segment interface {
	ReadByInd(int64) (*models.Message, error)
	PushBack(*models.Message) (int64, error)
}

type metadata struct {
	offset      int64
	bytesToRead int64
}

type segment struct {
	f          *os.File
	lstInd     int64
	totalBytes int64

	mu     sync.RWMutex
	ind2md map[int64]metadata
}

// file must be in append mode
func New(f *os.File) *segment {
	return &segment{
		f:      f,
		mu:     sync.RWMutex{},
		ind2md: make(map[int64]metadata),
	}
}

func (s *segment) PushBack(msg *models.Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.f.Write(msg.Body)
	if err != nil {
		return 0, err
	}

	l := s.lstInd
	s.ind2md[s.lstInd] = metadata{
		offset:      s.totalBytes,
		bytesToRead: int64(n),
	}
	s.totalBytes += int64(n)
	s.lstInd += 1
	log.Debug("pushed msg", "last ind", l)
	return l, nil
}

func (s *segment) ReadByInd(readInd int64) (*models.Message, error) {
	s.mu.RLock()
	md, ok := s.ind2md[int64(readInd)]
	if !ok {
		s.mu.RUnlock()
		return nil, fmt.Errorf("no such ind: %d\n", readInd)
	}
	s.mu.RUnlock()

	msg := &models.Message{
		Body: make([]byte, md.bytesToRead),
	}

	_, err := s.f.ReadAt(msg.Body, md.offset)
	if err != nil {
		return nil, err
	}

	return msg, nil
}
