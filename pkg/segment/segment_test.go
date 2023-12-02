package segment

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/din-mukhammed/messagebroker/pkg/models"
	. "gopkg.in/check.v1"

	"os"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SegmentSuite struct{}

var _ = Suite(&SegmentSuite{})

func (s *SegmentSuite) TestPushRead(c *C) {
	const fileName = "test_push_read_file"
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0600)
	c.Assert(err, IsNil)
	defer os.Remove(fileName)
	defer f.Close()

	seg := New(f)

	const total = 10_000
	wg := sync.WaitGroup{}
	for i := 0; i < total; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			bb := []byte(fmt.Sprintf("data-%d\n", i))
			ind, err := seg.PushBack(&models.Message{Body: bb})
			c.Assert(err, IsNil)
			m, err := seg.ReadByInd(ind)
			c.Assert(err, IsNil)
			c.Assert(bytes.Equal(m.Body, bb), Equals, true,
				Commentf("received: %s, expected: %s", string(m.Body), string(bb)))
		}()
	}
	wg.Wait()
}
