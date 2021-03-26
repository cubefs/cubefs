package xkit

import (
	"bytes"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/stretchr/testify/assert"
)

func TestLogger_Infof(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(log.NewLogfmtLogger(buf))

	logger.Infof("Formatted %s", "string")

	assert.Equal(t, "level=info msg=\"Formatted string\"\n", buf.String())
}

func TestLogger_Error(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(log.NewLogfmtLogger(buf))

	logger.Error("Something really bad happened")

	assert.Equal(t, "level=error msg=\"Something really bad happened\"\n", buf.String())
}

func TestMessageKey(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := NewLogger(log.NewLogfmtLogger(buf), MessageKey("message"))

	logger.Error("Something really bad happened")

	assert.Equal(t, "level=error message=\"Something really bad happened\"\n", buf.String())
}
