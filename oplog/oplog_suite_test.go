package oplog_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOplog(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Oplog Suite")
}
