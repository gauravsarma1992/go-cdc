package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterEqualToFailure(t *testing.T) {
	filter := Filter{
		ReceivedAttrValue: "Hello",
		DesiredAttrValue:  "World",
		Comparison:        EqualTo{},
		ShouldMatch:       true,
	}
	assert.Equal(t, filter.IsValid(), false, "EqualTo should fail if values are different")
	return
}

func TestFilterEqualToSuccess(t *testing.T) {
	filter := Filter{
		ReceivedAttrValue: "Hello",
		DesiredAttrValue:  "Hello",
		Comparison:        EqualTo{},
		ShouldMatch:       true,
	}
	assert.Equal(t, filter.IsValid(), true, "EqualTo should succeed if values are same")
	return
}
