package filters

type (
	Filter struct {
		ReceivedAttrValue interface{} `json:"recvd_attr_val"`
		DesiredAttrValue  interface{} `json:"desired_attr_value"`

		Comparison  Comparison `json:"comparison_type"`
		ShouldMatch bool       `json:"should_match"`
	}

	Comparison interface {
		IsValid(Filter) bool
	}

	EqualTo     struct{}
	GreaterThan struct{}
	LesserThan  struct{}
	RangeOf     struct{}
)

func (filter Filter) IsValid() (isValid bool) {
	isValid = filter.Comparison.IsValid(filter)
	return
}

func (equalTo EqualTo) IsValid(filter Filter) (isValid bool) {
	if filter.ReceivedAttrValue == filter.DesiredAttrValue {
		isValid = true
	}
	return
}
