package interval

import (
	"errors"
	"time"

	"github.com/grafana/loki/v3/pkg/serverless/protocol"
)

type Range struct {
	Start time.Time
	End   time.Time
}

func (r Range) Validate() error {
	if r.Start.IsZero() || r.End.IsZero() {
		return errors.New("range start and end are required")
	}
	if !r.End.After(r.Start) {
		return errors.New("range end must be after start")
	}
	return nil
}

func Split(r Range, max time.Duration) ([]Range, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	if max <= 0 || r.End.Sub(r.Start) <= max {
		return []Range{r}, nil
	}

	var out []Range
	for start := r.Start; start.Before(r.End); {
		end := start.Add(max)
		if end.After(r.End) {
			end = r.End
		}
		out = append(out, Range{Start: start, End: end})
		start = end
	}
	return out, nil
}

func SplitRequest(req protocol.ServerlessQueryRequest, max time.Duration) ([]*protocol.ServerlessQueryRequest, error) {
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.QueryType == protocol.QueryTypeInstant {
		return []*protocol.ServerlessQueryRequest{&req}, nil
	}

	ranges, err := Split(Range{Start: req.StartTime(), End: req.EndTime()}, max)
	if err != nil {
		return nil, err
	}

	out := make([]*protocol.ServerlessQueryRequest, 0, len(ranges))
	for _, r := range ranges {
		out = append(out, req.WithInterval(r.Start, r.End))
	}
	return out, nil
}

func HalveRequest(req protocol.ServerlessQueryRequest) ([]*protocol.ServerlessQueryRequest, error) {
	req.SetDefaults()
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.QueryType == protocol.QueryTypeInstant {
		return []*protocol.ServerlessQueryRequest{&req}, nil
	}

	start := req.StartTime()
	end := req.EndTime()
	if !end.After(start) {
		return nil, errors.New("cannot halve empty interval")
	}
	mid := start.Add(end.Sub(start) / 2)
	if !mid.After(start) || !end.After(mid) {
		return []*protocol.ServerlessQueryRequest{&req}, nil
	}
	return []*protocol.ServerlessQueryRequest{
		req.WithInterval(start, mid),
		req.WithInterval(mid, end),
	}, nil
}
