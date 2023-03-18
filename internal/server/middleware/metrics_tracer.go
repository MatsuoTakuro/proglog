package middleware

import (
	"strings"

	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
)

// TODO: find out why return values for tracer and metrics are not needed
func SetMetricsAndTracer() error {
	// create a tracer and trace requests
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	// trace.ApplyConfig(trace.Config{DefaultSampler: CustomSampler()})

	//crate a stats of metrics and begin collecting data
	err := view.Register(ocgrpc.DefaultServerViews...)

	return err
}

func customSampler() trace.Sampler {
	halfSampler := trace.ProbabilitySampler(0.5)
	trace.ApplyConfig(trace.Config{
		DefaultSampler: func(sp trace.SamplingParameters) trace.SamplingDecision {
			if strings.Contains(sp.Name, "Produce") {
				return trace.SamplingDecision{Sample: true}
			}
			return halfSampler(sp)
		},
	})
	return halfSampler
}
