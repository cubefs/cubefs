package trace

// TracerOption is a function that sets some option on the tracer
type TracerOption func(tracer *Tracer)

// TracerOptions is a factory for all available TracerOption's
var TracerOptions tracerOptions

type tracerOptions struct{}

func (tracerOptions) MaxLogsPerSpan(maxLogsPerSpan int) TracerOption {
	return func(tracer *Tracer) {
		tracer.options.maxLogsPerSpan = maxLogsPerSpan
	}
}
