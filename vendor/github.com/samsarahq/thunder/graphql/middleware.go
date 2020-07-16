package graphql

import (
	"context"
)

type ComputationInput struct {
	Id                   string
	Query                string
	ParsedQuery          *Query
	Variables            map[string]interface{}
	Ctx                  context.Context
	Previous             interface{}
	IsInitialComputation bool
	Extensions           map[string]interface{}
}

type ComputationOutput struct {
	Metadata map[string]interface{}
	Current  interface{}
	Error    error
}

type MiddlewareFunc func(input *ComputationInput, next MiddlewareNextFunc) *ComputationOutput
type MiddlewareNextFunc func(input *ComputationInput) *ComputationOutput

func RunMiddlewares(middlewares []MiddlewareFunc, input *ComputationInput) *ComputationOutput {
	var run func(index int, middlewares []MiddlewareFunc, input *ComputationInput) *ComputationOutput
	run = func(index int, middlewares []MiddlewareFunc, input *ComputationInput) *ComputationOutput {
		if index >= len(middlewares) {
			return &ComputationOutput{
				Metadata: make(map[string]interface{}),
			}
		}

		middleware := middlewares[index]
		return middleware(input, func(input *ComputationInput) *ComputationOutput {
			return run(index+1, middlewares, input)
		})
	}

	return run(0, middlewares, input)
}
