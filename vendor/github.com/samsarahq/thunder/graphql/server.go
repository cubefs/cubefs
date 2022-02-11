package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/samsarahq/go/oops"
	"github.com/samsarahq/thunder/batch"
	"github.com/samsarahq/thunder/diff"
	"github.com/samsarahq/thunder/reactive"
)

const (
	DefaultMaxSubscriptions = 200
	DefaultMinRerunInterval = 5 * time.Second
)

type JSONSocket interface {
	ReadJSON(value interface{}) error
	WriteJSON(value interface{}) error
	Close() error
}

type MakeCtxFunc func(context.Context) context.Context

type AlwaysSpawnGoroutineFunc func(context.Context, *Query) bool
type RerunIntervalFunc func(context.Context, *Query) time.Duration

type GraphqlLogger interface {
	StartExecution(ctx context.Context, tags map[string]string, initial bool)
	FinishExecution(ctx context.Context, tags map[string]string, delay time.Duration)
	Error(ctx context.Context, err error, tags map[string]string)
}

type SubscriptionLogger interface {
	// Subscribe is called when a new subscription is started. Subscribe is not
	// called for queries that fail to parse or validate.
	Subscribe(ctx context.Context, id string, tags map[string]string)

	// Unsubscribe is called  when a subscription ends. It is guaranteed
	// to be called even if the subscription ends due to the connection closing.
	// The id argument corresponds to the id tag.
	Unsubscribe(ctx context.Context, id string)
}

type conn struct {
	writeMu sync.Mutex
	socket  JSONSocket

	schema         *Schema
	mutationSchema *Schema
	ctx            context.Context
	makeCtx        MakeCtxFunc
	middlewares    []MiddlewareFunc

	executor ExecutorRunner

	logger             GraphqlLogger
	subscriptionLogger SubscriptionLogger

	url string

	mutateMu sync.Mutex

	mu            sync.Mutex
	subscriptions map[string]*reactive.Rerunner

	alwaysSpawnGoroutineFunc AlwaysSpawnGoroutineFunc
	minRerunIntervalFunc     RerunIntervalFunc
	maxSubscriptions         int
}

type inEnvelope struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Message    json.RawMessage        `json:"message"`
	Extensions map[string]interface{} `json:"extensions,omitempty"`
}

type outEnvelope struct {
	ID       string                 `json:"id,omitempty"`
	Type     string                 `json:"type"`
	Message  interface{}            `json:"message,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type subscribeMessage struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

type mutateMessage struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

func (c *conn) writeOrClose(out outEnvelope) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.socket.WriteJSON(out); err != nil {
		if !isCloseError(err) {
			c.socket.Close()
			log.Printf("socket.WriteJSON: %s\n", err)
		}
	}
}

func mustMarshalJson(v interface{}) string {
	bytes, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

func (c *conn) handleSubscribe(in *inEnvelope) error {
	id := in.ID
	var subscribe subscribeMessage
	if err := json.Unmarshal(in.Message, &subscribe); err != nil {
		return oops.Wrapf(err, "failed to parse subscribe message: %s", in.Message)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.subscriptions[id]; ok {
		return NewSafeError("duplicate subscription")
	}

	if len(c.subscriptions)+1 > c.maxSubscriptions {
		return NewSafeError("too many subscriptions")
	}

	tags := map[string]string{"url": c.url, "query": subscribe.Query, "queryVariables": mustMarshalJson(subscribe.Variables), "id": id}

	query, err := Parse(subscribe.Query, subscribe.Variables)
	if query != nil {
		tags["queryType"] = query.Kind
		tags["queryName"] = query.Name
	}
	if err != nil {
		c.logger.Error(c.ctx, err, tags)
		return err
	}
	if err := PrepareQuery(context.Background(), c.schema.Query, query.SelectionSet); err != nil {
		c.logger.Error(c.ctx, err, tags)
		return err
	}

	var previous interface{}

	e := c.executor

	initial := true
	c.subscriptionLogger.Subscribe(c.ctx, id, tags)
	c.subscriptions[id] = reactive.NewRerunner(c.ctx, func(ctx context.Context) (interface{}, error) {
		ctx = c.makeCtx(ctx)
		ctx = batch.WithBatching(ctx)

		start := time.Now()

		c.logger.StartExecution(ctx, tags, initial)

		var middlewares []MiddlewareFunc
		middlewares = append(middlewares, c.middlewares...)
		middlewares = append(middlewares, func(input *ComputationInput, next MiddlewareNextFunc) *ComputationOutput {
			output := next(input)
			output.Current, output.Error = e.Execute(input.Ctx, c.schema.Query, nil, input.ParsedQuery)
			return output
		})

		computationInput := &ComputationInput{
			Ctx:                  ctx,
			Id:                   id,
			ParsedQuery:          query,
			Previous:             previous,
			IsInitialComputation: initial,
			Query:                subscribe.Query,
			Variables:            subscribe.Variables,
			Extensions:           in.Extensions,
		}

		output := RunMiddlewares(middlewares, computationInput)
		current, err := output.Current, output.Error

		c.logger.FinishExecution(ctx, tags, time.Since(start))

		if err != nil {
			if ErrorCause(err) == context.Canceled {
				go c.closeSubscription(id)
				return nil, err
			}

			if !initial {
				// If this a re-computation, tell the Rerunner to retry the computation
				// without dumping the contents of the current computation cache.
				// Note that we are swallowing the propagation of the error in this case,
				// but we still log it.
				if _, ok := err.(SanitizedError); !ok {
					extraTags := map[string]string{"retry": "true"}
					for k, v := range tags {
						extraTags[k] = v
					}
					c.logger.Error(ctx, err, extraTags)
				}

				return nil, reactive.RetrySentinelError
			}

			c.writeOrClose(outEnvelope{
				ID:       id,
				Type:     "error",
				Message:  SanitizeError(err),
				Metadata: output.Metadata,
			})
			go c.closeSubscription(id)

			if _, ok := err.(SanitizedError); !ok {
				c.logger.Error(ctx, err, tags)
			}
			return nil, err
		}

		d := diff.Diff(computationInput.Previous, current)
		previous = current

		if d != nil {
			c.writeOrClose(outEnvelope{
				ID:       id,
				Type:     "update",
				Message:  d,
				Metadata: output.Metadata,
			})
		} else if initial {
			// When a client first subscribes, they expect a response with the new diff (even if the diff is unchanged).
			c.writeOrClose(outEnvelope{
				ID:       id,
				Type:     "update",
				Message:  struct{}{}, // This is an empty diff for any message, rather than nil which means the new message is empty.
				Metadata: output.Metadata,
			})
		}

		initial = false
		return nil, nil
	}, c.minRerunIntervalFunc(c.ctx, query), c.alwaysSpawnGoroutineFunc(c.ctx, query))

	return nil
}

func (c *conn) handleMutate(in *inEnvelope) error {
	// TODO: deduplicate code
	id := in.ID
	var mutate mutateMessage
	if err := json.Unmarshal(in.Message, &mutate); err != nil {
		return oops.Wrapf(err, "failed to parse mutate message: %s", in.Message)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	tags := map[string]string{"url": c.url, "query": mutate.Query, "queryVariables": mustMarshalJson(mutate.Variables), "id": id}

	query, err := Parse(mutate.Query, mutate.Variables)
	if query != nil {
		tags["queryType"] = query.Kind
		tags["queryName"] = query.Name
	}
	if err != nil {
		c.logger.Error(c.ctx, err, tags)
		return err
	}
	if err := PrepareQuery(c.ctx, c.mutationSchema.Mutation, query.SelectionSet); err != nil {
		c.logger.Error(c.ctx, err, tags)
		return err
	}

	initial := true
	e := c.executor
	c.subscriptions[id] = reactive.NewRerunner(c.ctx, func(ctx context.Context) (interface{}, error) {
		// Serialize all mutates for a given connection.
		c.mutateMu.Lock()
		defer c.mutateMu.Unlock()

		ctx = c.makeCtx(ctx)
		ctx = batch.WithBatching(ctx)

		start := time.Now()
		c.logger.StartExecution(ctx, tags, true)

		var middlewares []MiddlewareFunc
		middlewares = append(middlewares, c.middlewares...)
		middlewares = append(middlewares, func(input *ComputationInput, next MiddlewareNextFunc) *ComputationOutput {
			output := next(input)
			output.Current, output.Error = e.Execute(input.Ctx, c.mutationSchema.Mutation, c.mutationSchema.Mutation, query)
			return output
		})

		computationInput := &ComputationInput{
			Ctx:                  ctx,
			Id:                   id,
			ParsedQuery:          query,
			Previous:             nil,
			IsInitialComputation: initial,
			Query:                mutate.Query,
			Variables:            mutate.Variables,
			Extensions:           in.Extensions,
		}

		output := RunMiddlewares(middlewares, computationInput)
		current, err := output.Current, output.Error

		c.logger.FinishExecution(ctx, tags, time.Since(start))

		if err != nil {
			c.writeOrClose(outEnvelope{
				ID:       id,
				Type:     "error",
				Message:  SanitizeError(err),
				Metadata: output.Metadata,
			})

			go c.closeSubscription(id)

			if ErrorCause(err) == context.Canceled {
				return nil, err
			}

			if _, ok := err.(SanitizedError); !ok {
				c.logger.Error(ctx, err, tags)
			}
			return nil, err
		}

		c.writeOrClose(outEnvelope{
			ID:       id,
			Type:     "result",
			Message:  diff.Diff(nil, current),
			Metadata: output.Metadata,
		})

		go c.rerunSubscriptionsImmediately()

		initial = false
		go c.closeSubscription(id)
		return nil, errors.New("stop")
	}, c.minRerunIntervalFunc(c.ctx, query), c.alwaysSpawnGoroutineFunc(c.ctx, query))

	return nil
}

func (c *conn) rerunSubscriptionsImmediately() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, runner := range c.subscriptions {
		runner.RerunImmediately()
	}
}

func (c *conn) closeSubscription(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if runner, ok := c.subscriptions[id]; ok {
		runner.Stop()
		delete(c.subscriptions, id)
		c.subscriptionLogger.Unsubscribe(c.ctx, id)
	}
}

func (c *conn) closeSubscriptions() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, runner := range c.subscriptions {
		runner.Stop()
		delete(c.subscriptions, id)
	}
}

func (c *conn) handle(e *inEnvelope) error {
	switch e.Type {
	case "subscribe":
		return c.handleSubscribe(e)

	case "unsubscribe":
		c.closeSubscription(e.ID)
		return nil

	case "mutate":
		return c.handleMutate(e)

	case "echo":
		c.writeOrClose(outEnvelope{
			ID:       e.ID,
			Type:     "echo",
			Message:  nil,
			Metadata: nil,
		})
		return nil

	case "url":
		var url string
		if err := json.Unmarshal(e.Message, &url); err != nil {
			return err
		}
		c.url = url
		return nil

	default:
		return NewSafeError("unknown message type")
	}
}

type simpleLogger struct {
}

func (s *simpleLogger) StartExecution(ctx context.Context, tags map[string]string, initial bool) {
}
func (s *simpleLogger) FinishExecution(ctx context.Context, tags map[string]string, delay time.Duration) {
}
func (s *simpleLogger) Error(ctx context.Context, err error, tags map[string]string) {
	log.Printf("error:%v\n%s", tags, err)
}

func Handler(schema *Schema) http.Handler {
	upgrader := &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("upgrader.Upgrade: %v", err)
			return
		}
		defer socket.Close()

		makeCtx := func(ctx context.Context) context.Context {
			return ctx
		}

		ServeJSONSocket(r.Context(), socket, schema, makeCtx, &simpleLogger{})
	})
}

func (c *conn) Use(fn MiddlewareFunc) {
	c.middlewares = append(c.middlewares, fn)
}

// ServeJSONSocket is deprecated. Consider using CreateConnection instead.
func ServeJSONSocket(ctx context.Context, socket JSONSocket, schema *Schema, makeCtx MakeCtxFunc, logger GraphqlLogger) {
	conn := CreateJSONSocket(ctx, socket, schema, makeCtx, logger)
	conn.ServeJSONSocket()
}

// CreateJSONSocket is deprecated. Consider using CreateConnection instead.
func CreateJSONSocket(ctx context.Context, socket JSONSocket, schema *Schema, makeCtx MakeCtxFunc, logger GraphqlLogger) *conn {
	return CreateConnection(ctx, socket, schema, WithMakeCtx(makeCtx), WithExecutionLogger(logger))
}

// CreateJSONSocketWithMutationSchema is deprecated. Consider using CreateConnection instead.
func CreateJSONSocketWithMutationSchema(ctx context.Context, socket JSONSocket, schema, mutationSchema *Schema, makeCtx MakeCtxFunc, logger GraphqlLogger) *conn {
	return CreateConnection(ctx, socket, schema, WithMakeCtx(makeCtx), WithExecutionLogger(logger), WithMutationSchema(mutationSchema))
}

type nopSubscriptionLogger struct{}

func (l *nopSubscriptionLogger) Subscribe(ctx context.Context, id string, tags map[string]string) {}
func (l *nopSubscriptionLogger) Unsubscribe(ctx context.Context, id string)                       {}

type nopGraphqlLogger struct{}

func (l *nopGraphqlLogger) StartExecution(ctx context.Context, tags map[string]string, initial bool) {
}
func (l *nopGraphqlLogger) FinishExecution(ctx context.Context, tags map[string]string, delay time.Duration) {
}
func (l *nopGraphqlLogger) Error(ctx context.Context, err error, tags map[string]string) {}

type ConnectionOption func(*conn)

func CreateConnection(ctx context.Context, socket JSONSocket, schema *Schema, opts ...ConnectionOption) *conn {
	c := &conn{
		socket:             socket,
		ctx:                ctx,
		schema:             schema,
		mutationSchema:     schema,
		executor:           NewExecutor(NewImmediateGoroutineScheduler()),
		subscriptions:      make(map[string]*reactive.Rerunner),
		subscriptionLogger: &nopSubscriptionLogger{},
		logger:             &nopGraphqlLogger{},
		makeCtx: func(ctx context.Context) context.Context {
			return ctx
		},
		maxSubscriptions:         DefaultMaxSubscriptions,
		minRerunIntervalFunc:     func(context.Context, *Query) time.Duration { return DefaultMinRerunInterval },
		alwaysSpawnGoroutineFunc: func(context.Context, *Query) bool { return false },
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithExecutor(executor ExecutorRunner) ConnectionOption {
	return func(c *conn) {
		c.executor = executor
	}
}

func WithExecutionLogger(logger GraphqlLogger) ConnectionOption {
	return func(c *conn) {
		c.logger = logger
	}
}

func WithMinRerunInterval(d time.Duration) ConnectionOption {
	return func(c *conn) {
		c.minRerunIntervalFunc = func(context.Context, *Query) time.Duration { return d }
	}
}

func WithAlwaysSpawnGoroutineFunc(fn AlwaysSpawnGoroutineFunc) ConnectionOption {
	return func(c *conn) {
		c.alwaysSpawnGoroutineFunc = fn
	}
}

func WithMaxSubscriptions(max int) ConnectionOption {
	return func(c *conn) {
		c.maxSubscriptions = max
	}
}

func WithMutationSchema(schema *Schema) ConnectionOption {
	return func(c *conn) {
		c.mutationSchema = schema
	}
}

func WithMakeCtx(makeCtx MakeCtxFunc) ConnectionOption {
	return func(c *conn) {
		c.makeCtx = makeCtx
	}
}

func WithSubscriptionLogger(logger SubscriptionLogger) ConnectionOption {
	return func(c *conn) {
		c.subscriptionLogger = logger
	}
}

// WithMinRerunIntervalFunc is deprecated.
func WithMinRerunIntervalFunc(fn RerunIntervalFunc) ConnectionOption {
	return func(c *conn) {
		c.minRerunIntervalFunc = fn
	}
}

func (c *conn) ServeJSONSocket() {
	defer c.closeSubscriptions()

	for {
		var envelope inEnvelope
		if err := c.socket.ReadJSON(&envelope); err != nil {
			if !isCloseError(err) {
				log.Println("socket.ReadJSON:", err)
			}
			return
		}

		if err := c.handle(&envelope); err != nil {
			log.Println("c.handle:", err)
			c.writeOrClose(outEnvelope{
				ID:       envelope.ID,
				Type:     "error",
				Message:  SanitizeError(err),
				Metadata: nil,
			})
		}
	}
}
