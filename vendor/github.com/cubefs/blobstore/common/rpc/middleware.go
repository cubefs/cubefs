package rpc

import "net/http"

// MiddlewareHandler middleware above rpc server default router.
// Run sorted by progress handler order.
func MiddlewareHandler(phs ...ProgressHandler) http.Handler {
	DefaultRouter.hasMiddleware = true
	phs = append(DefaultRouter.headMiddlewares, phs...)
	return buildHTTPHandler(DefaultRouter.ServeHTTP, phs...)
}

// MiddlewareHandlerFunc middleware func above rpc server default router.
// Run sorted by progress handler order.
func MiddlewareHandlerFunc(phs ...ProgressHandler) http.HandlerFunc {
	DefaultRouter.hasMiddleware = true
	phs = append(DefaultRouter.headMiddlewares, phs...)
	return buildHTTPHandler(DefaultRouter.ServeHTTP, phs...)
}

// MiddlewareHandlerWith middleware above rpc server router
// Run sorted by progress handler order.
func MiddlewareHandlerWith(r *Router, phs ...ProgressHandler) http.Handler {
	r.hasMiddleware = true
	phs = append(r.headMiddlewares, phs...)
	return buildHTTPHandler(r.ServeHTTP, phs...)
}

// MiddlewareHandlerFuncWith middleware func above rpc server router
// Run sorted by progress handler order.
func MiddlewareHandlerFuncWith(r *Router, phs ...ProgressHandler) http.HandlerFunc {
	r.hasMiddleware = true
	phs = append(r.headMiddlewares, phs...)
	return buildHTTPHandler(r.ServeHTTP, phs...)
}

func buildHTTPHandler(h http.HandlerFunc, phs ...ProgressHandler) http.HandlerFunc {
	if len(phs) == 0 {
		return h
	}

	last := len(phs) - 1
	return buildHTTPHandler(func(w http.ResponseWriter, req *http.Request) {
		phs[last].Handler(w, req, h)
	}, phs[:last]...)
}
