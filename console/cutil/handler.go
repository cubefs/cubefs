/*
 * MIT License
 * Copyright (c) 2016 Samsara Networks Inc.
 * Modifications copyright 2020 The CubeFS Authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

package cutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/cubefs/cubefs/proto"
	client2 "github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/batch"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/reactive"
)

type graphqlProxyHandler struct {
	client *client2.MasterGClient
}

func NewProxyHandler(client *client2.MasterGClient) *graphqlProxyHandler {
	return &graphqlProxyHandler{client: client}
}

func Token(r *http.Request) (string, error) {
	if token := r.Header.Get(proto.HeadAuthorized); token != "" {
		return token, nil
	}

	if err := r.ParseForm(); err != nil {
		return "", err
	}

	return r.Form.Get(proto.ParamAuthorized), nil
}

type httpResponse struct {
	Data   interface{} `json:"data,omitempty"`
	Errors []string    `json:"errors,omitempty"`
}

type httpPostBody struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

func writeResponse(w http.ResponseWriter, code int, value interface{}, err error) {

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, X-Requested-With")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	response := httpResponse{}

	if err != nil {

		msg := struct {
			Messge string `json:"message"`
			Code   int    `json:"code"`
		}{
			Messge: err.Error(),
			Code:   code,
		}

		v, e := json.Marshal(msg)
		if e != nil {
			v = []byte(fmt.Sprintf("marshal has err , real err is:[%s]", err.Error()))
		}

		response.Errors = []string{string(v)}
	} else {
		response.Data = value
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}

	if _, err := w.Write(responseJSON); err != nil {
		log.LogWarnf("write reponse has err:[%s]", err.Error())
	}
}

//This code is borrowed https://github.com/samsarahq/thunder
func (h *graphqlProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		writeResponse(w, http.StatusMethodNotAllowed, nil, errors.New("request must be a POST"))
		return
	}

	if r.Body == nil {
		writeResponse(w, http.StatusBadRequest, nil, errors.New("request must include a query"))
		return
	}

	header := r.Header

	token, err := Token(r)
	if err != nil {
		writeResponse(w, http.StatusUnauthorized, nil, err)
		return
	}

	ui, err := TokenValidate(token)
	if err != nil {
		writeResponse(w, http.StatusProxyAuthRequired, nil, err)
		return
	}

	UserID := ui.User_id
	header.Set(proto.UserKey, UserID)

	rep, err := h.client.Proxy(r.Context(), r, header)
	if err != nil {
		if rep != nil {
			writeResponse(w, rep.Code, nil, err)
		} else {
			writeResponse(w, http.StatusInternalServerError, nil, err)
		}
		return
	}
	responseJSON, err := json.Marshal(rep)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	if _, err := w.Write(responseJSON); err != nil {
		log.LogWarnf("write reponse has err:[%s]", err.Error())
	}
}

type httpHandler struct {
	schema      *graphql.Schema
	middlewares []graphql.MiddlewareFunc
	executor    graphql.ExecutorRunner
}

func HTTPHandler(schema *graphql.Schema, middlewares ...graphql.MiddlewareFunc) http.Handler {
	return HTTPHandlerWithExecutor(schema, (graphql.NewExecutor(graphql.NewImmediateGoroutineScheduler())), middlewares...)
}

func HTTPHandlerWithExecutor(schema *graphql.Schema, executor graphql.ExecutorRunner, middlewares ...graphql.MiddlewareFunc) http.Handler {
	return &httpHandler{
		schema:      schema,
		middlewares: middlewares,
		executor:    executor,
	}
}

func (h *httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		writeResponse(w, http.StatusMethodNotAllowed, nil, errors.New("request must be a POST"))
		return
	}

	if r.Body == nil {
		writeResponse(w, http.StatusBadRequest, nil, errors.New("request must include a query"))
		return
	}

	ctx := r.Context()

	if r.RequestURI != "/login" {
		token, err := Token(r)
		if err != nil {
			writeResponse(w, http.StatusUnauthorized, nil, err)
			return
		}

		ui, err := TokenValidate(token)
		if err != nil {
			writeResponse(w, http.StatusProxyAuthRequired, nil, err)
			return
		}

		ctx = context.WithValue(ctx, proto.UserKey, ui.User_id)
		ctx = context.WithValue(ctx, proto.UserInfoKey, ui)
	}

	if r.RequestURI == "/monitor" {
		value := ctx.Value(proto.UserInfoKey)
		if value == nil {
			writeResponse(w, http.StatusProxyAuthRequired, nil, fmt.Errorf("user not found"))
		}

		ui := value.(*proto.UserInfo)
		if ui.UserType != proto.UserTypeRoot && ui.UserType != proto.UserTypeAdmin {
			writeResponse(w, http.StatusProxyAuthRequired, nil, fmt.Errorf("no access visit monitor"))
		}
	}

	var params httpPostBody
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		writeResponse(w, http.StatusInternalServerError, nil, err)
		return
	}

	query, err := graphql.Parse(params.Query, params.Variables)
	if err != nil {
		writeResponse(w, http.StatusInternalServerError, nil, err)
		return
	}

	schema := h.schema.Query
	if query.Kind == "mutation" {
		schema = h.schema.Mutation
	}
	if err := graphql.PrepareQuery(ctx, schema, query.SelectionSet); err != nil {
		writeResponse(w, http.StatusInternalServerError, nil, err)
		return
	}

	var wg sync.WaitGroup
	e := h.executor

	wg.Add(1)
	runner := reactive.NewRerunner(ctx, func(ctx context.Context) (interface{}, error) {
		defer wg.Done()

		ctx = batch.WithBatching(ctx)

		var middlewares []graphql.MiddlewareFunc
		middlewares = append(middlewares, h.middlewares...)
		middlewares = append(middlewares, func(input *graphql.ComputationInput, next graphql.MiddlewareNextFunc) *graphql.ComputationOutput {
			output := next(input)
			output.Current, output.Error = e.Execute(input.Ctx, schema, nil, input.ParsedQuery)
			return output
		})

		output := graphql.RunMiddlewares(middlewares, &graphql.ComputationInput{
			Ctx:         ctx,
			ParsedQuery: query,
			Query:       params.Query,
			Variables:   params.Variables,
		})
		current, err := output.Current, output.Error

		if err != nil {
			if graphql.ErrorCause(err) == context.Canceled {
				return nil, err
			}
			writeResponse(w, http.StatusBadRequest, nil, err)
			return nil, err
		}

		writeResponse(w, http.StatusOK, current, nil)
		return nil, nil
	}, graphql.DefaultMinRerunInterval, false)

	wg.Wait()
	runner.Stop()
}

func parseResponseNames(r *http.Request) ([]string, error) {
	var params httpPostBody
	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		return nil, err
	}

	query, err := graphql.Parse(params.Query, params.Variables)
	if err != nil {
		return nil, err
	}

	list := make([]string, 0)

	for _, s := range query.Selections {
		list = append(list, s.Name)
	}

	return list, nil

}

func IQLFun(writer http.ResponseWriter, request *http.Request) {
	writer.Write([]byte(`<!DOCTYPE html>

<html>

<head>
  <meta charset=utf-8 />
  <meta name="viewport" content="user-scalable=no, initial-scale=1.0, minimum-scale=1.0, maximum-scale=1.0, minimal-ui">
  <title>GraphQL Playground</title>
  <link rel="stylesheet" href="//cdn.jsdelivr.net/npm/graphql-playground-react/build/static/css/index.css" />
  <link rel="shortcut icon" href="//cdn.jsdelivr.net/npm/graphql-playground-react/build/favicon.png" />
  <script src="//cdn.jsdelivr.net/npm/graphql-playground-react/build/static/js/middleware.js"></script>

</head>

<body>
  <style type="text/css">
    html {
      font-family: "Open Sans", sans-serif;
      overflow: hidden;
    }

    body {
      margin: 0;
      background: #172a3a;
    }

    .playgroundIn {
      -webkit-animation: playgroundIn 0.5s ease-out forwards;
      animation: playgroundIn 0.5s ease-out forwards;
    }

    @-webkit-keyframes playgroundIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(10px);
        -ms-transform: translateY(10px);
        transform: translateY(10px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }

    @keyframes playgroundIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(10px);
        -ms-transform: translateY(10px);
        transform: translateY(10px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }
  </style>

  <style type="text/css">
    .fadeOut {
      -webkit-animation: fadeOut 0.5s ease-out forwards;
      animation: fadeOut 0.5s ease-out forwards;
    }

    @-webkit-keyframes fadeIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(-10px);
        -ms-transform: translateY(-10px);
        transform: translateY(-10px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }

    @keyframes fadeIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(-10px);
        -ms-transform: translateY(-10px);
        transform: translateY(-10px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }

    @-webkit-keyframes fadeOut {
      from {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
      to {
        opacity: 0;
        -webkit-transform: translateY(-10px);
        -ms-transform: translateY(-10px);
        transform: translateY(-10px);
      }
    }

    @keyframes fadeOut {
      from {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
      to {
        opacity: 0;
        -webkit-transform: translateY(-10px);
        -ms-transform: translateY(-10px);
        transform: translateY(-10px);
      }
    }

    @-webkit-keyframes appearIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(0px);
        -ms-transform: translateY(0px);
        transform: translateY(0px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }

    @keyframes appearIn {
      from {
        opacity: 0;
        -webkit-transform: translateY(0px);
        -ms-transform: translateY(0px);
        transform: translateY(0px);
      }
      to {
        opacity: 1;
        -webkit-transform: translateY(0);
        -ms-transform: translateY(0);
        transform: translateY(0);
      }
    }

    @-webkit-keyframes scaleIn {
      from {
        -webkit-transform: scale(0);
        -ms-transform: scale(0);
        transform: scale(0);
      }
      to {
        -webkit-transform: scale(1);
        -ms-transform: scale(1);
        transform: scale(1);
      }
    }

    @keyframes scaleIn {
      from {
        -webkit-transform: scale(0);
        -ms-transform: scale(0);
        transform: scale(0);
      }
      to {
        -webkit-transform: scale(1);
        -ms-transform: scale(1);
        transform: scale(1);
      }
    }

    @-webkit-keyframes innerDrawIn {
      0% {
        stroke-dashoffset: 70;
      }
      50% {
        stroke-dashoffset: 140;
      }
      100% {
        stroke-dashoffset: 210;
      }
    }

    @keyframes innerDrawIn {
      0% {
        stroke-dashoffset: 70;
      }
      50% {
        stroke-dashoffset: 140;
      }
      100% {
        stroke-dashoffset: 210;
      }
    }

    @-webkit-keyframes outerDrawIn {
      0% {
        stroke-dashoffset: 76;
      }
      100% {
        stroke-dashoffset: 152;
      }
    }

    @keyframes outerDrawIn {
      0% {
        stroke-dashoffset: 76;
      }
      100% {
        stroke-dashoffset: 152;
      }
    }

    .hHWjkv {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.2222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.2222222222222222s;
    }

    .gCDOzd {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.4222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.4222222222222222s;
    }

    .hmCcxi {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.6222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.6222222222222222s;
    }

    .eHamQi {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.8222222222222223s;
      animation: scaleIn 0.25s linear forwards 0.8222222222222223s;
    }

    .byhgGu {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 1.0222222222222221s;
      animation: scaleIn 0.25s linear forwards 1.0222222222222221s;
    }

    .llAKP {
      -webkit-transform-origin: 0px 0px;
      -ms-transform-origin: 0px 0px;
      transform-origin: 0px 0px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 1.2222222222222223s;
      animation: scaleIn 0.25s linear forwards 1.2222222222222223s;
    }

    .bglIGM {
      -webkit-transform-origin: 64px 28px;
      -ms-transform-origin: 64px 28px;
      transform-origin: 64px 28px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.2222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.2222222222222222s;
    }

    .ksxRII {
      -webkit-transform-origin: 95.98500061035156px 46.510000228881836px;
      -ms-transform-origin: 95.98500061035156px 46.510000228881836px;
      transform-origin: 95.98500061035156px 46.510000228881836px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.4222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.4222222222222222s;
    }

    .cWrBmb {
      -webkit-transform-origin: 95.97162628173828px 83.4900016784668px;
      -ms-transform-origin: 95.97162628173828px 83.4900016784668px;
      transform-origin: 95.97162628173828px 83.4900016784668px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.6222222222222222s;
      animation: scaleIn 0.25s linear forwards 0.6222222222222222s;
    }

    .Wnusb {
      -webkit-transform-origin: 64px 101.97999572753906px;
      -ms-transform-origin: 64px 101.97999572753906px;
      transform-origin: 64px 101.97999572753906px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 0.8222222222222223s;
      animation: scaleIn 0.25s linear forwards 0.8222222222222223s;
    }

    .bfPqf {
      -webkit-transform-origin: 32.03982162475586px 83.4900016784668px;
      -ms-transform-origin: 32.03982162475586px 83.4900016784668px;
      transform-origin: 32.03982162475586px 83.4900016784668px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 1.0222222222222221s;
      animation: scaleIn 0.25s linear forwards 1.0222222222222221s;
    }

    .edRCTN {
      -webkit-transform-origin: 32.033552169799805px 46.510000228881836px;
      -ms-transform-origin: 32.033552169799805px 46.510000228881836px;
      transform-origin: 32.033552169799805px 46.510000228881836px;
      -webkit-transform: scale(0);
      -ms-transform: scale(0);
      transform: scale(0);
      -webkit-animation: scaleIn 0.25s linear forwards 1.2222222222222223s;
      animation: scaleIn 0.25s linear forwards 1.2222222222222223s;
    }

    .iEGVWn {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 0.3333333333333333s, appearIn 0.1s ease-out forwards 0.3333333333333333s;
      animation: outerDrawIn 0.5s ease-out forwards 0.3333333333333333s, appearIn 0.1s ease-out forwards 0.3333333333333333s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .bsocdx {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 0.5333333333333333s, appearIn 0.1s ease-out forwards 0.5333333333333333s;
      animation: outerDrawIn 0.5s ease-out forwards 0.5333333333333333s, appearIn 0.1s ease-out forwards 0.5333333333333333s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .jAZXmP {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 0.7333333333333334s, appearIn 0.1s ease-out forwards 0.7333333333333334s;
      animation: outerDrawIn 0.5s ease-out forwards 0.7333333333333334s, appearIn 0.1s ease-out forwards 0.7333333333333334s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .hSeArx {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 0.9333333333333333s, appearIn 0.1s ease-out forwards 0.9333333333333333s;
      animation: outerDrawIn 0.5s ease-out forwards 0.9333333333333333s, appearIn 0.1s ease-out forwards 0.9333333333333333s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .bVgqGk {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 1.1333333333333333s, appearIn 0.1s ease-out forwards 1.1333333333333333s;
      animation: outerDrawIn 0.5s ease-out forwards 1.1333333333333333s, appearIn 0.1s ease-out forwards 1.1333333333333333s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .hEFqBt {
      opacity: 0;
      stroke-dasharray: 76;
      -webkit-animation: outerDrawIn 0.5s ease-out forwards 1.3333333333333333s, appearIn 0.1s ease-out forwards 1.3333333333333333s;
      animation: outerDrawIn 0.5s ease-out forwards 1.3333333333333333s, appearIn 0.1s ease-out forwards 1.3333333333333333s;
      -webkit-animation-iteration-count: 1, 1;
      animation-iteration-count: 1, 1;
    }

    .dzEKCM {
      opacity: 0;
      stroke-dasharray: 70;
      -webkit-animation: innerDrawIn 1s ease-in-out forwards 1.3666666666666667s, appearIn 0.1s linear forwards 1.3666666666666667s;
      animation: innerDrawIn 1s ease-in-out forwards 1.3666666666666667s, appearIn 0.1s linear forwards 1.3666666666666667s;
      -webkit-animation-iteration-count: infinite, 1;
      animation-iteration-count: infinite, 1;
    }

    .DYnPx {
      opacity: 0;
      stroke-dasharray: 70;
      -webkit-animation: innerDrawIn 1s ease-in-out forwards 1.5333333333333332s, appearIn 0.1s linear forwards 1.5333333333333332s;
      animation: innerDrawIn 1s ease-in-out forwards 1.5333333333333332s, appearIn 0.1s linear forwards 1.5333333333333332s;
      -webkit-animation-iteration-count: infinite, 1;
      animation-iteration-count: infinite, 1;
    }

    .hjPEAQ {
      opacity: 0;
      stroke-dasharray: 70;
      -webkit-animation: innerDrawIn 1s ease-in-out forwards 1.7000000000000002s, appearIn 0.1s linear forwards 1.7000000000000002s;
      animation: innerDrawIn 1s ease-in-out forwards 1.7000000000000002s, appearIn 0.1s linear forwards 1.7000000000000002s;
      -webkit-animation-iteration-count: infinite, 1;
      animation-iteration-count: infinite, 1;
    }

    #loading-wrapper {
      position: absolute;
      width: 100vw;
      height: 100vh;
      display: -webkit-box;
      display: -webkit-flex;
      display: -ms-flexbox;
      display: flex;
      -webkit-align-items: center;
      -webkit-box-align: center;
      -ms-flex-align: center;
      align-items: center;
      -webkit-box-pack: center;
      -webkit-justify-content: center;
      -ms-flex-pack: center;
      justify-content: center;
      -webkit-flex-direction: column;
      -ms-flex-direction: column;
      flex-direction: column;
    }

    .logo {
      width: 75px;
      height: 75px;
      margin-bottom: 20px;
      opacity: 0;
      -webkit-animation: fadeIn 0.5s ease-out forwards;
      animation: fadeIn 0.5s ease-out forwards;
    }

    .text {
      font-size: 32px;
      font-weight: 200;
      text-align: center;
      color: rgba(255, 255, 255, 0.6);
      opacity: 0;
      -webkit-animation: fadeIn 0.5s ease-out forwards;
      animation: fadeIn 0.5s ease-out forwards;
    }

    .dGfHfc {
      font-weight: 400;
    }
  </style>
  <div id="loading-wrapper">
    <svg class="logo" viewBox="0 0 128 128" xmlns:xlink="http://www.w3.org/1999/xlink">
      <title>GraphQL Playground Logo</title>
      <defs>
        <linearGradient id="linearGradient-1" x1="4.86%" x2="96.21%" y1="0%" y2="99.66%">
          <stop stop-color="#E00082" stop-opacity=".8" offset="0%"></stop>
          <stop stop-color="#E00082" offset="100%"></stop>
        </linearGradient>
      </defs>
      <g>
        <rect id="Gradient" width="127.96" height="127.96" y="1" fill="url(#linearGradient-1)" rx="4"></rect>
        <path id="Border" fill="#E00082" fill-rule="nonzero" d="M4.7 2.84c-1.58 0-2.86 1.28-2.86 2.85v116.57c0 1.57 1.28 2.84 2.85 2.84h116.57c1.57 0 2.84-1.26 2.84-2.83V5.67c0-1.55-1.26-2.83-2.83-2.83H4.67zM4.7 0h116.58c3.14 0 5.68 2.55 5.68 5.7v116.58c0 3.14-2.54 5.68-5.68 5.68H4.68c-3.13 0-5.68-2.54-5.68-5.68V5.68C-1 2.56 1.55 0 4.7 0z"></path>
        <path class="bglIGM" x="64" y="28" fill="#fff" d="M64 36c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8" style="transform: translate(100px, 100px);"></path>
        <path class="ksxRII" x="95.98500061035156" y="46.510000228881836" fill="#fff" d="M89.04 50.52c-2.2-3.84-.9-8.73 2.94-10.96 3.83-2.2 8.72-.9 10.95 2.94 2.2 3.84.9 8.73-2.94 10.96-3.85 2.2-8.76.9-10.97-2.94"
          style="transform: translate(100px, 100px);"></path>
        <path class="cWrBmb" x="95.97162628173828" y="83.4900016784668" fill="#fff" d="M102.9 87.5c-2.2 3.84-7.1 5.15-10.94 2.94-3.84-2.2-5.14-7.12-2.94-10.96 2.2-3.84 7.12-5.15 10.95-2.94 3.86 2.23 5.16 7.12 2.94 10.96"
          style="transform: translate(100px, 100px);"></path>
        <path class="Wnusb" x="64" y="101.97999572753906" fill="#fff" d="M64 110c-4.43 0-8-3.6-8-8.02 0-4.44 3.57-8.02 8-8.02s8 3.58 8 8.02c0 4.4-3.57 8.02-8 8.02"
          style="transform: translate(100px, 100px);"></path>
        <path class="bfPqf" x="32.03982162475586" y="83.4900016784668" fill="#fff" d="M25.1 87.5c-2.2-3.84-.9-8.73 2.93-10.96 3.83-2.2 8.72-.9 10.95 2.94 2.2 3.84.9 8.73-2.94 10.96-3.85 2.2-8.74.9-10.95-2.94"
          style="transform: translate(100px, 100px);"></path>
        <path class="edRCTN" x="32.033552169799805" y="46.510000228881836" fill="#fff" d="M38.96 50.52c-2.2 3.84-7.12 5.15-10.95 2.94-3.82-2.2-5.12-7.12-2.92-10.96 2.2-3.84 7.12-5.15 10.95-2.94 3.83 2.23 5.14 7.12 2.94 10.96"
          style="transform: translate(100px, 100px);"></path>
        <path class="iEGVWn" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M63.55 27.5l32.9 19-32.9-19z"></path>
        <path class="bsocdx" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M96 46v38-38z"></path>
        <path class="jAZXmP" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M96.45 84.5l-32.9 19 32.9-19z"></path>
        <path class="hSeArx" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M64.45 103.5l-32.9-19 32.9 19z"></path>
        <path class="bVgqGk" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M32 84V46v38z"></path>
        <path class="hEFqBt" stroke="#fff" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" d="M31.55 46.5l32.9-19-32.9 19z"></path>
        <path class="dzEKCM" id="Triangle-Bottom" stroke="#fff" stroke-width="4" d="M30 84h70" stroke-linecap="round"></path>
        <path class="DYnPx" id="Triangle-Left" stroke="#fff" stroke-width="4" d="M65 26L30 87" stroke-linecap="round"></path>
        <path class="hjPEAQ" id="Triangle-Right" stroke="#fff" stroke-width="4" d="M98 87L63 26" stroke-linecap="round"></path>
      </g>
    </svg>
    <div class="text">Loading
      <span class="dGfHfc">GraphQL Playground</span>
    </div>
  </div>

  <div id="root" />
  <script type="text/javascript">
    window.addEventListener('load', function (event) {

      const loadingWrapper = document.getElementById('loading-wrapper');
      loadingWrapper.classList.add('fadeOut');


      const root = document.getElementById('root');
      root.classList.add('playgroundIn');

      GraphQLPlayground.init(root, { endpoint: "/", subscriptionEndpoint: "" })
    })
  </script>
</body>
</html>`))
}
