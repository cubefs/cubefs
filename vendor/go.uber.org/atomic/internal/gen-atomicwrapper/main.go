// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// gen-atomicwrapper generates wrapper types around other atomic types.
//
// It supports plugging in functions which convert the value inside the atomic
// type to the user-facing value. For example,
//
// Given, atomic.Value and the functions,
//
//  func packString(string) interface{}
//  func unpackString(interface{}) string
//
// We can run the following command:
//
//  gen-atomicwrapper -name String -wrapped Value \
//    -type string -pack fromString -unpack tostring
//
// This wil generate approximately,
//
//  type String struct{ v Value }
//
//  func (s *String) Load() string {
//    return unpackString(v.Load())
//  }
//
//  func (s *String) Store(s string) {
//    return s.v.Store(packString(s))
//  }
//
// The packing/unpacking logic allows the stored value to be different from
// the user-facing value.
//
// Without -pack and -unpack, the output will be cast to the target type,
// defaulting to the zero value.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"text/template"
)

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("%+v", err)
	}
}

type stringList []string

func (sl *stringList) String() string {
	return strings.Join(*sl, ",")
}

func (sl *stringList) Set(s string) error {
	for _, i := range strings.Split(s, ",") {
		*sl = append(*sl, strings.TrimSpace(i))
	}
	return nil
}

func run(args []string) error {
	var opts struct {
		Name    string
		Wrapped string
		Type    string

		Imports      stringList
		Pack, Unpack string

		CAS  bool
		Swap bool
		JSON bool

		File string
	}

	flag := flag.NewFlagSet("gen-atomicwrapper", flag.ContinueOnError)

	// Required flags
	flag.StringVar(&opts.Name, "name", "",
		"name of the generated type (e.g. Duration)")
	flag.StringVar(&opts.Wrapped, "wrapped", "",
		"name of the wrapped atomic (e.g. Int64)")
	flag.StringVar(&opts.Type, "type", "",
		"name of the type exposed by the atomic (e.g. time.Duration)")

	// Optional flags
	flag.Var(&opts.Imports, "imports",
		"comma separated list of imports to add")
	flag.StringVar(&opts.Pack, "pack", "",
		"function to transform values with before storage")
	flag.StringVar(&opts.Unpack, "unpack", "",
		"function to reverse packing on loading")
	flag.StringVar(&opts.File, "file", "",
		"output file path (default: stdout)")

	// Switches for individual methods. Underlying atomics must support
	// these.
	flag.BoolVar(&opts.CAS, "cas", false,
		"generate a `CAS(old, new) bool` method; requires -pack")
	flag.BoolVar(&opts.Swap, "swap", false,
		"generate a `Swap(new) old` method; requires -pack and -unpack")
	flag.BoolVar(&opts.JSON, "json", false,
		"generate `MarshalJSON/UnmarshJSON` methods")

	if err := flag.Parse(args); err != nil {
		return err
	}

	if len(opts.Name) == 0 || len(opts.Wrapped) == 0 || len(opts.Type) == 0 {
		return errors.New("flags -name, -wrapped, and -type are required")
	}

	if (len(opts.Pack) == 0) != (len(opts.Unpack) == 0) {
		return errors.New("either both, or neither of -pack and -unpack must be specified")
	}

	if opts.CAS && len(opts.Pack) == 0 {
		return errors.New("flag -cas requires -pack")
	}

	if opts.Swap && len(opts.Pack) == 0 {
		return errors.New("flag -swap requires -pack and -unpack")
	}

	var w io.Writer = os.Stdout
	if file := opts.File; len(file) > 0 {
		f, err := os.Create(file)
		if err != nil {
			return fmt.Errorf("create %q: %v", file, err)
		}
		defer f.Close()

		w = f
	}

	// Import encoding/json if needed.
	if opts.JSON {
		found := false
		for _, imp := range opts.Imports {
			if imp == "encoding/json" {
				found = true
				break
			}
		}

		if !found {
			opts.Imports = append(opts.Imports, "encoding/json")
		}
	}

	sort.Strings([]string(opts.Imports))

	var buff bytes.Buffer
	if err := _tmpl.Execute(&buff, opts); err != nil {
		return fmt.Errorf("render template: %v", err)
	}

	bs, err := format.Source(buff.Bytes())
	if err != nil {
		return fmt.Errorf("reformat source: %v", err)
	}

	_, err = w.Write(bs)
	return err
}

var _tmpl = template.Must(template.New("int.go").Parse(`// @generated Code generated by gen-atomicwrapper.

// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package atomic

{{ with .Imports }}
import (
	{{ range . -}}
		{{ printf "%q" . }}
	{{ end }}
)
{{ end }}

// {{ .Name }} is an atomic type-safe wrapper for {{ .Type }} values.
type {{ .Name }} struct{
	_ nocmp // disallow non-atomic comparison

	v {{ .Wrapped }}
}

var _zero{{ .Name }} {{ .Type }}


// New{{ .Name }} creates a new {{ .Name }}.
func New{{ .Name}}(v {{ .Type }}) *{{ .Name }} {
	x := &{{ .Name }}{}
	if v != _zero{{ .Name }} {
		x.Store(v)
	}
	return x
}

// Load atomically loads the wrapped {{ .Type }}.
func (x *{{ .Name }}) Load() {{ .Type }} {
	{{ if .Unpack -}}
		return {{ .Unpack }}(x.v.Load())
	{{- else -}}
		if v := x.v.Load(); v != nil {
			return v.({{ .Type }})
		}
		return _zero{{ .Name }}
	{{- end }}
}

// Store atomically stores the passed {{ .Type }}.
func (x *{{ .Name }}) Store(v {{ .Type }}) {
	{{ if .Pack -}}
		x.v.Store({{ .Pack }}(v))
	{{- else -}}
		x.v.Store(v)
	{{- end }}
}

{{ if .CAS -}}
	// CAS is an atomic compare-and-swap for {{ .Type }} values.
	func (x *{{ .Name }}) CAS(o, n {{ .Type }}) bool {
		return x.v.CAS({{ .Pack }}(o), {{ .Pack }}(n))
	}
{{- end }}

{{ if .Swap -}}
	// Swap atomically stores the given {{ .Type }} and returns the old
	// value.
	func (x *{{ .Name }}) Swap(o {{ .Type }}) {{ .Type }} {
		return {{ .Unpack }}(x.v.Swap({{ .Pack }}(o)))
	}
{{- end }}

{{ if .JSON -}}
	// MarshalJSON encodes the wrapped {{ .Type }} into JSON.
	func (x *{{ .Name }}) MarshalJSON() ([]byte, error) {
		return json.Marshal(x.Load())
	}

	// UnmarshalJSON decodes a {{ .Type }} from JSON.
	func (x *{{ .Name }}) UnmarshalJSON(b []byte) error {
		var v {{ .Type }}
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		x.Store(v)
		return nil
	}
{{- end }}

`))
