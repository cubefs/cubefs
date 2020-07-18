// Package oops adds detailed stacktraces to your Go errors.
//
// To use the oops package, calls oops.Errorf when creating a new error, and
// oops.Wrapf when returning nested errors. To access the original error, use
// oops.Cause. Each function in the callstack can add extra debugging
// information to help you track down errors.
//
// An example error (from the program below) looks as follows:
//   20 is too large!
//
//   main.Foo
//     github.com/samsarahq/go/oops/example/main.go:12
//   main.Legacy
//     github.com/samsarahq/go/oops/example/main.go:19
//   main.Bar: Legacy(20) didn't work
//     github.com/samsarahq/go/oops/example/main.go:24
//   main.Go.func1
//     github.com/samsarahq/go/oops/example/main.go:35
//
//   main.Go: goroutine had a problem
//     github.com/samsarahq/go/oops/example/main.go:38
//   main.main
//     github.com/samsarahq/go/oops/example/main.go:42
//   runtime.main
//     runtime/proc.go:185
//
// The first time oops.Errorf or oops.Wrapf is called, it captures a
// stacktrace. To keep your stacktraces as detailed as possible, it is best to
// call oops.Wrapf every time you return an error. If you have no context to
// add, you can always pass an empty format string to oops.Wrapf.
//
// When adding oops to an existing package or program, you might have
// intermediate functions that don't yet call oops.Wrapf when returning errors.
// That is no problem, as later calls to oops.Wrapf will attach their messages
// to right stackframe. However, you might as well add oops.Wrapf there as
// well!
//
// Usage:
//   package main
//
//   import (
//     "fmt"
//
//     "github.com/samsarahq/go/oops"
//   )
//
//   // Foo creates new errors using oops.Errorf.
//   func Foo(i int) error {
//     if i > 10 {
//       return oops.Errorf("%d is too large!", i)
//     }
//     return nil
//   }
//
//   // Legacy is old code that does not use oops.
//   func Legacy(i int) error {
//     return Foo(i)
//   }
//
//   // Bar wraps errors using Wrapf.
//   func Bar() error {
//     if err := Legacy(20); err != nil {
//       return oops.Wrapf(err, "Legacy(20) didn't work")
//     }
//     return nil
//   }
//
//   // Go wraps errors using Wrapf after receiving one from a channel!
//   func Go() error {
//     ch := make(chan error)
//
//     go func() {
//       ch <- Bar()
//     }()
//
//     return oops.Wrapf(<-ch, "goroutine had a problem")
//   }
//
//   func main() {
//     if err := Go(); err != nil {
//       fmt.Print(err)
//     }
//   }
package oops
