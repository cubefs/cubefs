[![Build Status](https://kmalloc.visualstudio.com/ink/_apis/build/status/kmalloc.gohook?branchName=master)](https://kmalloc.visualstudio.com/ink/_build/latest?definitionId=1&branchName=master)

## Gohook

A funny library to hook golang function dynamically at runtime, enabling functionality like patching in dynamic language.

The most significant feature this library provided that makes it distinguished from others is that it supports calling back to the original function.

Read following blogpost for further explanation of the implementation detail: [1](https://www.cnblogs.com/catch/p/10973611.html),[2](https://onedrive.live.com/View.aspx?resid=7804A3BDAEB13A9F!58083&authkey=!AKVlLS9s9KYh07s)

## How it works

The general idea of this library is that gohook will find out the address of a go function and then insert a few jump instructions to redirect execution flow to the new function.

there are a few steps to perform a hook:

1. find out the address of a function, this can be accomplished by standard reflect library.
2. inject jump code into target function, with carefully crafted binary instruction.
3. implement trampoline function to enable calling back to the original function.

It may seem risky and dangerous to perform operations like these at first glance, but this is actually common practice in c/c++ though, you can google it, search for "hot patching" something like that for more information.

## Using gohook

5 api are exported from this library, the signatures are simple as illustrated following:

1. `func Hook(target, replace, trampoline interface{}) error;`
2. `func UnHook(target interface{}) error;`
3. `func HookByIndirectJmp(target, replace, trampoline interface{});`
4. `func HookMethod(instance interface{}, method string, replace, trampoline interface{}) error;`
5. `func UnHookMethod(instance interface{}, method string) error;`

The first 3 functions are used to hook/unhook regular functions, the rest are for instance method, as the naming implies(essentially, HookMethod(obj,x,y,z) is the same as Hook(ObjType.x,y,z)).

Basically, you can just call `gohook.Hook(fmt.Printf, myPrintf, myPrintfTramp)` to hook the fmt.Printf in the standard library.

Trampolines here serves as a shadow function after the target function is hooked, think of it as a copy of the original target function.

In situation where calling back to the original function is not needed, trampoline can be passed a nil value.

HookByIndirectJmp() differs from Hook() in that it uses rdx to perform an indirect jump from a funcval, and:

1. `rdx is the context register used by compiler to access funcval.`
2. `funcval contains extra information for a closure, which is used by compiler and runtime.`

this makes it possible to hook closure function and function created by reflect.MakeFunc(), **in a less compatible way**, since the implementaion of this hook has to guess the memory layout of a reflect.Value object, which may vary from different version of runtime.

```go
package main

import (
	"fmt"
	"os"
	"github.com/kmalloc/gohook"
)

func myPrintln(a ...interface{}) (n int, err error) {
    fmt.Fprintln(os.Stdout, "before real Printfln")
    return myPrintlnTramp(a...)
}

func myPrintlnTramp(a ...interface{}) (n int, err error) {
    // a dummy function to make room for a shadow copy of the original function.
    // it doesn't matter what we do here, just to create an addressable function with adequate size.
    myPrintlnTramp(a...)
    myPrintlnTramp(a...)
    myPrintlnTramp(a...)

    for {
        fmt.Printf("hello")
    }

    return 0, nil
}

func main() {
	gohook.Hook(fmt.Println, myPrintln, myPrintlnTramp)
	fmt.Println("hello world!")
}
```

For more usage example, please refer to the example folder.

## Notes

1. 32 bit mode may not work, far jump is not handled.
2. trampoline is used to make room for the original function, it will be overwrited.
3. in case of small function which may be inlined, gohook may fail:
    - disable inlining by passig -gcflags=-l to build cmd.
4. this library is created for integrated testing, and not fully tested in production(yet), user discretion is advised.
5. escape analysis may be influenced:
   - deep copy arguments if you need to copy argument from replacement function(see func_stack_test.go).
   - escape those arguments from trampoline(by passing it to a goroutine or to other function that can escape it)
 if that argument is allocated from the replacement function.
