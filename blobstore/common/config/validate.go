// Copyright 2022 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package config

import (
	"reflect"

	"gopkg.in/go-playground/validator.v9"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterValidation("required_with_parent", requiredWithParent)
}

func requiredWithParent(fl validator.FieldLevel) bool {
	parent := fl.Parent()
	if !isZero(parent) {
		return !isZero(fl.Field())
	}
	return true
}

// isZero is a func for checking whether value is zero
func isZero(field reflect.Value) bool {
	switch field.Kind() {
	case reflect.Ptr:
		if field.IsNil() {
			return true
		}
		return isZero(field.Elem())

	case reflect.Interface, reflect.Chan, reflect.Func:
		return field.IsNil()

	case reflect.Slice, reflect.Map:
		return field.Len() == 0

	case reflect.Array:
		for i, n := 0, field.Len(); i < n; i++ {
			fl := field.Index(i)
			if !isZero(fl) {
				return false
			}
		}
		return true

	case reflect.Struct:
		for i, n := 0, field.NumField(); i < n; i++ {
			fl := field.Field(i)
			if !isZero(fl) {
				return false
			}
		}
		return true

	default:
		if !field.IsValid() {
			return true
		}
		return reflect.DeepEqual(field.Interface(), reflect.Zero(field.Type()).Interface())
	}
}
