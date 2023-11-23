/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 * Modifications copyright 2019 The CubeFS Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package objectnode

// https://docs.aws.amazon.com/AmazonS3/latest/dev/access-policy-language-overview.html

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cubefs/cubefs/util/log"
)

var (
	invalidCIDR                 = "value %v must be CIDR string for %v condition"
	invalidIPKey                = "only %v key is allowed for %v condition"
	invalidStringValue          = "value must be a string for %v condition"
	invalidConditionKey         = "invalid condition key '%v'"
	emptyCondition              = "condition must not be empty"
	cannotHandledCondition      = "condition %v  cannot be handled"
	cannotTransformToString     = "%v cannot transform to string"
	cannotHandledConditionValue = "cannot handle condition values: %v"
	invalidConditionOperator    = "invalid condition operator '%v'"
	invalidConditionValue       = "invalid value"
	duplicateConditionValue     = "duplicate value found '%v'"
	unknownJsonData             = "unknown json data '%v'"
)

type operator string

const (
	stringLike    = "StringLike"
	stringNotLike = "StringNotLike"
	ipAddress     = "IpAddress"
	notIPAddress  = "NotIpAddress"
)

var supportedOperators = []operator{
	stringLike,
	stringNotLike,
	ipAddress,
	notIPAddress,
	// Add new conditions here.
}

func (op operator) IsValid() bool {
	for _, supOp := range supportedOperators {
		if op == supOp {
			return true
		}
	}

	return false
}

// encodes operator to JSON data.
func (op operator) MarshalJSON() ([]byte, error) {
	if !op.IsValid() {
		return nil, fmt.Errorf("invalid operator %v", op)
	}

	return json.Marshal(string(op))
}

// decodes JSON data to condition operator.
func (op *operator) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	parsedOperator, err := parseOperator(s)
	if err != nil {
		return err
	}

	*op = parsedOperator
	return nil
}

func parseOperator(s string) (operator, error) {
	n := operator(s)

	if n.IsValid() {
		return n, nil
	}

	return n, fmt.Errorf(invalidConditionOperator, s)
}

type Operation interface {
	// evaluates this condition operation with given values.
	evaluate(values map[string]string) bool

	// returns all condition keys used in this operation.
	keys() KeySet

	// returns condition operator of this operation.
	operator() operator

	toMap() map[Key]ValueSet
}

type Condition []Operation

// evaluates all operation with given values map.
func (operations Condition) Evaluate(values map[string]string) bool {
	for _, op := range operations {
		if !op.evaluate(values) {
			log.LogDebugf("cannot match a condition, operator:%v, conditionKV:%v, values: %v", op.operator(), op.toMap(), values)
			return false
		}
	}
	return true
}

// returns list of keys used in all operation.
func (operations Condition) Keys() KeySet {
	keySet := NewKeySet()

	for _, op := range operations {
		keySet.AddAll(op.keys())
	}

	return keySet
}

// encodes Condition to  JSON data.
func (operations Condition) MarshalJSON() ([]byte, error) {
	conditionKV := make(map[operator]map[Key]ValueSet)

	for _, op := range operations {
		if _, ok := conditionKV[op.operator()]; ok {
			for k, v := range op.toMap() {
				conditionKV[op.operator()][k] = v
			}
		} else {
			conditionKV[op.operator()] = op.toMap()
		}
	}

	return json.Marshal(conditionKV)
}

func (operations Condition) String() string {
	var opStrings []string
	for _, op := range operations {
		s := fmt.Sprintf("%v", op)
		opStrings = append(opStrings, s)
	}
	sort.Strings(opStrings)

	return fmt.Sprintf("%v", opStrings)
}

var conditionOpMap = map[operator]func(map[Key]ValueSet) (Operation, error){
	stringLike:    newStringLikeOp,
	stringNotLike: newStringNotLikeOp,
	ipAddress:     newIPAddressOp,
	notIPAddress:  newNotIPAddressOp,
	// Add new conditions here.
}

// decodes JSON data to Condition.
func (operations *Condition) UnmarshalJSON(data []byte) error {
	operatorMap := make(map[string]map[string]ValueSet)
	if err := json.Unmarshal(data, &operatorMap); err != nil {
		return err
	}

	if len(operatorMap) == 0 {
		return fmt.Errorf(emptyCondition)
	}

	var ops []Operation
	for operatorString, args := range operatorMap {
		o, err := parseOperator(operatorString)
		if err != nil {
			return err
		}

		vfn, ok := conditionOpMap[o]
		if !ok {
			return fmt.Errorf(cannotHandledCondition, o)
		}
		m := make(map[Key]ValueSet)
		for keyString, values := range args {
			key, err := parseKey(keyString)
			if err != nil {
				return err
			}
			m[key] = values

		}
		op, err := vfn(m)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}

	*operations = ops

	return nil
}

func (operations Condition) CheckValid() error {
	for _, op := range operations {
		if !op.operator().IsValid() {
			return NewError("InvalidConditionType", fmt.Sprintf("policy has invalid condition type: %s", op.operator()), 400)
		}
		for key := range op.keys() {
			if !key.IsValid() {
				return NewError("InvalidConditionKey", fmt.Sprintf("policy has invalid condition key: %s", key), 400)
			}
		}
	}
	return nil
}

// parse a map into Condition
func (operations *Condition) parseOperations(operatorMap map[string]map[string]interface{}) error {
	var ops []Operation
	for operatorString, args := range operatorMap {
		o, err := parseOperator(operatorString)
		if err != nil {
			return err
		}

		vfn, ok := conditionOpMap[o]
		if !ok {
			return fmt.Errorf(cannotHandledCondition, o)
		}
		m := make(map[Key]ValueSet)
		for keyString, values := range args {

			valueSet := NewValueSet()
			key, err := parseKey(keyString)
			if err != nil {
				return err
			}
			switch values.(type) {
			case string:
				valueSet.Add(NewStringValue(values.(string)))
			case []interface{}:
				for _, value := range values.([]interface{}) {
					if valueString, ok := value.(string); ok {
						valueSet.Add(NewStringValue(valueString))
					} else {
						return fmt.Errorf(cannotTransformToString, value)
					}
				}
			default:

				return fmt.Errorf(cannotHandledConditionValue, values)
			}
			m[key] = valueSet

		}
		op, err := vfn(m)
		if err != nil {
			return err
		}
		ops = append(ops, op)
	}

	*operations = ops
	return nil
}
