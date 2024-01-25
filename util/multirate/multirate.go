package multirate

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// BASIC CONCEPTS:
// Event     An event can be a request or task. An event has many properties and statistic.
// Property  Such as volume, opcode, disk, partition, etc.
// Statistic Such as event count, network flowing in bytes, network flowing out bytes, each of
//           them can has three dimensions: total, for each disk, for each partition.
// Rule      A rule is a combination of properties and rate limits. Each property in a rule has
//           three states: not exists, has a specific value, has a default value.
//           Example: for each volume and opcode read, the total request rate limit is 100, the
//           request rate limit for each disk is 10. In this rule, property volume has a default
//           value, property opcode has a specific value, property disk doesn't exist.
//           For an event which has n properties, there are 3^n possible rules.
//
//
// HTTP API: /multirate/get
//           /multirate/set?status=(1 on | 0 off | -1 clear all rules and limiters)
//

type PropertyType int
type propertyState int
type statType int

const (
	PropertyTypeVol PropertyType = iota
	PropertyTypeOp
	PropertyTypeDisk
	PropertyTypePartition
	PropertyTypeFlow
	propertyTypeMax // count of property types
)

const (
	propertyStateNotExist propertyState = iota
	propertyStateHasName
	propertyStateDefaultName
)

const (
	statTypeCount statType = iota
	statTypeInBytes
	statTypeOutBytes
	statTypeMax // count of stat types
)

const (
	FlowNetwork = "network"
	FlowDisk    = "disk"

	// don't use separator which may be used as volume name, such as _.-
	separator = ":"

	TimeoutUseDefault = 0
	TimeoutNotWait    = -1
	TimeoutWait       = -2

	defaultTimeout = time.Second

	controlSetStatus = "/multirate/set"
	controlGet       = "/multirate/get"
	statusKey        = "status"
)

type Property struct {
	Type  PropertyType
	Value string
}
type Properties []Property

type PropertiesBuilder struct {
	properties Properties
}

func NewPropertiesBuilder() *PropertiesBuilder {
	return &PropertiesBuilder{
		properties: Properties{},
	}
}

func (s *PropertiesBuilder) SetVol(v string) *PropertiesBuilder {
	s.properties = append(s.properties, Property{Type: PropertyTypeVol, Value: v})
	return s
}

func (s *PropertiesBuilder) SetDisk(v string) *PropertiesBuilder {
	s.properties = append(s.properties, Property{Type: PropertyTypeDisk, Value: v})
	return s
}

func (s *PropertiesBuilder) SetOp(v string) *PropertiesBuilder {
	s.properties = append(s.properties, Property{Type: PropertyTypeOp, Value: v})
	return s
}

func (s *PropertiesBuilder) SetPartition(v string) *PropertiesBuilder {
	s.properties = append(s.properties, Property{Type: PropertyTypePartition, Value: v})
	return s
}

func (s *PropertiesBuilder) SetBandType(v string) *PropertiesBuilder {
	s.properties = append(s.properties, Property{Type: PropertyTypeFlow, Value: v})
	return s
}

func (s *PropertiesBuilder) Properties() Properties {
	return s.properties
}

type LimiterWithTimeout struct {
	limiter *rate.Limiter
	timeout time.Duration
}
type LimiterGroup [statTypeMax]LimiterWithTimeout
type LimitGroup [statTypeMax]rate.Limit
type BurstGroup [statTypeMax]int

type Rule struct {
	properties   Properties
	defaultCount int
	state        [propertyTypeMax]propertyState
	limit        LimitGroup
	burst        BurstGroup
	timeout      time.Duration
}

type Stat struct {
	Count    int
	InBytes  int
	OutBytes int
}

type StatBuilder struct {
	stat Stat
}

func NewStatBuilder() *StatBuilder {
	return &StatBuilder{
		stat: Stat{},
	}
}

func (s *StatBuilder) SetCount(count int) *StatBuilder {
	s.stat.Count = count
	return s
}

func (s *StatBuilder) SetInBytes(in int) *StatBuilder {
	s.stat.InBytes = in
	return s
}

func (s *StatBuilder) SetOutBytes(out int) *StatBuilder {
	s.stat.OutBytes = out
	return s
}

func (s *StatBuilder) Stat() Stat {
	return s.stat
}

type MultiLimiter struct {
	status   bool
	rules    sync.Map // map[name]*Rule
	limiters sync.Map // map[name]LimiterGroup
}

func (t PropertyType) String() string {
	switch t {
	case PropertyTypeVol:
		return "Volume"
	case PropertyTypeOp:
		return "Opcode"
	case PropertyTypeDisk:
		return "Disk"
	case PropertyTypePartition:
		return "Partition"
	case PropertyTypeFlow:
		return "Flow"
	default:
		return fmt.Sprintf("Unkown(%v)", int(t))
	}
}

func (t statType) String() string {
	switch t {
	case statTypeCount:
		return "Count"
	case statTypeInBytes:
		return "InBytes"
	case statTypeOutBytes:
		return "OutBytes"
	default:
		return fmt.Sprintf("Unkown(%v)", int(t))
	}
}

func (p Property) String() string {
	return fmt.Sprintf("{Type:%v, Value:%v}", p.Type, p.Value)
}

func (p Property) name() string {
	// PropertyType may have a customed String(), p.Type should use placeholder %d instead of %v here
	return fmt.Sprintf("%d%v", p.Type, p.Value)
}

func (ps Properties) sort() {
	sort.Slice(ps, func(i, j int) bool {
		return ps[i].Type < ps[j].Type
	})
}

func (ps Properties) defaultNameCount() int {
	count := 0
	for _, p := range ps {
		if p.Value == "" {
			count++
		}
	}
	return count
}

func (ps Properties) name() string {
	var sb strings.Builder
	for _, p := range ps {
		sb.WriteString(strconv.Itoa(int(p.Type)))
		sb.WriteString(p.Value)
		sb.WriteString(separator)
	}
	return strings.TrimSuffix(sb.String(), separator)
}

func (ps Properties) match(name string) bool {
	var strs []string
	for _, p := range ps {
		strs = append(strs, p.name())
	}
	names := strings.Split(name, separator)
	if len(names) != len(strs) {
		return false
	}
	for i := 0; i < len(strs); i++ {
		// if a property has an empty name, its name len is 1
		if strs[i] != names[i] && len(strs[i]) > 1 {
			return false
		}
	}
	return true
}

func (ps Properties) state() (re [propertyTypeMax]propertyState) {
	for _, p := range ps {
		if p.Value == "" {
			re[p.Type] = propertyStateDefaultName
		} else {
			re[p.Type] = propertyStateHasName
		}
	}
	return
}

func (s Stat) index() (re [statTypeMax]bool) {
	if s.Count > 0 {
		re[statTypeCount] = true
	}
	if s.InBytes > 0 {
		re[statTypeInBytes] = true
	}
	if s.OutBytes > 0 {
		re[statTypeOutBytes] = true
	}
	return re
}

func (s Stat) val(t statType) (re int) {
	if t == statTypeCount {
		re = s.Count
	} else if t == statTypeInBytes {
		re = s.InBytes
	} else if t == statTypeOutBytes {
		re = s.OutBytes
	}
	return
}

func (g LimitGroup) haveLimit() bool {
	for _, l := range g {
		if l > 0 {
			return true
		}
	}
	return false
}

func (g LimiterGroup) String() string {
	var strs []string
	for i, l := range g {
		if l.limiter != nil {
			strs = append(strs, fmt.Sprintf("type(%v) addr(%p) limit(%v) timeout(%v)", statType(i), l.limiter, l.limiter.Limit(), l.timeout))
		}
	}
	return strings.Join(strs, ", ")
}

func NewRule(ps Properties, limit LimitGroup, burst BurstGroup) *Rule {
	r := new(Rule)
	r.properties = ps
	r.properties.sort()
	r.defaultCount = r.properties.defaultNameCount()
	r.state = r.properties.state()
	r.limit = limit
	r.burst = burst
	return r
}

func NewRuleWithTimeout(ps Properties, limit LimitGroup, burst BurstGroup, timeout time.Duration) *Rule {
	rule := NewRule(ps, limit, burst)
	rule.timeout = timeout
	return rule
}

func (r *Rule) String() string {
	return fmt.Sprintf("properties:%v, limit:%v, burst:%v, timeout:%v", r.properties, r.limit, r.burst, r.timeout)
}

func (r *Rule) match(ps Properties) (bool, string) {
	var eventPs [propertyTypeMax]string
	for _, p := range ps {
		if p.Type >= propertyTypeMax {
			continue
		}
		eventPs[p.Type] = p.Value
	}

	// if the rule has a property and the event doesn't have it, then not match
	for i, state := range r.state {
		if state != propertyStateNotExist && eventPs[i] == "" {
			return false, ""
		}
	}

	var sb strings.Builder
	isMatch := true
	for i, p := range eventPs {
		if r.state[i] != propertyStateNotExist {
			sb.WriteString(strconv.Itoa(i))
			sb.WriteString(p)
			sb.WriteString(separator)
		}
	}
	for _, p := range r.properties {
		if p.Value != eventPs[p.Type] && len(p.Value) > 0 {
			isMatch = false
		}
	}
	return isMatch, strings.TrimSuffix(sb.String(), separator)
}

func NewMultiLimiter() *MultiLimiter {
	ml := new(MultiLimiter)
	ml.status = true
	return ml
}

func NewMultiLimiterWithHandler() *MultiLimiter {
	ml := NewMultiLimiter()
	http.HandleFunc(controlGet, ml.handlerGet)
	http.HandleFunc(controlSetStatus, ml.handlerSetStatus)
	return ml
}

func (ml *MultiLimiter) String() string {
	var builder strings.Builder
	builder.WriteString("rules: ")
	ml.rules.Range(func(k, v interface{}) bool {
		rule := v.(*Rule)
		builder.WriteString(fmt.Sprintf("name(%v) limit(%v), ", k.(string), rule.limit))
		return true
	})
	builder.WriteString("limiters: ")
	ml.limiters.Range(func(k, v interface{}) bool {
		builder.WriteString(fmt.Sprintf("name(%v) limiter(%v), ", k.(string), v.(LimiterGroup)))
		return true
	})
	return strings.TrimRight(builder.String(), ", ")
}

func (ml *MultiLimiter) GetRulesDesc() string {
	var builder strings.Builder
	ml.rules.Range(func(k, v interface{}) bool {
		rule := v.(*Rule)
		builder.WriteString(rule.String())
		builder.WriteString("\n")
		return true
	})
	return builder.String()
}

func (ml *MultiLimiter) AddRule(r *Rule) *MultiLimiter {
	if !r.limit.haveLimit() {
		return ml.ClearRule(r.properties)
	}
	name := r.properties.name()
	val, ok := ml.rules.Load(name)
	ml.rules.Store(name, r)
	if ok {
		oldRule := val.(*Rule)
		if !reflect.DeepEqual(oldRule.limit, r.limit) || oldRule.timeout != r.timeout {
			ml.clearLimiter(r.properties)
		}
	}
	return ml
}

func (ml *MultiLimiter) ClearRule(ps Properties) *MultiLimiter {
	ps.sort()
	ml.rules.Delete(ps.name())
	ml.clearLimiter(ps)
	return ml
}

func (ml *MultiLimiter) clearLimiter(ps Properties) {
	ml.limiters.Range(func(k, v interface{}) bool {
		name := k.(string)
		if ps.match(name) {
			ml.limiters.Delete(name)
		}
		return true
	})
}

func (ml *MultiLimiter) clearAll() {
	ml.rules.Range(func(k, v interface{}) bool {
		ml.rules.Delete(k)
		return true
	})
	ml.limiters.Range(func(k, v interface{}) bool {
		ml.limiters.Delete(k)
		return true
	})
}

func (ml *MultiLimiter) setStatus(status bool) {
	ml.status = status
}

func (ml *MultiLimiter) Wait(ctx context.Context, ps Properties) error {
	return ml.WaitN(ctx, ps, Stat{Count: 1})
}

func (ml *MultiLimiter) WaitN(ctx context.Context, ps Properties, stat Stat) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	return ml.waitOrAlowN(ctx, ps, stat, false)
}

func (ml *MultiLimiter) Allow(ps Properties) bool {
	return ml.AllowN(ps, Stat{Count: 1})
}

func (ml *MultiLimiter) AllowN(ps Properties, stat Stat) bool {
	err := ml.waitOrAlowN(nil, ps, stat, false)
	return err == nil
}

// if ctx doesn't has Deadline, use rule timeout
func (ml *MultiLimiter) WaitUseDefaultTimeout(ctx context.Context, ps Properties) error {
	if ctx == nil {
		return fmt.Errorf("nil context")
	}
	return ml.WaitNUseDefaultTimeout(ctx, ps, Stat{Count: 1})
}

func (ml *MultiLimiter) WaitNUseDefaultTimeout(ctx context.Context, ps Properties, stat Stat) error {
	return ml.waitOrAlowN(ctx, ps, stat, true)
}

func (ml *MultiLimiter) waitOrAlowN(ctx context.Context, ps Properties, stat Stat, useDefault bool) (err error) {
	if !ml.status {
		return nil
	}

	statIndex := stat.index()
	groups := ml.getLimiters(ps)
	for _, group := range groups {
		for i, limiterWithTimeout := range group {
			limiter := limiterWithTimeout.limiter
			if limiter == nil || !statIndex[i] {
				continue
			}

			// ctx is nil only in Allow()
			if ctx == nil {
				if !limiter.AllowN(time.Now(), stat.val(statType(i))) {
					err = fmt.Errorf("not allow")
					return
				}
				continue
			}

			timeout := limiterWithTimeout.timeout
			if timeout == 0 {
				timeout = defaultTimeout
			}
			_, hasDeadline := ctx.Deadline()
			var cancel context.CancelFunc
			if !hasDeadline && useDefault {
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			if err = waitN(limiter, ctx, stat.val(statType(i))); err != nil {
				return
			}
		}
	}
	return
}

func waitN(lim *rate.Limiter, ctx context.Context, n int) (err error) {
	deadline, ok := ctx.Deadline()
	if n > lim.Burst() {
		var sleepTime time.Duration
		if ok {
			now := time.Now()
			if deadline.After(now) {
				sleepTime = deadline.Sub(now)
			}
		} else {
			sleepTime = time.Duration(float64(n) / float64(lim.Limit()) * float64(time.Second))
		}
		time.Sleep(sleepTime)
		return
	}

	if err = lim.WaitN(ctx, n); err == nil {
		return
	}
	if !ok {
		return
	}
	now := time.Now()
	if deadline.After(now) {
		time.Sleep(deadline.Sub(now))
	}
	return
}

// may return duplicate limiter groups
func (ml *MultiLimiter) getLimiters(ps Properties) (re []LimiterGroup) {
	ml.iterate(ps, func(name string, limit LimitGroup, burst BurstGroup, timeout time.Duration) {
		group := ml.getLimiterGroup(name, limit, burst, timeout)
		re = append(re, group)
	})
	return
}

func (ml *MultiLimiter) getLimiterGroup(name string, limit LimitGroup, burst BurstGroup, timeout time.Duration) (re LimiterGroup) {
	if l, ok := ml.limiters.Load(name); ok {
		re = l.(LimiterGroup)
		return
	}

	for i, l := range limit {
		if l > 0 {
			re[i] = LimiterWithTimeout{rate.NewLimiter(l, burst[i]), timeout}
		}
	}
	ml.limiters.Store(name, re)
	return
}

func (ml *MultiLimiter) iterate(ps Properties, f func(name string, limit LimitGroup, burst BurstGroup, timeout time.Duration)) {
	var nameRules []*nameRule
	ml.rules.Range(func(k, v interface{}) bool {
		rule := v.(*Rule)
		isMatch, name := rule.match(ps)
		if !isMatch {
			return true
		}

		nameRule := getNameRule(nameRules, name, rule)
		if nameRule != nil {
			nameRules = append(nameRules, nameRule)
		}
		return true
	})

	for _, item := range nameRules {
		f(item.name, item.limit, item.burst, item.timeout)
	}
}

type nameRule struct {
	name string
	// default name count for each statType, a rule has smaller count has a higher priority
	defaultCount [statTypeMax]int
	limit        LimitGroup
	burst        BurstGroup
	timeout      time.Duration
}

func getNameRule(nameRules []*nameRule, name string, rule *Rule) *nameRule {
	var item *nameRule
	new := true
	for _, item = range nameRules {
		if name == item.name {
			new = false
			break
		}
	}

	if new {
		nameRule := nameRule{name: name, limit: rule.limit, burst: rule.burst, timeout: rule.timeout}
		for i := range nameRule.defaultCount {
			nameRule.defaultCount[i] = rule.defaultCount
		}
		return &nameRule
	}

	for i, l := range item.limit {
		if rule.limit[i] == 0 {
			continue
		}
		if l == 0 || rule.defaultCount < item.defaultCount[i] || (rule.defaultCount == item.defaultCount[i] && rule.limit[i] < l) {
			item.limit[i] = rule.limit[i]
			item.burst[i] = rule.burst[i]
			item.defaultCount[i] = rule.defaultCount
			item.timeout = rule.timeout
		}
	}
	return nil
}

func (ml *MultiLimiter) handlerGet(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(ml.GetRulesDesc()))
}

func (ml *MultiLimiter) handlerSetStatus(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if statusVal := r.FormValue(statusKey); statusVal != "" {
		status, err := strconv.Atoi(statusVal)
		if err != nil {
			w.Write([]byte("status is not integer\n"))
			return
		}
		var msg string
		if status > 0 {
			ml.setStatus(true)
			msg = "have set on"
		} else if status == 0 {
			ml.setStatus(false)
			msg = "have set off"
		} else {
			ml.clearAll()
			msg = "have cleared all rules and limiters"
		}
		w.Write([]byte(fmt.Sprintf("%v successfully\n", msg)))
	}
}
