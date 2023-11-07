package multirate

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type PropertyType int

const (
	PropertyTypeNetwork PropertyType = iota
	PropertyTypeVol
	PropertyTypeOp
	PropertyTypeDisk
)

const (
	separator  = "_"
	NetworkIn  = "in"
	NetworkOut = "out"
)

type Property struct {
	Type PropertyType
	Name string
}

type PropertyConstruct struct {
	Properties Properties
}

type Properties []Property

func NewPropertyConstruct() *PropertyConstruct {
	return &PropertyConstruct{
		Properties: make([]Property, 0),
	}
}

func (construct *PropertyConstruct) Result() Properties {
	return construct.Properties
}
func (construct *PropertyConstruct) AddVol(vol string) *PropertyConstruct {
	construct.Properties = append(construct.Properties, Property{Type: PropertyTypeVol, Name: vol})
	return construct
}

func (construct *PropertyConstruct) AddOp(op int) *PropertyConstruct {
	construct.Properties = append(construct.Properties, Property{Type: PropertyTypeOp, Name: strconv.Itoa(op)})
	return construct
}

func (construct *PropertyConstruct) AddPath(disk string) *PropertyConstruct {
	construct.Properties = append(construct.Properties, Property{Type: PropertyTypeDisk, Name: disk})
	return construct
}

func (construct *PropertyConstruct) AddNetIn() *PropertyConstruct {
	construct.Properties = append(construct.Properties, Property{Type: PropertyTypeNetwork, Name: NetworkIn})
	return construct
}

func (construct *PropertyConstruct) AddNetOut() *PropertyConstruct {
	construct.Properties = append(construct.Properties, Property{Type: PropertyTypeNetwork, Name: NetworkOut})
	return construct
}

func (p Property) RuleName() string {
	return fmt.Sprintf("%v%v", p.Type, p.Name)
}

func (p Property) DefaultRuleName() string {
	return fmt.Sprintf("%v", p.Type)
}

func (ps Properties) haveDefaultName() bool {
	for _, p := range ps {
		if p.Name == "" {
			return true
		}
	}
	return false
}

func (ps Properties) RuleName() string {
	var strs []string
	for _, p := range ps {
		strs = append(strs, p.RuleName())
	}
	sort.Slice(strs, func(i, j int) bool {
		return strs[i] < strs[j]
	})
	return strings.Join(strs, separator)
}

// get all 2-degree default rule names
// e.g., []Property{{PropertyTypeVol, "vol"}, {PropertyTypeOp, "write"}} has following rules:
// 1vol_2write, 1_2write, 1vol_2, 1_2
func (ps Properties) DefaultRuleName() (names []string) {
	l := len(ps)
	for i := 0; i < l; i++ {
		for j := i + 1; j < l; j++ {
			str1 := ps[i].RuleName()
			str2 := ps[j].RuleName()
			strs := []string{str1, str2}
			sort.Slice(strs, func(i, j int) bool {
				return strs[i] < strs[j]
			})
			names = append(names, strings.Join(strs, separator))

			str1 = ps[i].RuleName()
			str2 = ps[j].DefaultRuleName()
			strs = []string{str1, str2}
			sort.Slice(strs, func(i, j int) bool {
				return strs[i] < strs[j]
			})
			names = append(names, strings.Join(strs, separator))

			str1 = ps[i].DefaultRuleName()
			str2 = ps[j].RuleName()
			strs = []string{str1, str2}
			sort.Slice(strs, func(i, j int) bool {
				return strs[i] < strs[j]
			})
			names = append(names, strings.Join(strs, separator))

			str1 = ps[i].DefaultRuleName()
			str2 = ps[j].DefaultRuleName()
			strs = []string{str1, str2}
			sort.Slice(strs, func(i, j int) bool {
				return strs[i] < strs[j]
			})
			names = append(names, strings.Join(strs, separator))
		}
	}
	return
}

func (ps Properties) includeName(name string) bool {
	var strs []string
	for _, p := range ps {
		strs = append(strs, p.RuleName())
	}
	sort.Slice(strs, func(i, j int) bool {
		return strs[i] < strs[j]
	})
	names := strings.Split(name, separator)
	if len(names) != len(strs) {
		return false
	}
	for i := 0; i < len(strs); i++ {
		// if a property has an empty name, its rule name len is 1
		if strs[i] != names[i] && len(strs[i]) > 1 {
			return false
		}
	}
	return true
}

type Rule struct {
	Properties
	Limit rate.Limit
	Burst int
}

type MultiLimiter struct {
	rules    sync.Map // map[string]*Rule
	limiters sync.Map // map[string]*rate.Limiter
}

// empty name means the rule is for every object of the same type
func NewRule(t PropertyType, name string, limit int, burst int) *Rule {
	properties := []Property{{Type: t, Name: name}}
	return NewMultiPropertyRule(properties, limit, burst)
}

func NewMultiPropertyRule(p Properties, limit int, burst int) *Rule {
	r := new(Rule)
	r.Properties = p
	r.Limit = rate.Limit(limit)
	r.Burst = burst
	return r
}

func NewMultiLimiter() *MultiLimiter {
	ml := new(MultiLimiter)
	return ml
}

func (ml *MultiLimiter) AddRule(r *Rule) *MultiLimiter {
	name := r.Properties.RuleName()
	ml.rules.Store(name, r)
	if r.Properties.haveDefaultName() {
		return ml
	}
	l, ok := ml.limiters.Load(name)
	if !ok {
		ml.limiters.Store(name, rate.NewLimiter(r.Limit, r.Burst))
	} else {
		limiter, ok := l.(*rate.Limiter)
		if ok {
			limiter.SetLimit(r.Limit)
			limiter.SetBurst(r.Burst)
		}
	}
	return ml
}

func (ml *MultiLimiter) ClearRule(p Properties) *MultiLimiter {
	name := p.RuleName()
	ml.rules.Delete(name)
	if !p.haveDefaultName() {
		ml.limiters.Delete(name)
		return ml
	}

	ml.limiters.Range(func(k, v interface{}) bool {
		name = k.(string)
		if _, ok := ml.rules.Load(name); ok {
			return true
		}
		if p.includeName(name) {
			ml.limiters.Delete(name)
		}
		return true
	})
	return ml
}

func (ml *MultiLimiter) Wait(ctx context.Context, properties Properties) error {
	return ml.WaitN(ctx, 1, properties)
}

const defaultLimiterTimeout = time.Second * 3

var limiterOpTimeoutMap = map[int]time.Duration{
	int(proto.OpRead):                 proto.ReadDeadlineTime * time.Second,
	int(proto.OpStreamRead):           proto.ReadDeadlineTime * time.Second,
	int(proto.OpWrite):                proto.WriteDeadlineTime * time.Second,
	int(proto.OpCreateExtent):         proto.WriteDeadlineTime * time.Second,
	int(proto.OpTinyExtentRepairRead): time.Second,
	int(proto.OpExtentRepairRead):     5 * time.Second,
	proto.OpRepairWrite_:              time.Second,
}

func getLimitOpTimeout(opcode int) time.Duration {
	if timeout, ok := limiterOpTimeoutMap[opcode]; !ok {
		return defaultLimiterTimeout
	} else {
		return timeout
	}
}

func (ml *MultiLimiter) WaitN(ctx context.Context, n int, properties Properties) error {
	var curContext context.Context
	var cancel context.CancelFunc
	if ctx == nil {
		curContext, cancel = context.WithTimeout(context.Background(), getLimitOpTimeout(proto.OpRepairWrite_))
		defer cancel()
	} else {
		curContext = ctx
	}
	limiters := ml.getLimiters(properties)
	for _, l := range limiters {
		if err := l.WaitN(curContext, n); err != nil {
			return err
		}
	}
	return nil
}

func (ml *MultiLimiter) Allow(properties Properties) bool {
	return ml.AllowN(time.Now(), 1, properties)
}

func (ml *MultiLimiter) AllowN(now time.Time, n int, properties Properties) bool {
	limiters := ml.getLimiters(properties)
	for _, l := range limiters {
		if !l.AllowN(now, n) {
			return false
		}
	}
	return true
}

func (ml *MultiLimiter) getLimiters(properties Properties) (limiters []*rate.Limiter) {
	var limiter *rate.Limiter
	// for every single property rule
	for _, p := range properties {
		name := p.RuleName()
		if l, ok := ml.limiters.Load(name); !ok {
			if r, ok := ml.rules.Load(p.DefaultRuleName()); ok {
				rule, ok := r.(*Rule)
				if ok {
					limiter = rate.NewLimiter(rule.Limit, rule.Burst)
					ml.limiters.Store(name, limiter)
					limiters = append(limiters, limiter)
				}
			}
		} else {
			limiter, ok = l.(*rate.Limiter)
			if ok {
				limiters = append(limiters, limiter)
			}
		}
	}
	// for combined properties rule
	if len(properties) > 1 {
		name := properties.RuleName()
		if l, ok := ml.limiters.Load(name); !ok {
			defaultNames := properties.DefaultRuleName()
			for _, defaultName := range defaultNames {
				if r, ok := ml.rules.Load(defaultName); ok {
					rule, ok := r.(*Rule)
					if ok {
						limiter = rate.NewLimiter(rule.Limit, rule.Burst)
						ml.limiters.Store(name, limiter)
						limiters = append(limiters, limiter)
						// there should be only one limiter for a single property(or compound property),
						// here just break and give up finding a most strict limiter
						break
					}
				}
			}
		} else {
			limiter, ok = l.(*rate.Limiter)
			if ok {
				limiters = append(limiters, limiter)
			}
		}
	}

	return
}
