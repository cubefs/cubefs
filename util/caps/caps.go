package caps

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Caps defines the capability type
type Caps struct {
	API          []string
	OwnerVOL     []string
	NoneOwnerVOL []string
}

// ContainCaps whether contain a capability with kind
func (c *Caps) ContainCaps(cat string, cap string) (r bool) {
	if cat == "API" {
		return traversalCaps(c.API, cap)
	} else if cat == "OwnerVOL" {
		return traversalCaps(c.OwnerVOL, cap)
	} else if cat == "NoneOwnerVOL" {
		return traversalCaps(c.NoneOwnerVOL, cap)
	}
	return false
}

func traversalCaps(caps []string, cap string) (r bool) {
	r = false
	for _, s := range caps {
		a := strings.Split(s, ":")
		b := strings.Split(cap, ":")
		i := 0
		for ; i < 3; i++ {
			if a[i] != "*" && a[i] != b[i] {
				break
			}
		}
		if i == 3 {
			r = true
			break
		}
	}
	return
}

// Init init a Caps instance
func (c *Caps) Init(b []byte) (err error) {
	if err = json.Unmarshal(b, c); err != nil {
		return
	}
	if err = c.check(); err != nil {
		return
	}
	c.cleanDup()
	return
}

// Dump dump the content of Caps
func (c *Caps) Dump() (d string) {
	for _, s := range c.API {
		d += fmt.Sprintf("API:%s,", s)
	}
	//TODO c.vol (no usage?)
	return
}

// Union union caps
func (c *Caps) Union(caps *Caps) {
	c.API = append(c.API, caps.API...)
	c.OwnerVOL = append(c.OwnerVOL, caps.OwnerVOL...)
	c.NoneOwnerVOL = append(c.NoneOwnerVOL, caps.NoneOwnerVOL...)
	c.cleanDup()
}

func (c *Caps) check() (err error) {
	apiRe := regexp.MustCompile("^[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}$")
	volRe := regexp.MustCompile("^[A-Za-z0-9*]{1,20}:[a-zA-Z0-9_-]{3,256}:[A-Za-z0-9*]{1,20}$")
	if err = checkRegexp(apiRe, c.API); err != nil {
		return
	}
	if err = checkRegexp(volRe, c.OwnerVOL); err != nil {
		return
	}
	if err = checkRegexp(volRe, c.NoneOwnerVOL); err != nil {
		return
	}
	return
}

func checkRegexp(re *regexp.Regexp, caps []string) (err error) {
	for _, cap := range caps {
		if !re.MatchString(cap) {
			err = fmt.Errorf("invalid cap [%s]", cap)
			return
		}
	}
	return
}

// Delete delete caps
func (c *Caps) Delete(caps *Caps) {
	c.API = deleteCaps(c.API, caps.API)
	c.OwnerVOL = deleteCaps(c.OwnerVOL, caps.OwnerVOL)
	c.NoneOwnerVOL = deleteCaps(c.NoneOwnerVOL, caps.NoneOwnerVOL)
}

func deleteCaps(caps []string, deleteCaps []string) []string {
	m := make(map[string]bool)
	for _, item := range caps {
		m[item] = true
	}
	caps = []string{}
	for _, item := range deleteCaps {
		delete(m, item)
	}
	for k := range m {
		caps = append(caps, k)
	}
	return caps
}

func (c *Caps) cleanDup() {
	c.API = cleanCaps(c.API)
	c.OwnerVOL = cleanCaps(c.OwnerVOL)
	c.NoneOwnerVOL = cleanCaps(c.NoneOwnerVOL)
}

func cleanCaps(caps []string) []string {
	newCaps := make([]string, 0)
	m := make(map[string]map[string]bool)
	for _, cap := range caps {
		a := strings.Split(cap, ":")
		key1 := a[0]
		key2 := a[1] + ":" + a[2]
		if _, ok := m[key1]; !ok {
			m[key1] = make(map[string]bool)
		}
		if _, ok := m[key1][key2]; !ok {
			newCaps = append(newCaps, cap)
			m[key1][key2] = true
		}
	}
	return newCaps
}
