package caps

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Caps defines the capability type
type Caps struct {
	API []string
}

// ContainCaps whether contain a capability with kind
func (c *Caps) ContainCaps(cat string, cap string) (r bool) {
	r = false
	if cat == "API" {
		for _, s := range c.API {
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
	return
}

// Union union caps
func (c *Caps) Union(caps *Caps) {
	c.API = append(c.API, caps.API...)
	c.cleanDup()
}

func (c *Caps) check() (err error) {
	re := regexp.MustCompile("^[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}$")
	for _, cap := range c.API {
		if !re.MatchString(cap) {
			err = fmt.Errorf("invalid cap [%s]", cap)
			return
		}
	}
	return
}

// Delete delete caps
func (c *Caps) Delete(caps *Caps) {
	m := make(map[string]bool)
	for _, item := range c.API {
		m[item] = true
	}
	c.API = []string{}
	for _, item := range caps.API {
		delete(m, item)
	}
	for k := range m {
		c.API = append(c.API, k)
	}
}

func (c *Caps) cleanDup() {
	API := make([]string, 0)
	m := make(map[string]map[string]bool)
	for _, cap := range c.API {
		a := strings.Split(cap, ":")
		key1 := a[0]
		key2 := a[1] + ":" + a[2]
		if _, ok := m[key1]; !ok {
			m[key1] = make(map[string]bool, 0)
		}
		if _, ok := m[key1][key2]; !ok {
			API = append(API, cap)
			m[key1][key2] = true
		}
	}
	c.API = API
	return
}
