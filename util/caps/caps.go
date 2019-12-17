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
	VOL []string
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
	} else if cat == "VOL" {
		for _, s := range c.VOL {
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
	//TODO c.vol (no usage?)
	return
}

// Union union caps
func (c *Caps) Union(caps *Caps) {
	c.API = append(c.API, caps.API...)
	c.VOL = append(c.VOL, caps.VOL...)
	c.cleanDup()
}

func (c *Caps) check() (err error) {
	apiRe := regexp.MustCompile("^[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}:[A-Za-z0-9*]{1,20}$")
	volRe := regexp.MustCompile("^[A-Za-z0-9*]{1,20}:[a-zA-Z0-9_-]{3,256}:[A-Za-z0-9*]{1,20}$")
	for _, cap := range c.API {
		if !apiRe.MatchString(cap) {
			err = fmt.Errorf("invalid cap [%s]", cap)
			return
		}
	}
	for _, cap := range c.VOL {
		if !volRe.MatchString(cap) {
			err = fmt.Errorf("invalid cap [%s]", cap)
			return
		}
	}
	return
}

// Delete delete caps
func (c *Caps) Delete(caps *Caps) {
	mapi := make(map[string]bool)
	mvol := make(map[string]bool)
	for _, item := range c.API {
		mapi[item] = true
	}
	for _, item := range c.VOL {
		mvol[item] = true
	}
	c.API = []string{}
	c.VOL = []string{}
	for _, item := range caps.API {
		delete(mapi, item)
	}
	for _, item := range caps.VOL {
		delete(mvol, item)
	}
	for k := range mapi {
		c.API = append(c.API, k)
	}
	for k := range mvol {
		c.VOL = append(c.VOL, k)
	}
}

func (c *Caps) cleanDup() {
	API := make([]string, 0)
	VOL := make([]string, 0)
	mapi := make(map[string]map[string]bool)
	mvol := make(map[string]map[string]bool)
	for _, cap := range c.API {
		a := strings.Split(cap, ":")
		key1 := a[0]
		key2 := a[1] + ":" + a[2]
		if _, ok := mapi[key1]; !ok {
			mapi[key1] = make(map[string]bool, 0)
		}
		if _, ok := mapi[key1][key2]; !ok {
			API = append(API, cap)
			mapi[key1][key2] = true
		}
	}
	for _, cap := range c.VOL {
		a := strings.Split(cap, ":")
		key1 := a[0]
		key2 := a[1] + ":" + a[2]
		if _, ok := mvol[key1]; !ok {
			mvol[key1] = make(map[string]bool, 0)
		}
		if _, ok := mvol[key1][key2]; !ok {
			VOL = append(VOL, cap)
			mvol[key1][key2] = true
		}
	}
	c.API = API
	c.VOL = VOL
	return
}
