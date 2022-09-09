// Copyright 2019 The CubeFS Authors.
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

package objectnode

import (
	"net/url"
	"regexp"
	"strings"

	"net"
	"net/http"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

const (
	pathSep         = "/"
	tempFileNameSep = "_"
)

var (
	emptyPathItem = PathItem{}

	// Regular expression used to match one or more path separators.
	regexpSepPrefix = regexp.MustCompile("^/+")

	// Regular expression to match more than two consecutive path separators.
	regexpDupSep = regexp.MustCompile("/{2,}")
)

var (
	keyEscapedSkipBytes = []byte{'/', '*', '.', '-', '_'}
)

// PathItem defines path node attribute information,
// including node name and whether it is a directory.
type PathItem struct {
	Name        string
	IsDirectory bool
}

// PathIterator is a path iterator. Allocated to sequentially iterate each path node from a complete path.
type PathIterator struct {
	cursor int
	path   string
	inited bool
}

func (p *PathIterator) init() {
	if !p.inited {
		p.path = strings.TrimSpace(p.path)
		loc := regexpSepPrefix.FindStringIndex(p.path)
		if len(loc) == 2 {
			p.path = p.path[loc[1]:]
		}
		p.path = regexpDupSep.ReplaceAllString(p.path, pathSep)
		p.inited = true
	}

}

func (p *PathIterator) HasNext() bool {
	p.init()
	return p.cursor < len(p.path)
}

func (p *PathIterator) Reset() {
	p.cursor = 0
}

func (p PathIterator) ToSlice() []PathItem {
	newIterator := NewPathIterator(p.path)
	result := make([]PathItem, 0)
	for newIterator.HasNext() {
		result = append(result, newIterator.Next())
	}
	return result
}

func (p *PathIterator) Next() PathItem {
	p.init()
	if p.cursor >= len(p.path) {
		return emptyPathItem
	}
	var item PathItem
	index := strings.Index(p.path[p.cursor:], pathSep)
	if index >= 0 {
		item = PathItem{
			Name:        p.path[p.cursor : p.cursor+index],
			IsDirectory: true,
		}
		p.cursor = p.cursor + index + 1
	} else {
		item = PathItem{
			Name:        p.path[p.cursor:],
			IsDirectory: false,
		}
		p.cursor = len(p.path)
	}
	return item
}

func NewPathIterator(path string) PathIterator {
	return PathIterator{
		path: path,
	}
}

func splitPath(path string) (dirs []string, filename string) {
	pathParts := strings.Split(path, pathSep)
	if len(pathParts) > 1 {
		dirs = pathParts[:len(pathParts)-1]
	}
	filename = pathParts[len(pathParts)-1]
	return
}

func tempFileName(origin string) string {
	return "." + origin + tempFileNameSep + util.RandomString(16, util.LowerLetter|util.UpperLetter)
}

func formatSimpleTime(time time.Time) string {
	return time.UTC().Format("2006-01-02T15:04:05")
}

func formatTimeISO(time time.Time) string {
	return time.UTC().Format("2006-01-02T15:04:05.000Z")
}
func formatTimeISOLocal(time time.Time) string {
	return time.Local().Format("2006-01-02T15:04:05.000Z")
}

func formatTimeRFC1123(time time.Time) string {
	return time.UTC().Format(http.TimeFormat)
}

func parseTimeRFC1123(timeStr string) (time.Time, error) {
	t, err := time.Parse("Mon, 2 Jan 2006 15:04:05 GMT", timeStr)
	if err != nil {
		return t, err
	}
	return t, err
}

func transferError(key string, err error) Error {
	// TODO: complete sys error transfer
	ossError := Error{
		Key:     key,
		Message: err.Error(),
	}
	return ossError
}

// get request remote IP
func getRequestIP(r *http.Request) string {
	IPAddress := r.Header.Get("X-Real-Ip")
	if IPAddress == "" {
		IPAddress = r.Header.Get("X-Forwarded-For")
	}
	if IPAddress == "" {
		IPAddress = r.RemoteAddr
	}
	if ok := strings.Contains(IPAddress, ":"); ok {
		IPAddress = strings.Split(IPAddress, ":")[0]
	}

	return IPAddress
}

// check ipnet contains ip
// ip: 172.17.0.2
// ipnet: 172.17.0.0/16
func isIPNetContainsIP(ipStr, ipnetStr string) (bool, error) {
	if !strings.Contains(ipnetStr, "/") {
		if ipStr == ipnetStr {
			return true, nil
		} else {
			return false, nil
		}
	}
	_, ipnet, err := net.ParseCIDR(ipnetStr)
	if err != nil {
		log.LogInfof("parse ipnet error ipnet   %v", ipnetStr)
		return false, err
	}

	ip := net.ParseIP(ipStr)
	if ipnet.Contains(ip) {
		return true, nil
	}

	return false, nil
}

func patternMatch(pattern, key string) bool {
	if pattern == "" {
		return key == pattern
	}
	if pattern == "*" {
		return true
	}
	matched, err := regexp.MatchString(pattern, key)
	if err != nil {
		log.LogErrorf("patternMatch error %v", err)
		return false
	}

	return matched
}

func wrapUnescapedQuot(src string) string {
	return "\"" + src + "\""
}

func encodeKey(key, encodingType string) string {
	var isKeyEscapedSkipByte = func(b byte) bool {
		for _, skipByte := range keyEscapedSkipBytes {
			if b == skipByte {
				return true
			}
		}
		return false
	}
	if strings.ToLower(encodingType) == "url" {
		encodedKeyBuilder := strings.Builder{}
		for i := 0; i < len(key); i++ {
			b := byte(key[i])
			if isKeyEscapedSkipByte(b) {
				encodedKeyBuilder.Write([]byte{b})
			} else {
				encodedKeyBuilder.Write([]byte(url.QueryEscape(string(b))))
			}
		}
		return encodedKeyBuilder.String()
	}
	return key
}

func SplitFileRange(size, blockSize int64) (ranges [][2]int64) {
	blocks := size / blockSize
	if size%blockSize != 0 {
		blocks += 1
	}
	ranges = make([][2]int64, 0, blocks)
	remain := size
	aboveRage := [2]int64{0, 0}
	for remain > 0 {
		curRange := [2]int64{aboveRage[1], 0}
		if remain < blockSize {
			curRange[1] = size
			remain = 0
		} else {
			curRange[1] = blockSize
			remain -= blockSize
		}
		ranges = append(ranges, curRange)
		aboveRage[0], aboveRage[1] = curRange[0], curRange[1]
	}
	return ranges
}

// Checking and parsing user-defined metadata from request header.
// The optional user-defined metadata names must begin with "x-amz-meta-" to
// distinguish them from other HTTP headers.
// Notes:
// The PUT request header is limited to 8 KB in size. Within the PUT request header,
// the user-defined metadata is limited to 2 KB in size. The size of user-defined
// metadata is measured by taking the sum of the number of bytes in the UTF-8 encoding
// of each key and value.
// Reference: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
func ParseUserDefinedMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for name, values := range header {
		if strings.HasPrefix(name, http.CanonicalHeaderKey(HeaderNameXAmzMetaPrefix)) &&
			name != http.CanonicalHeaderKey(HeaderNameXAmzMetadataDirective) {
			metaName := strings.ToLower(name[len(HeaderNameXAmzMetaPrefix):])
			metaValue := strings.Join(values, ",")
			if !strings.HasPrefix(metaName, "oss:") {
				metadata[metaName] = metaValue
			}
		}
	}
	return metadata
}

// validate Cache-Control
var cacheControlDir = []string{"public", "private", "no-cache", "no-store", "no-transform", "must-revalidate", "proxy-revalidate"}
var maxAgeRegexp = regexp.MustCompile("^((max-age)|(s-maxage))=[1-9][0-9]*$")

func ValidateCacheControl(cacheControl string) bool {
	var cacheDirs = strings.Split(cacheControl, ",")
	for _, dir := range cacheDirs {
		if !contains(cacheControlDir, dir) && !maxAgeRegexp.MatchString(dir) {
			log.LogErrorf("invalid cache-control directive: %v", dir)
			return false
		}
	}
	return true
}

func ValidateCacheExpires(expires string) bool {
	var err error
	var stamp time.Time
	if stamp, err = time.Parse(RFC1123Format, expires); err != nil {
		log.LogErrorf("invalid expires: %v", expires)
		return false
	}
	expiresInt := stamp.Unix()
	now := time.Now().UTC().Unix()
	if now < expiresInt {
		return true
	}
	log.LogErrorf("Expires less than now: %v, now: %v", expires, now)
	return false
}
