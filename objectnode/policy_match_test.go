package objectnode

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegexp(t *testing.T) {
	//case1, any keyname
	raw := "*"
	pattern := makeRegexPattern(raw)

	keynames := []string{"abc", "", " ", "*", ".*", "*.", "**", "/a", "a/", "/a*a", "a/*"}
	for _, k := range keynames {
		ok, err := regexp.MatchString(pattern, k)
		require.NoError(t, err)
		require.True(t, ok)
	}

	//case2, specify name
	raw = "user?/img/*/2019/*"
	pattern = makeRegexPattern(raw)

	keynames = []string{"user1/img/jpg/2019/Jan", "user2/img//2019/"}
	for _, k := range keynames {
		ok, err := regexp.MatchString(pattern, k)
		require.NoError(t, err)
		require.True(t, ok)
	}
	keynames = []string{"user1a/img/jpg/2019/Jan", "user2/img/png/2019"}
	for _, k := range keynames {
		ok, err := regexp.MatchString(pattern, k)
		require.NoError(t, err)
		require.False(t, ok)
	}
}

func TestResourceMatch(t *testing.T) {
	var s Statement
	//case1: resource is bucket, specified object(case sensitive)
	s.Resource = []interface{}{"arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/abc/*", "arn:aws:s3:::mybucket/ABD"}
	b := s.matchResource(LIST_OBJECTS, nil)
	require.True(t, b)

	b = s.matchResource(PUT_OBJECT, "abc/de")
	require.True(t, b)

	b = s.matchResource(PUT_OBJECT, "ABD")
	require.True(t, b)

	b = s.matchResource(PUT_OBJECT, "abd")
	require.False(t, b)

	b = s.matchResource(PUT_OBJECT, "ABC/ab")
	require.False(t, b)

	//case2: resource is any object
	s.Resource = "arn:aws:s3:::examplebucket/*"
	keynames := []string{"", "*", "abc", "/*"}
	for _, k := range keynames {
		b = s.matchResource(PUT_OBJECT, k)
		require.True(t, b)
	}
	b = s.matchResource(LIST_OBJECTS, "")
	require.False(t, b)
}

func TestPrincipalMatch_MultiUsers(t *testing.T) {
	s := &Statement{}
	principal := `{"AWS":["11","22"]}`
	json.Unmarshal([]byte(principal), &s.Principal)
	result := s.matchPrincipal("22")
	require.True(t, result)

	result = s.matchPrincipal("33")
	require.False(t, result)

	result = s.matchPrincipal("")
	require.False(t, result)
}

func TestPrincipalMatch_Anyone(t *testing.T) {
	s := &Statement{}
	err := json.Unmarshal([]byte(`"*"`), &s.Principal)
	require.NoError(t, err)
	result := s.matchPrincipal("22")
	require.True(t, result)
	result = s.matchPrincipal("")
	require.True(t, result)
}

func TestPrincipalMatch_Anyone_Case2(t *testing.T) {
	s := &Statement{}
	json.Unmarshal([]byte(`{"AWS":"*"}`), &s.Principal)
	result := s.matchPrincipal("22")
	require.True(t, result)
	result = s.matchPrincipal("")
	require.True(t, result)
}

func TestPrincipalMatch_SpecificalUser(t *testing.T) {
	s := &Statement{}
	json.Unmarshal([]byte(`{"AWS":"11"}`), &s.Principal)
	result := s.matchPrincipal("11")
	require.True(t, result)
	result = s.matchPrincipal("22")
	require.False(t, result)
	result = s.matchPrincipal("")
	require.False(t, result)
}

func TestActionMatch_Any(t *testing.T) {
	s := &Statement{}
	err := json.Unmarshal([]byte(`"s3:*"`), &s.Action)
	require.NoError(t, err)
	result := s.matchAction(GET_OBJECT)
	require.True(t, result)
}

func TestActionMatch_Any_Mix(t *testing.T) {
	s := &Statement{}
	err := json.Unmarshal([]byte(`["s3:PutObject","s3:*"]`), &s.Action)
	require.NoError(t, err)
	result := s.matchAction(GET_OBJECT)
	require.True(t, result)
}

func TestActionMatch_Specifical(t *testing.T) {
	s := &Statement{}
	err := json.Unmarshal([]byte(`"s3:PutObject"`), &s.Action)
	require.NoError(t, err)
	result := s.matchAction(PUT_OBJECT)
	require.True(t, result)
	result = s.matchAction(GET_OBJECT)
	require.False(t, result)

	err = json.Unmarshal([]byte(`"s3:DeleteObject"`), &s.Action)
	require.NoError(t, err)
	result = s.matchAction(DELETE_OBJECT)
	require.True(t, result)
	result = s.matchAction(BATCH_DELETE)
	require.True(t, result)
}

func TestActionMatch_Multiaction(t *testing.T) {
	s := &Statement{}
	err := json.Unmarshal([]byte(`["s3:PutObject","s3:GetObject"]`), &s.Action)
	require.NoError(t, err)
	result := s.matchAction(PUT_OBJECT)
	require.True(t, result)
	result = s.matchAction(GET_OBJECT)
	require.True(t, result)
	result = s.matchAction(DELETE_OBJECT)
	require.False(t, result)
}

func TestIpMatch_Whitelist(t *testing.T) {
	ipcondition := `{"IpAddress": {"aws:SourceIp": ["1.2.3.4/24", "1.1.1.1","fe80::45e:9d4c:20ca:20f7/64"] }}`
	s := &Statement{}
	err := json.Unmarshal([]byte(ipcondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck := map[string]string{}

	clientIps := []string{"1.2.3.5", "1.1.1.1", "fe80::45e:9d4c:20ca:20f8"}
	for _, ip := range clientIps {
		conditionToCheck[SOURCEIP] = ip
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}

	conditionToCheck[SOURCEIP] = "1.2.4.5"
	result := s.matchCondition(conditionToCheck)
	require.False(t, result)
}

func TestIpMatch_Blacklist(t *testing.T) {
	ipcondition := `{"NotIpAddress": {"aws:SourceIp": "1.2.3.4/24" }}`
	s := &Statement{}
	err := json.Unmarshal([]byte(ipcondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck := map[string]string{}

	conditionToCheck[SOURCEIP] = "1.2.3.5"
	result := s.matchCondition(conditionToCheck)
	require.False(t, result)

	conditionToCheck[SOURCEIP] = "1.2.4.5"
	result = s.matchCondition(conditionToCheck)
	require.True(t, result)
}

func TestIpMatch_InWhite_NotInBlack(t *testing.T) {
	ipcondition := `{
			"IpAddress": {"aws:SourceIp": ["1.2.3.0/24", "2.2.2.2","4.4.4.4"] },
			"NotIpAddress":{"aws:SourceIp":["1.2.3.5","2.2.2.2","3.3.3.3"] }
		}`
	s := &Statement{}
	err := json.Unmarshal([]byte(ipcondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck := map[string]string{}

	//white ip
	clientIps := []string{"1.2.3.6", "4.4.4.4"}
	for _, ip := range clientIps {
		conditionToCheck[SOURCEIP] = ip
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}
	//black ip
	clientIps = []string{"1.2.3.5", "2.2.2.2", "3.3.3.3"}
	for _, ip := range clientIps {
		conditionToCheck[SOURCEIP] = ip
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}

}
func TestStringLikeMatch(t *testing.T) {
	strCondition := `{
			"StringLike":{"aws:Referer":["http://*.example.com/*","http://example.com/*"]}
		}`
	s := &Statement{}
	err := json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck := map[string]string{}
	clientReferer := []string{"http://www.example.com/bucket", "http://example.com/ac", "http://bucekt.example.com/"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}

	clientReferer = []string{"http://www.example.cn/bucket", "http://example.cn/ac"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}

	strCondition = `{
			"StringLike":{"aws:Host":["http://*.example.com/*","http://example.com/*"]}
		}`
	err = json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck = map[string]string{}
	clientHost := []string{"http://www.example.com/bucket", "http://example.com/ac", "http://bucekt.example.com/"}
	for _, host := range clientHost {
		conditionToCheck[HOST] = host
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}

	clientHost = []string{"http://www.example.cn/bucket", "http://example.cn/ac"}
	for _, host := range clientHost {
		conditionToCheck[HOST] = host
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}

	strCondition = `{"StringLike":{"aws:Referer":"http://*.example.com/*"}}`
	err = json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	clientReferer = []string{"http://www.example.com/bucket", "http://a.example.com/ac"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}
}

func TestStringNotLikeMatch(t *testing.T) {
	strCondition := `{
			"StringNotLike":{"aws:Referer":["http://*.example.com/*","http://example.com/*"]}
		}`
	s := &Statement{}
	err := json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck := map[string]string{}
	clientReferer := []string{"http://www.example.com/bucket", "http://example.com/ac", "http://bucekt.example.com/"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}

	clientReferer = []string{"http://www.example.cn/bucket", "http://example.cn/ac"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}

	strCondition = `{
			"StringNotLike":{"aws:Host":["http://*.example.com/*","http://example.com/*"]}
		}`
	err = json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	conditionToCheck = map[string]string{}
	clientHost := []string{"http://www.example.com/bucket", "http://example.com/ac", "http://bucekt.example.com/"}
	for _, host := range clientHost {
		conditionToCheck[HOST] = host
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}

	clientHost = []string{"http://www.example.cn/bucket", "http://example.cn/ac"}
	for _, host := range clientHost {
		conditionToCheck[HOST] = host
		result := s.matchCondition(conditionToCheck)
		require.True(t, result)
	}

	strCondition = `{"StringNotLike":{"aws:Referer":"http://*.example.com/*"}}`
	err = json.Unmarshal([]byte(strCondition), &s.Condition)
	require.NoError(t, err)
	clientReferer = []string{"http://www.example.com/bucket", "http://a.example.com/ac"}
	for _, referer := range clientReferer {
		conditionToCheck[REFERER] = referer
		result := s.matchCondition(conditionToCheck)
		require.False(t, result)
	}
}
