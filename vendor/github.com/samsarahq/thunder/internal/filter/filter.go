package filter

import (
	"regexp"
	"strings"
)

var matchGroups = regexp.MustCompile(`(?:([^\s"]+)|"([^"]*)"?)+`)

func GetDefaultSearchTokens(query string) []string {
	if query == "" {
		return nil
	}
	matches := matchGroups.FindAllStringSubmatch(query, -1)
	searchTokens := make([]string, 0, len(matches))
	for _, match := range matches {
		// Empty quotes can count as a match, ignore them.
		if match[1] == "" && match[2] == "" {
			searchTokens = append(searchTokens, "")
			continue
		}
		// Get relevant match (one of these has to be a non-empty string, otherwise this wouldn't be a match).
		matchString := match[1]
		if matchString == "" {
			matchString = match[2]
		}
		// searchTokens[i] = matchString
		searchTokens = append(searchTokens, matchString)
	}
	return searchTokens
}

func DefaultFilterFunc(str string, searchTokens []string) bool {
	if len(searchTokens) == 0 {
		return true
	}
	for _, matchString := range searchTokens {
		if strings.Contains(strings.ToLower(str), strings.ToLower(matchString)) && matchString != "" {
			return true
		}
	}
	return false
}
