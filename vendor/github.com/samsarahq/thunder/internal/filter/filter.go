package filter

import (
	"regexp"
	"strings"
)

var matchGroups = regexp.MustCompile(`(?:([^\s"]+)|"([^"]*)"?)+`)

func GetMatchStrings(query string) []string {
	if query == "" {
		return nil
	}
	matches := matchGroups.FindAllStringSubmatch(query, -1)
	matchStrings := make([]string, 0, len(matches))
	for _, match := range matches {
		// Empty quotes can count as a match, ignore them.
		if match[1] == "" && match[2] == "" {
			matchStrings = append(matchStrings, "")
			continue
		}
		// Get relevant match (one of these has to be a non-empty string, otherwise this wouldn't be a match).
		matchString := match[1]
		if matchString == "" {
			matchString = match[2]
		}
		// matchStrings[i] = matchString
		matchStrings = append(matchStrings, matchString)
	}
	return matchStrings
}

func MatchText(str string, matchStrings []string) bool {
	if len(matchStrings) == 0 {
		return true
	}
	for _, matchString := range matchStrings {
		if strings.Contains(strings.ToLower(str), strings.ToLower(matchString)) && matchString != "" {
			return true
		}
	}
	return false
}
