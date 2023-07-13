package common

import (
	"strconv"
	"strings"
)

type SpecialOwnerCheckRule struct {
	CheckIntervalMin     uint64
	SafeCleanIntervalMin uint64
}

func ParseSpecialOwnerRules(ruleMap map[string]string) (rules map[string]*SpecialOwnerCheckRule)  {
	var err error
	ruleValue, ok := ruleMap["check_special_owner"]
	if !ok {
		return
	}

	specialOwnerRulesStr := strings.Split(ruleValue, ";")
	rules = make(map[string]*SpecialOwnerCheckRule, len(specialOwnerRulesStr))
	for _, specialOwnerRuleStr := range specialOwnerRulesStr {
		ruleInfoArr := strings.Split(specialOwnerRuleStr, "_")
		if len(ruleInfoArr) != 3 {
			continue
		}

		specialOwner := ruleInfoArr[0]
		checkRule := new(SpecialOwnerCheckRule)
		if checkRule.CheckIntervalMin, err = strconv.ParseUint(ruleInfoArr[1], 10, 64); err != nil {
			continue
		}

		if checkRule.SafeCleanIntervalMin, err = strconv.ParseUint(ruleInfoArr[2], 10, 64); err != nil {
			continue
		}
		rules[specialOwner] = checkRule
	}
	return
}

func ParseCheckAllRules(ruleMap map[string]string) (checkAll bool, checkVolumes map[string]byte, skipVolumes map[string]byte) {
	checkAll = true
	ruleValue, ok := ruleMap["check_all"]
	if !ok {
		return
	}

	ruleInfoArr := strings.Split(ruleValue, ":")
	if len(ruleInfoArr) == 0 {
		return
	}

	parseValue, err := strconv.ParseBool(ruleInfoArr[0])
	if err != nil {
		return
	}

	checkAll = parseValue
	if len(ruleInfoArr) < 2 {
		return
	}

	volumes := strings.Split(ruleInfoArr[1], ",")
	if checkAll {
		skipVolumes = make(map[string]byte, len(volumes))
		for _, volume := range volumes {
			skipVolumes[volume] = 0
		}
	} else {
		checkVolumes = make(map[string]byte, len(volumes))
		for _, volume := range volumes {
			checkVolumes[volume] = 0
		}
	}
	return
}
