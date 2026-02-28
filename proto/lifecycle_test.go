package proto

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidRules(t *testing.T) {
	t.Run("empty rules", func(t *testing.T) {
		rules := []*Rule{}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMissingRules, err)
	})

	t.Run("too many rules", func(t *testing.T) {
		rules := make([]*Rule, RuleMaxCounts+1)
		for i := 0; i <= RuleMaxCounts; i++ {
			days := 30
			rules[i] = &Rule{
				ID:     fmt.Sprintf("rule-%d", i),
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			}
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrTooManyRules, err)
	})

	t.Run("invalid ByMp", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Filter: &Filter{
					ByMp: 2,
				},
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrByMpAndDir, err)
	})

	t.Run("duplicate rule ID", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test1",
				},
			},
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   3,
						ToPoolId:     4,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test2",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrSameRuleID, err)
	})

	t.Run("missing rule ID", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMissingRuleID, err)
	})

	t.Run("rule ID too long", func(t *testing.T) {
		days := 30
		longID := make([]byte, MaxIdLength+1)
		for i := range longID {
			longID[i] = 'a'
		}
		rules := []*Rule{
			{
				ID:     string(longID),
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrTooLongRuleID, err)
	})

	t.Run("invalid rule ID format", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule with spaces",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrInvalidRuleID, err)
	})

	t.Run("invalid status", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: "InvalidStatus",
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMalformedXML, err)
	})

	t.Run("missing actions", func(t *testing.T) {
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMissingActions, err)
	})

	t.Run("rule prefix starts with /", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "/test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrRulePrefix, err)
	})

	t.Run("missing FromPoolId or ToPoolId", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   0,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrPoolId, err)
	})

	t.Run("DelayDelMinute too small", func(t *testing.T) {
		days := 30
		delayDelMinute := uint64(MinDelayDelMinute - 1)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:           &days,
						FromPoolId:     1,
						ToPoolId:       2,
						StorageClass:   OpTypeStorageClassHDD,
						DelayDelMinute: &delayDelMinute,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrDelayDelMinute, err)
	})

	t.Run("DelayDelMinute too large", func(t *testing.T) {
		days := 30
		delayDelMinute := uint64(MaxDelayDelMinute + 1)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:           &days,
						FromPoolId:     1,
						ToPoolId:       2,
						StorageClass:   OpTypeStorageClassHDD,
						DelayDelMinute: &delayDelMinute,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrDelayDelMinute, err)
	})

	t.Run("circular dependency in transitions", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test1",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   2,
						ToPoolId:     1,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test2",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrTransitionCycle, err)
	})

	t.Run("circular dependency with multiple pools", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test1",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   2,
						ToPoolId:     3,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test2",
				},
			},
			{
				ID:     "rule-3",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   3,
						ToPoolId:     1,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test3",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrTransitionCycle, err)
	})

	t.Run("conflicting rule prefix with same fromPoolId", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     3,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test/prefix",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrConflictRules, err)
	})

	t.Run("ByMp and Prefix cannot be specified together", func(t *testing.T) {
		days := 30
		// Need at least 2 rules for ValidRulePrefix to check ByMp and Prefix conflict
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
					ByMp:   ScanByMp,
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   3,
						ToPoolId:     4,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test2",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrByMpAndPrefix, err)
	})

	t.Run("valid rules with different fromPoolIds", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test1",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   3,
						ToPoolId:     4,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test1",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("valid rules with valid DelayDelMinute", func(t *testing.T) {
		days := 30
		delayDelMinute := uint64(MinDelayDelMinute)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:           &days,
						FromPoolId:     1,
						ToPoolId:       2,
						StorageClass:   OpTypeStorageClassHDD,
						DelayDelMinute: &delayDelMinute,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("valid rules with date transition", func(t *testing.T) {
		date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Date:         &date,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("invalid date not at midnight", func(t *testing.T) {
		date := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Date:         &date,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrDateType, err)
	})

	t.Run("invalid days zero or negative", func(t *testing.T) {
		days := 0
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrDaysType, err)
	})

	t.Run("date and days both set in transition", func(t *testing.T) {
		days := 30
		date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						Date:         &date,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMalformedXML, err)
	})

	t.Run("date and days both nil in transition", func(t *testing.T) {
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrMalformedXML, err)
	})

	t.Run("valid single rule", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("valid multiple rules with different prefixes and pools", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "prefix1",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   3,
						ToPoolId:     4,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "prefix2",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("self-loop transition should be ignored in cycle detection", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     1,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("empty prefix with ByMp", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					ByMp: ScanByMp,
				},
			},
		}
		err := ValidRules(rules)
		require.NoError(t, err)
	})

	t.Run("conflicting rule prefix with empty prefix", func(t *testing.T) {
		days := 30
		rules := []*Rule{
			{
				ID:     "rule-1",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     2,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "",
				},
			},
			{
				ID:     "rule-2",
				Status: RuleEnabled,
				Transitions: []*Transition{
					{
						Days:         &days,
						FromPoolId:   1,
						ToPoolId:     3,
						StorageClass: OpTypeStorageClassHDD,
					},
				},
				Filter: &Filter{
					Prefix: "test",
				},
			},
		}
		err := ValidRules(rules)
		require.Error(t, err)
		require.Equal(t, LifeCycleErrConflictRules, err)
	})
}
