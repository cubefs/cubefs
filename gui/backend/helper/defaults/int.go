package defaults

func Int(val, defVal int) int {
	if val != 0 {
		return val
	}
	return defVal
}
