package defaults


func Str(val, defVal string) string {
	if val != "" {
		return val
	}
	return defVal
}
