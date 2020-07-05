package common

func FilterString(arr []string, filter func(i int) bool) []string {
	var ret []string
	for i := range arr {
		if filter(i) {
			ret = append(ret, arr[i])
		}
	}
	return ret
}
