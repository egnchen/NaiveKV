package common

func RemoveElement(arr []string, ele string) []string {
	for i, e := range arr {
		if e == ele {
			return append(arr[:i], arr[i+1:]...)
		}
	}
	return arr
}
