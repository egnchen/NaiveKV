package common

// Remove several elements from an array.
// This algorithm is unstable, which means permutation of arr is not guaranteed.
func RemoveElements(arr []string, ele ...string) []string {
	tmp := make(map[string]struct{})
	for _, v := range arr {
		tmp[v] = struct{}{}
	}
	for _, v := range ele {
		delete(tmp, v)
	}
	var ret []string
	for k := range tmp {
		ret = append(ret, k)
	}
	return ret
}
