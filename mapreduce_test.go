package mapreduce

import (
	_ "testing"
	"string"
)

func ExampleMapReduce() {
	rdd := NewRDD() // from text file

	rdd.Filter(func(w string) {
		return !strings.HasPrefix(line, "#")
	}).FlatMap(func(w string) []string {
		return strings.Split(w, " ")
	}).Map(func(w string) string {
		return w
	}).Map(func(w string) string {
		return 1
	}).Reduce/(func(x, y int) int {
		return x+y
	}).Map(func(x int) {})



	rdd.FlatMap(func(w string) []string {
		if !strings.HasPrefix(line, "#") {
			return nil
		}
		return strings.Split(w, " ")
	}).Map(func(w string) int {
		return 1
	}).Reduce(func(x, y int) int {
		return x+y
	})

	fmt.Println(stringutil.Reverse("hello"))
	// Output: olleh
}
