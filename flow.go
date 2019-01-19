package mapreduce

import (
	"strings"

	"github.com/chrislusf/glow/flow"
)

func main() {
	flow.New().TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).Map(func(line string, ch chan string) {
		for _, token := range strings.Split(line, ":") {
			ch <- token
		}
	}).Map(func(key string) (string, int) {
		return key, 1
	}).ReduceByKey(func(x int, y int) int {
		return x + y
	}).Map(func(x int) (string, string, int) {
		return "partition key", "clustering key", x
	}).Repartition("my topic", func(x int) (string, int) { // action
		return "kafka key", x
	}).ReduceByKey(func(x, y int) int {
		return x + y
	}).Map().AggreageByKey(func(laststate string, y int) string { // have stored
		return laststate + "D"
	}).Map(func(x int) (string, string, int) {
		return "partition key", "clustering key", x
	}).Save().Map(func(x int) {
		println("count:", x)
	}).Run()
}

func wordcountbyword() {
	flow.New().TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).FlatMap(func(line string) []string {
		return strings.Split(line, " ")
	}).Map(func(key string) (string, int) {
		return key, 1
	}).ReduceByKeyGlobal("my topic", func(x, y int) int { // action
		return x + y
	}).Map(func(key string, c int) (string, string, int) {
		return key, "", c
	}).Map(func(x int) {
		println("count:", x)
		return x
	}).Save()
}

func wordcount() {
	flow.New().TextFile(
		"/etc/passwd", 3,
	).Filter(func(line string) bool {
		return !strings.HasPrefix(line, "#")
	}).FlatMap(func(line string) []string {
		return strings.Split(line, " ")
	}).Map(func(key string) (string, int) {
		return key, 1
	}).Reduce("my topic", func(x, y int) int { // action
		return x + y
	}).Map(func(x int) int {
		println("count:", x)
		return x
	}).Save()
}

func countUserByAttributeByHour() {
	flow.New().TextFile(
		"/etc/passwd",
		1000,
	).FlatMap(func(u User) []string {
		return []string{
			"accid, fullname, createdhour",
			"accid, age, createdhour",
			"accid, email, createdhour",
		}
	}).Map(func (key string) (string, int){
		return key, 1
	}).ReduceByKeyGlobal("TT", func(x, y int) {
		return x + y
	}).Map(func(key string, c int) (string, string, int) {
		// get day from key
		accday:= "S"
		return  accday, key, c
	}).Save() // created
}

func countUserByAttributeTotal() {
	flow.New().TextFile(
		"/etc/passwd",
		1000,
	).FlatMap(func(u User) []string {
		return []string{
			"accid, fullname",
			"accid, age",
			"accid, email",
		}
	}).Map(func (key string) (string, int){
		return key, 1
	}).ReduceByKeyGlobal("TT", func(x, y int) {
		return x + y
	}).Map(func(key string, c int) (string, string, int) {
		return  key, "", c
	}).Save() // created
}

// able to delete
func countUserBySegment() {
	flow.New().TextFile(
		"/etc/passwd",
		1000,
	).FlatMap(func(u User) []string {
		return []string{
			"accid, fullname, s1",
			"accid, fullname, s2",
			"accid, age, s1",
			"accid, age, s2",
		}
	}).Map(func (key string) (string, int){
		return key, 1
	}).ReduceByKeyGlobal("TT", func(x, y int) {
		return x + y
	}).Map(func(key string, c int) (string, string, int) {
		return  key, "", c
	}).Save() // created
}
