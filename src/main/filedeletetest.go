package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
)

func main() {
	files, err := os.ReadDir("./")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, file := range files {
		found, err := regexp.MatchString("^mr-", file.Name())
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Println(file.Name(), "已删除")
			os.Remove(file.Name())
		}
	}
}
