package main

import (
	"fmt"
	"os"
)

func main() {
	for index, filename := range os.Args[1:] {

		fmt.Println(index, filename)

	}
}
