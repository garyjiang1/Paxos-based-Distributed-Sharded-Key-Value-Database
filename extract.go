package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run extract.go <file>")
		os.Exit(1)
	}

	fileName := os.Args[1]
	file, err := os.Open(fileName + ".txt")

	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}

	defer file.Close()

	outputFile, err := os.Create(fileName + "-parsed.txt")
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	ignorePatterns := []string{"paxos_previous Dial() failed:", "unexpected EOF", "write unix ->", "2022/", "reading body EOF"}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		ignore := false
		for _, pattern := range ignorePatterns {
			if strings.HasPrefix(line, pattern) {
				ignore = true
				break
			}
		}
		if !ignore {
			outputFile.WriteString(line + "\n")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
	}
}
