package main

import (
	"os"
	"demo/gointro/pipeline"
	"bufio"
	"fmt"
	"strconv"
)

func main() {
	//p := createPipeline(
	//	"large.in", 800000000, 8)
	//writeToFile(p, "large.out")
	//printFile("large.out")

	p := createNetworkPipeline(
		"large.in", 800000000, 4)
	writeToFile(p, "large.out")
	printFile("large.out")
}

func printFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p := pipeline.ReaderSource(file,-1)
	count := 0
	for v := range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}

}
func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	pipeline.WriterSink(writer, p)

}

func createPipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {

	chunkSize := fileSize / chunkCount

	pipeline.Init()

	sortResults := []<-chan int{}

	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		sortResults = append(sortResults,
			pipeline.InMemSort(source))
	}

	return pipeline.MergeN(sortResults...)
}

/**
	网络版
 */
func createNetworkPipeline(
	filename string,
	fileSize, chunkCount int) <-chan int {

	chunkSize := fileSize / chunkCount

	pipeline.Init()

	sortAddr := []string{}

	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i * chunkSize), 0)

		source := pipeline.ReaderSource(
			bufio.NewReader(file), chunkSize)

		addr := ":" + strconv.Itoa(7000 + i) // 7000 + i 这样一个端口号
		pipeline.NetworkSink(addr,
			pipeline.InMemSort(source))
		sortAddr = append(sortAddr, addr)
	}

	sortResults := []<-chan int{}
	for _, addr := range  sortAddr {
		sortResults = append(sortResults,
			pipeline.NetworkSource(addr))
	}

	return pipeline.MergeN(sortResults...)
}
