package main

import (
	"demo/gointro/pipeline"
	"fmt"
	"os"
	"bufio"
)

func main() {

	const filename = "small.in"
	const n = 64 // 100M数据，每个数8字节，最后应该是一个800M的文件

	file, err := os.Create(filename) // 创建文件
	if err != nil { // 创建不成功就报错
		panic(err)
	}
	defer file.Close() // 函数退出之前close掉，有点像java的finally

	p := pipeline.RandomSource(n) // 生成随机数

	writer := bufio.NewWriter(file)  // 使用这个函数有默认的buffer，读的更快
	pipeline.WriterSink(writer, p)  // 随机数写到文件里
	writer.Flush() // 使用buffer需要flush, 确保数据全部写出去

	file, err = os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	p = pipeline.ReaderSource(bufio.NewReader(file), -1)
	count := 0
	for v:= range p {
		fmt.Println(v)
		count++
		if count >= 100 {
			break
		}
	}
}

func mergeDemo() {
	p := pipeline.Merge(
		pipeline.InMemSort(
			pipeline.ArraySource(3, 2, 6, 7, 4)),
		pipeline.InMemSort(
			pipeline.ArraySource(7, 4, 0, 3, 2, 13, 8)))
	/*for {
		if num, ok := <- p; ok { // ok 为true证明channel还在，为false证明被close了
			fmt.Println(num)
		} else {
			break
		}
	}*/
	// 更简单的方法，但这种方法发送方一定的要close，不然range不知道什么时候结束
	for v := range p {
		fmt.Println(v)
	}
}
