package pipeline

import (
	"math/rand"
	"io"
	"encoding/binary"
	"sort"
	"time"
	"fmt"
)

/**
	pipeline 是一个库，类型是Empty file，不包括main函数
*/


var startTime time.Time

func Init() {
	startTime = time.Now()
}

/**
	所有的数据都扔到这个channel里面去
 */
func ArraySource(a ...int) <-chan int { // <-表示方向，用的人只能从channel里拿东西，我们只能从channel里放东西
	out := make(chan int)

	// channel是goroutine和goroutine之间的通信，必须和goroutine连接
	// 所以这里用goroutine往channel里面送数据
	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

/**
	在内存里排序
 */
func InMemSort(in <-chan int) <-chan int { // 只进不出，返回是只出不进
	out := make(chan int, 1024)
	go func() {

		// Read into memory，获得所有数据，存在内存里
		a := []int{}

		for v := range in {
			// slice 是不可变对象，因此要用a去收它的返回值
			a = append(a, v)
		}
		fmt.Println("Read done:",
			time.Now().Sub(startTime))

		// Sort
		sort.Ints(a)
		fmt.Println("InMemSort done:",
			time.Now().Sub(startTime))

		// Output
		for _, v := range  a {
			out <- v
		}
		close(out)
	}()
	return out
}

/**
	从2个channel里归并数据
 */
func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int, 1024)
	go func() {
		v1, ok1 := <-in1
		v2, ok2 := <-in2
		for ok1 || ok2 { // 至少归并的一路输入数据才比较
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				v1, ok1 = <-in1
			} else {
				out <- v2
				v2, ok2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge done:",
			time.Now().Sub(startTime))
	}()
	return out
}

/**
	从文件读数据
	chunkSize 是为了分块读
 */
func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int, 1024) // 不要发一个收一个，而是1024再发收
	go func() {
		buffer := make([]byte, 8) // 大小是8字节 ，64位/8位
		bytesRead := 0
		for {
			n, err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil ||
				(chunkSize != -1 && // -1表示可以一直读，不然就只能读chunkSize那么大
					bytesRead >= chunkSize) {
				break
			}
		}
		close(out)
	}()
	return out
}

/**
	写数据到文件
 */
func WriterSink(writer io.Writer, in <-chan int)  {
	for v := range  in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}


/*
	测试数据生成
 */
func RandomSource(count int) <-chan int {
	out := make(chan int)
	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

/**
	多路两两归并
 */
func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	m := len(inputs) / 2
	// merge inputs[0..m) inputs(m..end)
	return Merge(
		MergeN(inputs[:m]...),
		MergeN(inputs[m:]...))

}