package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var bs = uint64(16 * 1024 * 1024)

func main() {

	// zeroes, err := os.ReadFile("/tmp/0.zero")
	// if err != nil {
	// 	panic(err)
	// }
	// zeroes2, err := os.ReadFile("/tmp/1.zero")
	// if err != nil {
	// 	panic(err)
	// }

	// var blocksCounted uint64
	var nonZeroBlocks uint64
	var lastBlockReported uint64
	var i int

	var blocksNum []uint64
	var bn uint64

	// outFD, err := os.Create("/tmp/out2.data")
	// if err != nil {
	// 	panic(err)
	// }
	// defer outFD.Close()

	fd, err := os.Open("/dev/vdb")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	// Reading file by chunks (blocks) and parse every block
	// If any byte in block isn't 0, mark whole block to zeroing
	r := bufio.NewReader(fd)
	for {
		buf := make([]byte, bs) //the chunk size
		n, err := r.Read(buf)   //loading chunk into buffer
		buf = buf[:n]
		if n == 0 {

			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
			panic(err)
		}
		// TODO: split big buf to few small bufs for best resolution of blockmap
		// We can read big block, but we shouldn't write whole big block if it's not full of data
		for i = range buf {
			if (buf[i] ^ byte(0)) != byte(0) {
				blocksNum = append(blocksNum, bn)
				nonZeroBlocks += 1
				break
			}
		}
		if bn > lastBlockReported+10 {
			// fmt.Printf("Bytes handled: '%d', actual non zeroes bytes: '%d'\n", bn*uint64(bs), nonZeroBlocks*uint64(bs))
			lastBlockReported = bn
		}
		bn += 1
	}
	fd.Close()

	blockmap := mergeBlockMap(blocksNum)

	// TODO: rework - collect offset and size in blocks, not in bytes

	fmt.Printf("Block numbers to zeroing: '%d'\n", blocksNum)

	var buf string

	var b strings.Builder
	var i2 uint64
	b.Grow(int(bs))
	for i2 = 0; i2 < bs; i2++ {
		b.WriteByte(0)
	}
	buf = b.String()

	fd, err = os.Create("/dev/vdb")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	// TODO: sort keys of map before file write
	for offset, size := range blockmap {
		fmt.Printf("start: %d, end: %d, size: %d\n", offset, offset+size, size)

		_, err := fd.Seek(int64(offset), 0)
		if err != nil {
			panic(err)
		}
		fmt.Printf("VSDEBUG: offset: '%d'\n", offset)

		count := int(size / bs)

		for c := 0; c < count; c++ {
			_, err = fd.Write([]byte(buf))
			if err != nil {
				panic(err)
			}
		}
	}
}

func mergeBlockMap(blocksNum []uint64) (blockmap map[uint64]uint64) {
	var prevOffset, lastEnd uint64
	var offset, size uint64
	blockmap = make(map[uint64]uint64, 0)
	for _, bn := range blocksNum {
		offset = bn * bs
		size = bs
		if lastEnd > 0 && lastEnd == offset {
			// fmt.Println("merged")
			blockmap[prevOffset] += size
		} else {
			blockmap[offset] = size
			prevOffset = offset
		}

		lastEnd = offset + size
	}
	return blockmap
}
