package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	bufSize       int = 100
	flushInterval int = 2 // Интервал опустошения буфера в секундах
)

type RingBuffer struct {
	data     []int
	size     int
	head     int
	tail     int
	count    int
	mutex    sync.Mutex
	nonEmpty *sync.Cond
	nonFull  *sync.Cond
	closed   bool
}

func NewRingBuffer(size int) *RingBuffer {
	rb := &RingBuffer{
		data: make([]int, size),
		size: size,
	}
	rb.nonEmpty = sync.NewCond(&rb.mutex)
	rb.nonFull = sync.NewCond(&rb.mutex)
	return rb
}

func (rb *RingBuffer) Push(val int) bool {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	for rb.count == rb.size && !rb.closed {
		rb.nonFull.Wait()
	}

	if rb.closed {
		return false
	}

	rb.data[rb.tail] = val
	rb.tail = (rb.tail + 1) % rb.size
	rb.count++
	rb.nonEmpty.Signal()
	return true
}

func (rb *RingBuffer) Pop() (int, bool) {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	for rb.count == 0 && !rb.closed {
		rb.nonEmpty.Wait()
	}

	if rb.count == 0 {
		return 0, false
	}

	val := rb.data[rb.head]
	rb.head = (rb.head + 1) % rb.size
	rb.count--
	rb.nonFull.Signal()
	return val, true
}

func (rb *RingBuffer) Flush() []int {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	result := make([]int, rb.count)
	for i := 0; i < rb.count; i++ {
		idx := (rb.head + i) % rb.size
		result[i] = rb.data[idx]
	}

	rb.head = 0
	rb.tail = 0
	rb.count = 0
	rb.nonFull.Broadcast()
	return result
}

func (rb *RingBuffer) Close() {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()
	rb.closed = true
	rb.nonEmpty.Broadcast()
	rb.nonFull.Broadcast()
}

func filterNegative(input <-chan int, output chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for num := range input {
		if num >= 0 {
			output <- num
		}
	}
}

func filterMultiplesOfThree(input <-chan int, output chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	for num := range input {
		if num != 0 && num%3 != 0 {
			output <- num
		}
	}
}

func bufferStage(input <-chan int, output chan<- int, rb *RingBuffer, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(output)

	// Горутина для периодического сброса буфера
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(flushInterval))
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				flushed := rb.Flush()
				for _, val := range flushed {
					select {
					case output <- val:
					case <-done:
						return
					}
				}
			case <-done:
				return
			}
		}
	}()

	// Основная обработка входящих данных
	for {
		select {
		case num, ok := <-input:
			if !ok {
				// Финальный сброс при закрытии входного канала
				flushed := rb.Flush()
				for _, val := range flushed {
					output <- val
				}
				return
			}
			if !rb.Push(num) {
				return
			}
		case <-done:
			return
		}
	}
}

func consoleInput(input chan<- int, done chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(input)
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Вводите числа (или 'print' для вывода результата):")

	for scanner.Scan() {
		text := strings.TrimSpace(scanner.Text())

		if text == "print" {
			close(done)
			return
		}

		num, err := strconv.Atoi(text)
		if err != nil {
			fmt.Printf("Ошибка: '%s' — не число\n", text)
			continue
		}

		input <- num
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Ошибка чтения ввода:", err)
	}
	close(done)
}

func consoleOutput(input <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for num := range input {
		fmt.Printf("Получены данные: %d\n", num)
	}
}

func main() {
	var wg sync.WaitGroup

	inputChan := make(chan int, bufSize)
	filteredNegChan := make(chan int, bufSize)
	filteredThreeChan := make(chan int, bufSize)
	outputChan := make(chan int, bufSize)
	doneChan := make(chan struct{})

	ringBuffer := NewRingBuffer(bufSize)

	wg.Add(1)
	go consoleInput(inputChan, doneChan, &wg)

	wg.Add(1)
	go filterNegative(inputChan, filteredNegChan, &wg)

	wg.Add(1)
	go filterMultiplesOfThree(filteredNegChan, filteredThreeChan, &wg)

	wg.Add(1)
	go bufferStage(filteredThreeChan, outputChan, ringBuffer, doneChan, &wg)

	wg.Add(1)
	go consoleOutput(outputChan, &wg)

	// Ожидаем завершения всех горутин
	wg.Wait()
	fmt.Println("Программа завершена корректно")
}
