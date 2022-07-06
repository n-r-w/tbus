// package tbus
// Очередь собщений, направленная через транзакционную БД
// Используется при необходимости рассылать только закомиченные изменения в рамках транзакции
package tbus

import (
	"github.com/n-r-w/mbus"
	"sync"
	"time"
)

type Event struct {
	Topic  mbus.TopicID
	Values []any
}

// SaveFunc функция записи событий в БД в рамках транзакции
// tranID идентификатор транзакции - зависит от используемой БД (например *pgx.Tx)
// Уровень изоляции транзакции как минимум READ COMMITED
type SaveFunc func(tranID any, event Event) error

// ReadFunc функция чтения закомиченных событий из БД
// Должна в рамках собственной транзакции не только читать, но обеспечить чтобы не было повторного чтения,
// например удалять или сдвигать указатель на последнее прочитанное событие
// Уровень изоляции транзакции как минимум READ COMMITED
type ReadFunc func() (events []Event, err error)

// ErrorFunc необязательная функция, в которую приходят ошибки чтения событий из БД
type ErrorFunc func(err error)

// TranMessageBus очередь собщений, направленная через транзакционную БД
type TranMessageBus struct {
	saveFn    SaveFunc
	readFn    ReadFunc
	errorFunc ErrorFunc
	pollTime  time.Duration

	bus *mbus.MessageBus

	started bool
	mu      sync.Mutex
	closeCh chan struct{}
	closeWg sync.WaitGroup
}

// New создать TranMessageBus
// parallelCall вызов функции обработчика для каждого подписчика в отдельной горутине не блокируя его
// handlerQueueSize размер буферизированного канала для каждого подписчика
// pollTime период опроса БД на наличие новых записей
// saveFn функция для записи в БД
// readFn функция для чтения новых, закомиченных событий из БД
// errorFunc функция, в которую будут направляться ошибки чтения из БД
func New(parallelCall bool, handlerQueueSize int, pollTime time.Duration, saveFn SaveFunc, readFn ReadFunc, errorFunc ErrorFunc) *TranMessageBus {
	if saveFn == nil || readFn == nil {
		panic("bad parameters")
	}

	return &TranMessageBus{
		saveFn:    saveFn,
		readFn:    readFn,
		errorFunc: errorFunc,
		pollTime:  pollTime,
		bus:       mbus.New(parallelCall, handlerQueueSize),
		started:   false,
		mu:        sync.Mutex{},
		closeCh:   make(chan struct{}),
		closeWg:   sync.WaitGroup{},
	}
}

func (b *TranMessageBus) check() {
	if !b.started {
		panic("TranMessageBus already started")
	}
}

func (b *TranMessageBus) Start() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.started {
		panic("TranMessageBus already started")
	}

	b.closeWg.Add(1)
	go b.commitedWorker()
	b.started = true
}

func (b *TranMessageBus) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.closeCh <- struct{}{}
	b.closeWg.Wait()
	close(b.closeCh)

	b.bus.WaitAllAndClose()

	b.started = false
}

func (b *TranMessageBus) commitedWorker() {
	defer b.closeWg.Done()
	for {
		select {
		case <-b.closeCh:
			return
		default:
			time.Sleep(b.pollTime)
		}

		if events, err := b.readFn(); err != nil {
			if b.errorFunc != nil {
				b.errorFunc(err)
			}
		} else {
			if len(events) == 0 {
				continue
			}

			for _, e := range events {
				b.bus.Publish(e.Topic, e.Values...)
			}
		}
	}
}

func (b *TranMessageBus) Publish(tranID any, topic mbus.TopicID, args ...any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.saveFn(tranID, Event{
		Topic:  topic,
		Values: args,
	})
}

func (b *TranMessageBus) Subscribe(topic mbus.TopicID, fn any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.Subscribe(topic, fn)
}

func (b *TranMessageBus) Unsubscribe(topic mbus.TopicID, fn any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.Unsubscribe(topic, fn)
}

func (b *TranMessageBus) Close(topic mbus.TopicID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.Close(topic)
}

func (b *TranMessageBus) BufferSize(topic mbus.TopicID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.BufferSize(topic)
}

func (b *TranMessageBus) WaitAndClose(topic mbus.TopicID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.WaitAndClose(topic)
}

func (b *TranMessageBus) WaitAllAndClose() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.WaitAllAndClose()
}
