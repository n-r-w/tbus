// package tbus
// Очередь собщений, направленная через транзакционную БД
// Используется при необходимости рассылать только закомиченные изменения в рамках транзакции
package tbus

import (
	"sync"
	"time"

	"github.com/n-r-w/mbus"
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

// Start запуск фоновой горутины, читающей события из БД и отправляющей их подписчикам
// до запуска нельзя проводить никакие операции
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

// Stop завершение обработки всех событий, которые уже были взяты в работу и отписка всех подписчиков
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

// Publish поместить сообщение в топик
// сообщение сначала попадает в БД в рамках транзакции, чтобы затем быть вычитанным оттуда после завершения этой транзакции
func (b *TranMessageBus) Publish(tranID any, topic mbus.TopicID, args ...any) error {
	if tranID == nil {
		panic("transaction is nil")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// даем записывать вне зависимости от того, запущена горутина чтения или нет
	// т.к. в момент остановки могут прилететь новые события и они должны попасть в БД, чтобы быть обработанными при следующем запуске
	return b.saveFn(tranID, Event{
		Topic:  topic,
		Values: args,
	})
}

// Subscribe подписаться на события в топике
func (b *TranMessageBus) Subscribe(topic mbus.TopicID, fn any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.Subscribe(topic, fn)
}

// Unsubscribe описаться от топика
func (b *TranMessageBus) Unsubscribe(topic mbus.TopicID, fn any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.Unsubscribe(topic, fn)
}

// Close закрыть топик. Все не обработанные события будут потеряны. Лучше не использовать
func (b *TranMessageBus) Close(topic mbus.TopicID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.Close(topic)
}

// BufferSize текущий размер буфера событий в топике. Для мониторинга
func (b *TranMessageBus) BufferSize(topic mbus.TopicID) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	return b.bus.BufferSize(topic)
}

// WaitAndClose закрыть топик, предварительно дождавшись завершения всех обработчиков и освобождения очередей
func (b *TranMessageBus) WaitAndClose(topic mbus.TopicID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.WaitAndClose(topic)
}

// WaitAllAndClose закрыть все топики, предварительно дождавшись завершения всех обработчиков и освобождения очередей
func (b *TranMessageBus) WaitAllAndClose() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.check()

	b.bus.WaitAllAndClose()
}

func (b *TranMessageBus) check() {
	if !b.started {
		panic("TranMessageBus already started")
	}
}

// commitedWorker горутина для чтения закомиченных событий из БД
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
