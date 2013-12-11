/*
NotifyDispatcher attempts to make working with a single Listener easy for a
dynamic set of independent listeners.


Usage


If the Listener emits a ListenerEventReconnected event, all listeners
receive a notification with BePid set to -1.


    import (
        "github.com/lib/pq"
        "github.com/johto/notifydispatcher"
        "fmt"
        "time"
    )

    func listener(dispatcher *notifydispatcher.NotifyDispatcher) {
        ch := make(chan pq.Notification, 8)
        err := dispatcher.Listen("listenerchannel", ch)
        if err != nil {
            panic(err)
        }
        for n := range ch {
            if n.BePid == -1 {
                fmt.Println("lost connection, but we're fine now!")
                continue
            }

            fmt.Println("received notification!")
            // do something with notification
        }
        panic("could not keep up!")
    }

    func main() {
        dispatcher := notifydispatcher.NewNotifyDispatcher(pq.NewListener("", time.Second, time.Minute))
        for i := 0; i < 8; i++ {
            go listener(dispatcher)
        }
        select{}
    }
    
*/
package notifydispatcher

import (
	"errors"
	"fmt"
	"github.com/lib/pq"
	"sync"
)

var (
	ErrChannelAlreadyActive = errors.New("channel is already active")
	ErrChannelNotActive = errors.New("channel is not active")
)

var errClosed = errors.New("NotifyDispatcher has been closed")

// SlowReaderEliminationStrategy controls the behaviour of the dispatcher in
// case the buffer of a listener's channel is full and attempting to send to it
// would block the dispatcher, preventing it from delivering notifications for
// unrelated listeners.  The default is CloseSlowReaders, but it can be changed
// at any point during a dispatcher's lifespan using
// SetSlowReaderEliminationStrategy.
type SlowReaderEliminationStrategy int
const (
	// When a send would block, the listener's channel is removed from the set
	// of listeners for that notification channel, and the channel is closed.
	// This is the default strategy.
	CloseSlowReaders SlowReaderEliminationStrategy = iota

	// When a send would block, the notification is not delivered.  Delivery is
	// not reattempted.
	NeglectSlowReaders
)

type listenRequest struct {
	channel string
	unlisten bool
}

type NotifyDispatcher struct {
	listener *pq.Listener
	slowReaderEliminationStrategy SlowReaderEliminationStrategy

	listenRequestch chan listenRequest

	lock sync.Mutex
	channels map[string] *listenSet
	closed bool
	// provide an escape hatch for goroutines sending on listenRequestch
	closeChannel chan bool
}

// NewNotifyDispatcher creates a new NotifyDispatcher, using the supplied
// pq.Listener underneath.  The ownership of the Listener is transferred to
// NotifyDispatcher.  You should not use it after calling NewNotifyDispatcher.
func NewNotifyDispatcher(l *pq.Listener) *NotifyDispatcher {
	d := &NotifyDispatcher{
		listener: l,
		slowReaderEliminationStrategy: CloseSlowReaders,
		listenRequestch: make(chan listenRequest, 64),
		channels: make(map[string] *listenSet),
		closeChannel: make(chan bool),
	}
	go d.dispatcherLoop()
	go d.listenRequestHandlerLoop()
	return d
}

// Sets the strategy for mitigating the adverse effects slow readers might have
// on the dispatcher.  See SlowReaderEliminationStrategy.
func (d *NotifyDispatcher) SetSlowReaderEliminationStrategy(strategy SlowReaderEliminationStrategy) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.slowReaderEliminationStrategy = strategy
}

func (d *NotifyDispatcher) broadcast() {
	reapchans := []string{}

	d.lock.Lock()
	for channel, set := range d.channels {
		n := pq.Notification{
			BePid: -1,
			Channel: channel,
			Extra: "",
		}
		if !set.broadcast(d.slowReaderEliminationStrategy, n) {
			reapchans = append(reapchans, channel)
		}
	}
	d.lock.Unlock()

	for _, ch := range reapchans {
		d.listenRequestch <- listenRequest{ch, true}
	}
}

func (d *NotifyDispatcher) dispatch(n pq.Notification) {
	reap := false

	d.lock.Lock()
	set, ok := d.channels[n.Channel]
	if ok {
		reap = !set.broadcast(d.slowReaderEliminationStrategy, n)
	}
	d.lock.Unlock()

	if reap {
		d.listenRequestch <- listenRequest{n.Channel, true}
	}
}

func (d *NotifyDispatcher) dispatcherLoop() {
	for {
		select {
			case e := <-d.listener.Event:
				if e.Event == pq.ListenerEventReconnected {
					d.broadcast()
				}
			case n := <-d.listener.Notify:
				d.dispatch(n)
		}
	}
}

func (d *NotifyDispatcher) execListen(channel string) {
	for {
		err := d.listener.Listen(channel)
		// ErrChannelAlreadyOpen is a valid return value here; we could have
		// abandoned a channel in Unlisten() if the server returned an error
		// for no apparent reason.
		if err == nil ||
		   err == pq.ErrChannelAlreadyOpen {
			break
		}
	}

	d.lock.Lock()
	defer d.lock.Unlock()
	set, ok := d.channels[channel]
	if !ok {
		panic("oops")
	}
	set.setState(listenSetStateActive)
}

func (d *NotifyDispatcher) execUnlisten(channel string) {
	// we don't really care about the error
	_ = d.listener.Unlisten(channel)

	d.lock.Lock()
	set, ok := d.channels[channel]
	if !ok {
		panic("oops")
	}
	if set.state != listenSetStateZombie {
		panic("oops")
	}
	if set.reap() {
		delete(d.channels, channel)
		d.lock.Unlock()
	} else {
		// Couldn't reap the set because it got new listeners while we were
		// waiting for the UNLISTEN to go through.  Re-LISTEN it, but remember
		// to release the lock first.
		d.lock.Unlock()

		d.execListen(channel)
	}
}

func (d *NotifyDispatcher) listenRequestHandlerLoop() {
	for {
		// check closeChannel, just in case we've been closed and there's a
		// backlog of requests
		select {
			case <-d.closeChannel:
				return
			default:
		}

		select {
			case <-d.closeChannel:
				return

			case req := <-d.listenRequestch:
				if req.unlisten {
					d.execUnlisten(req.channel)
				} else {
					d.execListen(req.channel)
				}
		}
	}
}

func (d *NotifyDispatcher) requestListen(channel string, unlisten bool) error {
	select {
		// make sure we don't get stuck here if someone Close()s us
		case <-d.closeChannel:
			return errClosed
		case d.listenRequestch <- listenRequest{channel, unlisten}:
			return nil
	}

	panic("not reached")
}

// Listen adds ch to the set of listeners for notification channel channel.  ch
// should be a buffered channel.  If SlowReaderEliminationStrategy is
// CloseSlowReaders, ch should not already be a listener of a different
// channel.  If ch is already in the set of listeners for channel,
// ErrChannelAlreadyActive is returned.  After Listen has returned, the
// notification channel is open and the dispatcher will attempt to deliver all
// notifications received for that channel to ch.
func (d *NotifyDispatcher) Listen(channel string, ch chan<- pq.Notification) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.closed {
		return errClosed
	}

	set, ok := d.channels[channel]
	if ok {
		err := set.add(ch)
		if err != nil {
			return err
		}
	} else {
		set = d.newListenSet(ch)
		d.channels[channel] = set

		// must not be holding the lock while requesting a listen
		d.lock.Unlock()
		err := d.requestListen(channel, false)
		if err != nil {
			return err
		}
		d.lock.Lock()
	}

	return set.waitForActive(&d.closed)
}

// Removes ch from the set of listeners for notification channel channel.  If
// ch is not in the set of listeners for channel, ErrChannelNotActive is
// returned.
func (d *NotifyDispatcher) Unlisten(channel string, ch chan<- pq.Notification) error {
	d.lock.Lock()

	if d.closed {
		d.lock.Unlock()
		return errClosed
	}

	set, ok := d.channels[channel]
	if !ok {
		d.lock.Unlock()
		return ErrChannelNotActive
	}
	last, err := set.remove(ch)
	if err != nil {
		d.lock.Unlock()
		return err
	}
	if !last {
		// the set isn't empty; nothing further for us to do
		d.lock.Unlock()
		return nil
	}
	d.lock.Unlock()

	// we were the last listener, cue the reaper
	return d.requestListen(channel, true)
}

func (d *NotifyDispatcher) Close() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.closed {
		return errClosed
	}

	d.closed = true
	close(d.closeChannel)

	for _, set := range d.channels {
		set.activeOrClosedCond.Broadcast()
	}

	return nil
}

type listenSetState int
const (
	// The set was recently spawned or respawned, and it's waiting for a call
	// to Listen() to succeed.
	listenSetStateNewborn listenSetState = iota
	// The set is ready and any notifications from the database will be
	// dispatched to the set.
	listenSetStateActive
	// The set has recently been emptied, and it's waiting for a call to
	// Unlisten() to finish.
	listenSetStateZombie
)

type listenSet struct {
	channels map[chan<- pq.Notification] struct{}
	state listenSetState
	activeOrClosedCond *sync.Cond
}

func (d *NotifyDispatcher) newListenSet(firstInhabitant chan<- pq.Notification) *listenSet {
	s := &listenSet{
		channels: make(map[chan<- pq.Notification] struct{}),
		state: listenSetStateNewborn,
	}
	s.activeOrClosedCond = sync.NewCond(&d.lock)
	s.channels[firstInhabitant] = struct{}{}
	return s
}

func (s *listenSet) setState(newState listenSetState) {
	var expectedState listenSetState
	switch newState {
		case listenSetStateNewborn:
			expectedState = listenSetStateZombie
		case listenSetStateActive:
			expectedState = listenSetStateNewborn
		case listenSetStateZombie:
			expectedState = listenSetStateActive
	}
	if s.state != expectedState {
		panic(fmt.Sprintf("illegal state transition from %v to %v", s.state, newState))
	}
	s.state = newState
	if s.state == listenSetStateActive {
		s.activeOrClosedCond.Broadcast()
	}
}

func (s *listenSet) add(ch chan<- pq.Notification) error {
	_, ok := s.channels[ch]
	if ok {
		return ErrChannelAlreadyActive
	}
	s.channels[ch] = struct{}{}
	return nil
}

func (s *listenSet) remove(ch chan<- pq.Notification) (last bool, err error) {
	_, ok := s.channels[ch]
	if !ok {
		return false, ErrChannelNotActive
	}
	delete(s.channels, ch)

	if len(s.channels) == 0 {
		s.setState(listenSetStateZombie)
		return true, nil
	}
	return false, nil
}

func (s *listenSet) broadcast(strategy SlowReaderEliminationStrategy, n pq.Notification) bool {
	// must be active
	if s.state != listenSetStateActive {
		return true
	}

	for ch := range s.channels {
		select {
			case ch <- n:

			default:
				if strategy == CloseSlowReaders {
					delete(s.channels, ch)
					close(ch)
				}
		}
	}

	if len(s.channels) == 0 {
		s.setState(listenSetStateZombie)
		return false
	}

	return true
}

// Marks the set active after a successful call to Listen().
func (s *listenSet) markActive() {
	s.setState(listenSetStateActive)
}

// Wait for the listen set to become "active".  Returns nil if successfull, or
// errClosed if the dispatcher was closed while waiting.  The caller should be
// holding d.lock.
func (s *listenSet) waitForActive(closed *bool) error {
	for {
		if *closed {
			return errClosed
		}
		if s.state == listenSetStateActive {
			return nil
		}
		s.activeOrClosedCond.Wait()
	}
}

// Try to reap a zombie set after Unlisten().  Returns true if the set should
// be removed, false otherwise.
func (s *listenSet) reap() bool {
	if s.state != listenSetStateZombie {
		panic("unexpected state in reap")
	}

	if len(s.channels) > 0 {
		// we need to be respawned
		s.setState(listenSetStateNewborn)
		return false
	}

	return true
}
