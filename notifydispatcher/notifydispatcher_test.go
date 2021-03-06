package notifydispatcher

import (
	"github.com/lib/pq"
	"fmt"
	"runtime"
	"sync"
	"testing"
)


type mlListenRequest struct {
	channel string
	unlisten bool

	// synchronization points. don't ask
	s1 chan struct{}
	s2 chan error
	s3 chan struct{}
	s4 chan struct{}
}

type waitFlags int
const (
	noWait waitFlags = iota
)

type mockedListener struct {
	sync.Mutex
	t *testing.T
	requestQueue []mlListenRequest
	notifyCh chan *pq.Notification
	listenErrors []error
}

func panicf(f string, v ...interface{}) {
	panic(fmt.Sprintf(f, v...))
}

// see e.g. assertNotification()
func yield() {
	for i := 0; i < 10; i++ {
		runtime.Gosched()
	}
}


// assertions
func assert(t *testing.T, cond bool, condition string) {
	if !cond {
		var buf [8192]byte
		s := runtime.Stack(buf[:], true)
		t.Errorf("Condition %q failed", condition)
		t.Errorf("  Stack trace:")
		t.Errorf("%s", buf[:s])
		t.FailNow()
	}
}
func assertClosedCh(t *testing.T, ch <-chan *pq.Notification, condition string) {
	select {
		case _, ok := <-ch:
			assert(t, !ok, condition)
		default:
			assert(t, false, condition)
	}
}
func assertEmptyCh(t *testing.T, ch <-chan *pq.Notification, condition string) {
	select {
		case n, ok := <-ch:
			assert(t, ok, "channel must not be closed in assertEmptyCh")
			t.Errorf("received notification %v in assertEmptyCh()", n.Channel)
			assert(t, false, condition)
		default:
	}
}
func assertEmptyBroadcastCh(t *testing.T, ch BroadcastChannel, condition string) {
	select {
		case _, ok := <-ch.Channel:
			assert(t, ok, "channel must not be closed in assertEmptyBroadcastCh")
			t.Errorf("received notification in assertBroadcastEmptyCh()")
			assert(t, false, condition)
		default:
	}
}
func assertNotification(t *testing.T, ch <-chan *pq.Notification, channel interface{}, condition string) {
	// This is sketchy as hell, but nothing else seems to be working :-(  The
	// problem is that by the time we run this function, it's not guaranteed
	// that the dispatcherLoop in NotificationDispatcher will have executed for
	// long enough for it to have actually sent the notification over to ch.
	// There doesn't seem to be any good place to inject any kind of
	// synchronization, either :-(
	//
	// I really wish go had some kind of a magic function which said "run every
	// other goroutine until they're all asleep, and only then wake me up, or
	// panic if that doesn't happen within time.Duration".  Being able to
	// recover from a deadlock is another way of looking at it, but that
	// doesn't seem to be possible either.
	yield()

	select {
		case n := <-ch:
			if channel == nil {
				assert(t, n == nil,
					   fmt.Sprintf("notification %+#v must be nil", n))
			} else {
				chstr := channel.(string)
				assert(t, n.Channel == chstr,
					   fmt.Sprintf("channel %s must be %s", n.Channel, chstr))
			}
		default:
			assert(t, false, condition)
	}
}
func assertBroadcastNotification(t *testing.T, ch BroadcastChannel, condition string) {
	// see assertNotification
	yield()

	select {
		case n := <-ch.Channel:
			assert(t, n == struct{}{}, fmt.Sprintf("%+#v must be nil", n))
		default:
			assert(t, false, condition)
	}
}


func (ml *mockedListener) assert(cond bool, condition string) {
	assert(ml.t, cond, condition)
}
func (ml *mockedListener) assertEmptyQueue() {
	ml.assert(len(ml.requestQueue) == 0, "listener's request queue must be empty")
}
func (ml *mockedListener) assertQueuedRequest() {
	ml.assert(len(ml.requestQueue) > 0, "listener must have queued requests")
}
func (ml *mockedListener) assertNoListenErrors() {
	ml.assert(len(ml.listenErrors) == 0, "listener must have no accrued errors")
}

// Waits for an asynchronous listen which can be satisfied without having to
// send a LISTEN query to the server.
func (ml *mockedListener) satisfiedAsyncListenWait(channel string) {
	ml.assertQueuedRequest()
	rq := ml.pop()
	ml.assert(rq.channel == channel, "satisfiedAsyncListenWait")
	rq.s3 <- struct{}{}
	<-rq.s4
}

// operations for dealing with requestQueue; don't assert() since we might be
// called from another goroutine
func (ml *mockedListener) top() mlListenRequest {
	ml.Lock()
	if len(ml.requestQueue) <= 0 {
		panicf("%d <= 0", len(ml.requestQueue))
	}
	item := ml.requestQueue[0]
	ml.Unlock()
	return item
}

func (ml *mockedListener) pop() mlListenRequest {
	ml.Lock()
	if len(ml.requestQueue) <= 0 {
		panicf("%d <= 0", len(ml.requestQueue))
	}
	item := ml.requestQueue[0]
	ml.requestQueue = ml.requestQueue[1:]
	ml.Unlock()
	return item
}

func (ml *mockedListener) push(rq mlListenRequest) {
	ml.Lock()
	ml.requestQueue = append(ml.requestQueue, rq)
	ml.Unlock()
}

func (ml *mockedListener) accrueListenError(err error) {
	ml.listenErrors = append(ml.listenErrors, err)
}

func (ml *mockedListener) acceptListenError(expected error) {
	ml.assert(len(ml.listenErrors) > 0, "must have a listen error")
	got := ml.listenErrors[0]
	ml.listenErrors = ml.listenErrors[1:]
	ml.assert(got.Error() == expected.Error(),
			  fmt.Sprintf("%q must be %q", got.Error(), expected.Error()))
}

// The following two methods satisfy the first queued request, which must be of
// the type and on the channel specified in the call.
func (ml *mockedListener) satisfyListenErr(channel string, err error) {
	ml.assertQueuedRequest()
	rq := ml.top()
	ml.assert(rq.channel == channel && !rq.unlisten,
			  fmt.Sprintf("%q must be %q and %v must be false", rq.channel, channel, rq.unlisten))
	rq.s2 <- err
	<-rq.s4
}
func (ml *mockedListener) satisfyListen(channel string) {
	ml.satisfyListenErr(channel, nil)
}
func (ml *mockedListener) satisfyUnlistenErr(channel string, err error) {
	ml.assertQueuedRequest()
	rq := ml.top()
	ml.assert(rq.channel == channel && rq.unlisten,
			  fmt.Sprintf("%q must be %q and %v must be true", rq.channel, channel, rq.unlisten))
	rq.s2 <- err
	<-rq.s4
}
func (ml *mockedListener) satisfyUnlisten(channel string) {
	ml.satisfyUnlistenErr(channel, nil)
}
// expect a call to Unlisten() not initiated by us
func (ml *mockedListener) expectUnlisten(channel string) {
	s13 := make(chan struct{}, 2)
	s2 := make(chan error, 1)
	s2 <- nil
	ml.push(mlListenRequest{channel, true, s13, s2, s13, nil})
}

// sends a notification on the specified channel over to the NotifyDispatcher
func (ml *mockedListener) notify(channel string) {
	ml.notifyCh <- &pq.Notification{Channel: channel}
	// give the dispatcher some time to do its thing
	yield()
}

func (ml *mockedListener) listen(nd *NotifyDispatcher, channel string, ch chan<- *pq.Notification, flags ...waitFlags) {
	s1 := make(chan struct{}, 1)
	s2 := make(chan error, 1)
	s3 := make(chan struct{}, 1)
	s4 := make(chan struct{}, 1)
	ml.push(mlListenRequest{channel, false, s1, s2, s3, s4})
	go func() {
		err := nd.Listen(channel, ch)
		if err != nil {
			ml.accrueListenError(err)
		}
		<-s3
		s4 <- struct{}{}
	}()
	// assume only noWait is implemented
	if len(flags) == 0 {
		<-s1
	}
}

func (ml *mockedListener) unlisten(nd *NotifyDispatcher, channel string, ch chan<- *pq.Notification, flags ...waitFlags) {
	s1 := make(chan struct{}, 1)
	s2 := make(chan error, 1)
	s3 := make(chan struct{}, 1)
	s4 := make(chan struct{}, 1)
	ml.push(mlListenRequest{channel, true, s1, s2, s3, s4})
	go func() {
		err := nd.Unlisten(channel, ch)
		if err != nil {
			panic(err)
		}
		<-s3
		s4 <- struct{}{}
	}()
	// assume only noWait is implemented
	if len(flags) == 0 {
		<-s1
	}
}

func (ml *mockedListener) broadcast() {
	ml.notifyCh <- nil
}


// Implementation of the Listener interface.  Don't use assert() since these
// won't be executed on the test goroutine.
func (ml *mockedListener) Listen(channel string) error {
	item := ml.top()
	if item.channel != channel || item.unlisten {
		panicf("%s != %s || %v", item.channel, channel, item.unlisten)
	}
	// wake up listen(), wait for satisfyListen()
	item.s1 <- struct{}{}
	err := <-item.s2
	ml.pop()
	item.s3 <- struct{}{}
	return err
}

func (ml *mockedListener) Unlisten(channel string) error {
	item := ml.top()
	if item.channel != channel || !item.unlisten {
		panicf("%s != %s || !%v", item.channel, channel, item.unlisten)
	}
	// wake up unlisten(), wait for satisfyUnlisten()
	item.s1 <- struct{}{}
	err := <-item.s2
	ml.pop()
	item.s3 <- struct{}{}
	return err
}

func (ml *mockedListener) NotificationChannel() <-chan *pq.Notification {
	return ml.notifyCh
}


func testSetup(t *testing.T) (*NotifyDispatcher, *mockedListener) {
	notifyCh := make(chan *pq.Notification)
	ml := &mockedListener{t: t, notifyCh: notifyCh}
	nd := NewNotifyDispatcher(ml)
	return nd, ml
}

func endTest(t *testing.T, nd *NotifyDispatcher, ml *mockedListener) {
	ml.assertEmptyQueue()
	ml.assertNoListenErrors()
	assertEmptyCh(t, ml.notifyCh, "must not have any queued notifications at the end of the test")
	err := nd.Close()
	if err != nil {
		t.Fatal(err)
	}
}


func TestBasics(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch := make(chan *pq.Notification, 1)

	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	ml.notify("bar")
	assertEmptyCh(t, ch, "not listening on bar")

	ml.unlisten(nd, "foo", ch)
	// it's undefined whether a notification here would be delivered or not
	ml.satisfyUnlisten("foo")
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active anymore")
}

func TestMultipleChannelsOnMultipleGoChannels(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	foo := make(chan *pq.Notification, 1)
	bar := make(chan *pq.Notification, 1)

	// activate foo
	ml.notify("foo")
	assertEmptyCh(t, foo, "not listening yet")
	ml.listen(nd, "foo", foo)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, foo, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, foo, "foo", "listen request satisfied")

	// activate bar
	ml.notify("bar")
	assertEmptyCh(t, foo, "not listening on bar")
	ml.listen(nd, "bar", bar)

	yield()
	ml.notify("bar")
	assertEmptyCh(t, bar, "set not active yet")

	ml.satisfyListen("bar")
	ml.assertEmptyQueue()
	ml.notify("bar")
	assertNotification(t, bar, "bar", "listen request satisfied")

	// deliver some more stuff
	ml.notify("foo")
	ml.notify("bar")
	assertNotification(t, foo, "foo", "set fully active")
	assertNotification(t, bar, "bar", "set fully active")

	ml.notify("bar")
	ml.notify("foo")
	assertNotification(t, bar, "bar", "set fully active")
	assertNotification(t, foo, "foo", "set fully active")
}

func TestMultipleChannelsOnSingleGoChannel(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch := make(chan *pq.Notification, 2)

	// activate foo
	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	// activate bar
	ml.notify("bar")
	assertEmptyCh(t, ch, "not listening on bar")
	ml.listen(nd, "bar", ch)

	yield()
	ml.notify("bar")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("bar")
	ml.assertEmptyQueue()
	ml.notify("bar")
	assertNotification(t, ch, "bar", "listen request satisfied")

	// deliver some more stuff
	ml.notify("foo")
	ml.notify("bar")
	assertNotification(t, ch, "foo", "set fully active")
	assertNotification(t, ch, "bar", "set fully active")

	ml.notify("bar")
	ml.notify("foo")
	assertNotification(t, ch, "bar", "set fully active")
	assertNotification(t, ch, "foo", "set fully active")
}

func TestMultipleGoChannelsOnSingleChannel(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch1 := make(chan *pq.Notification, 1)
	ch2 := make(chan *pq.Notification, 1)

	// activate ch1
	ml.notify("foo")
	assertEmptyCh(t, ch1, "not listening yet")
	ml.listen(nd, "foo", ch1)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "listen request satisfied")

	// activate ch2; nd.Listen() should not block
	assert(t, nd.Listen("foo", ch2) == nil, "another listener on the same channel")

	yield()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set still active for ch1")
	assertNotification(t, ch2, "foo", "set immediately active for ch2")

	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set fully active")
	assertNotification(t, ch2, "foo", "set fully active")

	// deactivate ch2; nd.Unlisten() should not block
	assert(t, nd.Unlisten("foo", ch2) == nil, "remove one listener on a shared channel")

	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set still active for ch1")
	assertEmptyCh(t, ch2, "ch2 not in set anymore")

	ml.unlisten(nd, "foo", ch1)
	ml.satisfyUnlisten("foo")
	ml.notify("foo")
	assertEmptyCh(t, ch1, "ch1 not in set anymore")
	assertEmptyCh(t, ch2, "ch2 not in set anymore")
}


func TestListenUnlistenListenRaceCondition(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch := make(chan *pq.Notification, 1)

	// activate foo
	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	// start an unlisten, but before it finishes, listen again
	ml.unlisten(nd, "foo", ch)
	ml.listen(nd, "foo", ch, noWait)
	ml.satisfyUnlisten("foo")
	ml.satisfyListen("foo")
}

// Test two Listen() calls being satisfied with one LISTEN
func TestDoubleListenSingleLISTEN(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch1 := make(chan *pq.Notification, 1)
	ch2 := make(chan *pq.Notification, 1)

	ml.listen(nd, "foo", ch1)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active")

	ml.listen(nd, "foo", ch2, noWait)

	ml.satisfyListen("foo")
	ml.satisfiedAsyncListenWait("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")

	assertNotification(t, ch1, "foo", "listen request satisfied")
	assertNotification(t, ch2, "foo", "listen request satisfied")
}

// Test that ErrChannelAlreadyOpen is not treated as an error.
func TestListenErrChannelAlreadyOpen(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch := make(chan *pq.Notification, 1)

	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")

	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListenErr("foo", pq.ErrChannelAlreadyOpen)
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")
}

// Test error responses to LISTEN command
func TestListenError(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	listenErr := fmt.Errorf("oopsie daisies")

	ch1 := make(chan *pq.Notification, 1)
	ch2 := make(chan *pq.Notification, 1)

	ml.notify("foo")
	assertEmptyCh(t, ch1, "not listening yet")

	ml.listen(nd, "foo", ch1)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active")

	ml.satisfyListenErr("foo", listenErr)
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active")

	ml.acceptListenError(listenErr)
	ml.assertNoListenErrors()

	ml.listen(nd, "foo", ch1)
	ml.listen(nd, "foo", ch2, noWait)

	ml.satisfyListenErr("foo", listenErr)
	ml.satisfiedAsyncListenWait("foo")
	ml.assertEmptyQueue()

	ml.acceptListenError(listenErr)
	ml.acceptListenError(listenErr)
	ml.assertNoListenErrors()

	ml.listen(nd, "foo", ch1)
	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "listen request satisfied")
}

// Test that errors from Unlisten are ignored.
func TestUnlistenErr(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch := make(chan *pq.Notification, 1)

	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	ml.unlisten(nd, "foo", ch)
	ml.satisfyUnlistenErr("foo", fmt.Errorf("this error should be ignored"))
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active anymore")
}

func TestCloseSlowReaders(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch1 := make(chan *pq.Notification, 1)
	ch2 := make(chan *pq.Notification, 2)

	// activate ch1
	ml.notify("foo")
	assertEmptyCh(t, ch1, "not listening yet")
	ml.listen(nd, "foo", ch1)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "listen request satisfied")

	// activate ch2; nd.Listen() should not block
	assert(t, nd.Listen("foo", ch2) == nil, "another listener on the same channel")

	yield()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set still active for ch1")
	assertNotification(t, ch2, "foo", "set immediately active for ch2")

	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set fully active")
	assertNotification(t, ch2, "foo", "set fully active")

	assertEmptyCh(t, ch1, "no queued notifications")
	assertEmptyCh(t, ch2, "no queued notifications")

	ml.notify("foo")
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "first notification")
	assertNotification(t, ch2, "foo", "first notification")
	assertClosedCh(t, ch1, "slow reader; should be closed")
	assertNotification(t, ch2, "foo", "not a slower reader")

	ml.notify("foo")
	ml.notify("foo")
	ml.expectUnlisten("foo")
	ml.notify("foo")
	assertNotification(t, ch2, "foo", "first notification")
	assertNotification(t, ch2, "foo", "second notification")
	assertClosedCh(t, ch2, "slow reader; should be closed")
}

func TestNeglectSlowReaders(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	nd.SetSlowReaderEliminationStrategy(NeglectSlowReaders)

	ch := make(chan *pq.Notification, 1)

	// activate ch
	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	ml.notify("foo")
	ml.notify("foo")
	assertNotification(t, ch, "foo", "first notification")
	assertEmptyCh(t, ch, "not closed, but notification was lost")
	ml.notify("foo")
	assertNotification(t, ch, "foo", "notifications are still delivered")
}

func TestExportedErrors(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	nd.SetSlowReaderEliminationStrategy(NeglectSlowReaders)

	ch := make(chan *pq.Notification, 1)

	// activate ch
	ml.notify("foo")
	assertEmptyCh(t, ch, "not listening yet")
	ml.listen(nd, "foo", ch)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch, "foo", "listen request satisfied")

	err := nd.Listen("foo", ch)
	if err != ErrChannelAlreadyActive {
		t.Fatalf("expected ErrChannelAlreadyActive; got %+#v", err)
	}
	err = nd.Unlisten("bar", make(chan *pq.Notification))
	if err != ErrChannelNotActive {
		t.Fatalf("expected ErrChannelNotActive; got %+#v", err)
	}
	err = nd.Unlisten("foo", make(chan *pq.Notification))
	if err != ErrChannelNotActive {
		t.Fatalf("expected ErrChannelNotActive; got %+#v", err)
	}
}

func TestBroadcastChannels(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch1 := nd.OpenBroadcastChannel()
	defer nd.CloseBroadcastChannel(ch1)
	ch2 := nd.OpenBroadcastChannel()
	defer nd.CloseBroadcastChannel(ch2)

	ml.broadcast()
	assertBroadcastNotification(t, ch1, "broadcast")
	assertBroadcastNotification(t, ch2, "broadcast")

	ml.broadcast()
	assertBroadcastNotification(t, ch1, "broadcast")
	ml.broadcast()
	assertBroadcastNotification(t, ch1, "broadcast")
	assertBroadcastNotification(t, ch2, "broadcast")

	assertEmptyBroadcastCh(t, ch1, "buffer of 1")
	assertEmptyBroadcastCh(t, ch2, "buffer of 1")
}

func TestCloseSlowReadersOnBroadcast(t *testing.T) {
	nd, ml := testSetup(t)
	defer endTest(t, nd, ml)

	ch1 := make(chan *pq.Notification, 1)
	ch2 := make(chan *pq.Notification, 1)
	ch3 := make(chan *pq.Notification, 1)

	// activate ch1
	ml.notify("foo")
	assertEmptyCh(t, ch1, "not listening yet")
	ml.listen(nd, "foo", ch1)

	yield()
	ml.notify("foo")
	assertEmptyCh(t, ch1, "set not active yet")

	ml.satisfyListen("foo")
	ml.assertEmptyQueue()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "listen request satisfied")

	// activate ch2; nd.Listen() should not block
	assert(t, nd.Listen("foo", ch2) == nil, "another listener on the same channel")

	yield()
	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set still active for ch1")
	assertNotification(t, ch2, "foo", "set immediately active for ch2")

	ml.notify("foo")
	assertNotification(t, ch1, "foo", "set fully active")
	assertNotification(t, ch2, "foo", "set fully active")

	assertEmptyCh(t, ch1, "no queued notifications")
	assertEmptyCh(t, ch2, "no queued notifications")

	ml.broadcast()
	ml.expectUnlisten("foo")
	ml.broadcast()
	assertNotification(t, ch1, nil, "first notification")
	assertNotification(t, ch2, nil, "first notification")
	assertClosedCh(t, ch1, "slow reader; should be closed")
	assertClosedCh(t, ch2, "slow reader; should be closed")

	// now with just one channel
	ml.notify("bar")
	assertEmptyCh(t, ch3, "not listening yet")
	ml.listen(nd, "bar", ch3)

	yield()
	ml.notify("bar")
	assertEmptyCh(t, ch3, "set not active yet")

	ml.satisfyListen("bar")
	ml.assertEmptyQueue()
	ml.notify("bar")
	assertNotification(t, ch3, "bar", "listen request satisfied")

	ml.broadcast()
	ml.expectUnlisten("bar")
	ml.broadcast()
	assertNotification(t, ch3, nil, "first notification")
	assertClosedCh(t, ch3, "slow reader; should be closed")
}

