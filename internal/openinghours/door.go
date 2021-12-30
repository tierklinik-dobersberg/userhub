package openinghours

// TODO(ppacher): move all the parsing work away from this package to schema or utils.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tevino/abool"
)

// DoorState describes the current state of the entry door.
type DoorState string

// Possible door states.
const (
	Locked   = DoorState("locked")
	Unlocked = DoorState("unlocked")
)

// Reset types.
var (
	resetSoft = (*struct{})(nil)
	resetHard = &struct{}{}
)

type stateOverwrite struct {
	state       DoorState
	until       time.Time
	sessionUser string
}

// DoorController interacts with the entry door controller via MQTT.
type DoorController struct {
	*Controller

	// overwriteLock protects access to manualOverwrite.
	overwriteLock sync.Mutex

	// door is the actual interface to control the door.
	door DoorInterfacer

	// manualOverwrite is set when a user has manually overwritten
	// the current state of the entry door.
	manualOverwrite *stateOverwrite

	// stop is closed when the scheduler should stop.
	stop chan struct{}

	// reset triggers a reset of the scheduler.
	// A nil value means soft-reset while struct{}{}
	// is interpreted as a hard-reset causing a unlock-lock-unlock
	// sequence
	reset chan *struct{}

	// Whether or not a door reset is currently in progress.
	resetInProgress *abool.AtomicBool

	// wg is used to wait for door controller operations to finish.
	wg sync.WaitGroup
}

// NewDoorController returns a new door controller.
func NewDoorController(ohCtrl *Controller, door DoorInterfacer) (*DoorController, error) {
	dc := &DoorController{
		Controller:      ohCtrl,
		door:            door,
		stop:            make(chan struct{}),
		reset:           make(chan *struct{}),
		resetInProgress: abool.NewBool(false),
	}

	return dc, nil
}

// Overwrite overwrites the current door state with state until untilTime.
func (dc *DoorController) Overwrite(ctx context.Context, state DoorState, untilTime time.Time) error {
	log.From(ctx).V(7).Logf("overwritting door state to %s until %s", state, untilTime)

	if err := isValidState(state); err != nil {
		return err
	}

	dc.overwriteLock.Lock()
	{
		dc.manualOverwrite = &stateOverwrite{
			state:       state,
			sessionUser: "", // FIXME(ppacher)
			until:       untilTime,
		}
	}
	dc.overwriteLock.Unlock()

	// trigger a soft reset, unlocking above is REQUIRED
	// to avoid deadlocking with getManualOverwrite() in
	// scheduler() (which triggers immediately)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-dc.stop:
		return errors.New("stopped")
	case dc.reset <- resetSoft:
		log.From(ctx).V(6).Logf("door overwrite forcing %s until %s done", state, untilTime)
	}

	return nil
}

// Lock implements DoorInterfacer.
func (dc *DoorController) Lock(ctx context.Context) error {
	dc.wg.Add(1)
	defer dc.wg.Done()
	return dc.door.Lock(ctx)
}

// Unlock implements DoorInterfacer.
func (dc *DoorController) Unlock(ctx context.Context) error {
	dc.wg.Add(1)
	defer dc.wg.Done()
	return dc.door.Unlock(ctx)
}

// Open implements DoorInterfacer.
func (dc *DoorController) Open(ctx context.Context) error {
	dc.wg.Add(1)
	defer dc.wg.Done()
	return dc.door.Open(ctx)
}

// Start starts the scheduler for the door controller.
func (dc *DoorController) Start() error {
	dc.wg.Add(1)
	go dc.scheduler()

	return nil
}

// Stop requests the scheduler to stop and waits for all
// operations to complete.
func (dc *DoorController) Stop() error {
	close(dc.stop)

	dc.wg.Wait()

	return nil
}

// Reset triggers a reset of the door scheduler.
func (dc *DoorController) Reset(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-dc.stop:
		return errors.New("stopped")

	// trigger a hard-reset
	case dc.reset <- resetHard:
		return nil
	}
}

// resetDoor resets the entry door by unlocking, locking and unlocking
// it again. For whatever reason, this proved to work best when the door
// does not behave as it should.
func (dc *DoorController) resetDoor(ctx context.Context) {
	dc.wg.Add(1)
	defer dc.wg.Done()

	dc.resetInProgress.Set()
	defer dc.resetInProgress.UnSet()

	// remove any manual overwrite when we do a reset.
	dc.overwriteLock.Lock()
	dc.manualOverwrite = nil
	dc.overwriteLock.Unlock()

	log := log.From(ctx)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	if err := dc.door.Unlock(ctx); err != nil {
		log.Errorf("failed to unlock door: %s", err)
	}

	time.Sleep(time.Second * 2)
	if err := dc.door.Lock(ctx); err != nil {
		log.Errorf("failed to unlock door: %s", err)
	}

	time.Sleep(time.Second * 2)
	if err := dc.door.Unlock(ctx); err != nil {
		log.Errorf("failed to unlock door: %s", err)
	}
}

func (dc *DoorController) scheduler() {
	defer dc.wg.Done()
	var lastState DoorState
	var state DoorState

	const maxTriesLocked = 60
	const maxTriesUnlocked = 20

	retries := 0
	maxTries := maxTriesLocked
	// trigger immediately
	until := time.Now().Add(time.Second)

	for {
		ctx := context.Background()

		select {
		case <-dc.stop:
			return
		case hard := <-dc.reset:
			if hard != resetSoft {
				// reset the door state. it will unlock for a second or so.
				dc.resetDoor(ctx)
			}
			// force applying the door state.
			lastState = DoorState("")
		case <-time.After(time.Until(until)):

		// resend lock commands periodically as the door
		// might be open and may thus miss commands.
		case <-time.After(time.Minute):
		}

		ctx, cancel := context.WithTimeout(ctx, time.Second)

		var resetInProgress bool
		state, until, resetInProgress = dc.Current(ctx)

		// a reset may never be in progress at this point (because only this loop
		// executes a reset and it must have finished already)
		if resetInProgress {
			log.From(ctx).Errorf("BUG: a door reset is expected to be false")
		}

		if until.IsZero() {
			until = time.Now().Add(time.Minute * 5)
		}

		if state != lastState {
			retries = 0

			switch state {
			case Locked:
				maxTries = maxTriesLocked
			case Unlocked:
				maxTries = maxTriesUnlocked
			}
		}

		// only trigger when we need to change state.
		if retries < maxTries {
			retries++

			var err error
			switch state {
			case Locked:
				err = dc.Lock(ctx)
			case Unlocked:
				err = dc.Unlock(ctx)
			default:
				log.From(ctx).Errorf("invalid door state returned by Current(): %s", string(state))
				cancel()
				continue
			}

			if err != nil {
				log.From(ctx).Errorf("failed to set desired door state %s: %s", string(state), err)
			} else {
				lastState = state
			}
		}
		cancel()
	}
}

// Current returns the current door state.
func (dc *DoorController) Current(ctx context.Context) (DoorState, time.Time, bool) {
	state, until := dc.stateFor(ctx, time.Now().In(dc.Location()))

	return state, until, dc.resetInProgress.IsSet()
}

// StateFor returns the desired door state for the time t.
// It makes sure t is in the correct location. Like in ChangeOnDuty, the
// caller must make sure that t is in the desired timezone as StateFor will copy
// hour and date information.
func (dc *DoorController) StateFor(ctx context.Context, t time.Time) (DoorState, time.Time) {
	return dc.stateFor(ctx, t)
}

func (dc *DoorController) stateFor(ctx context.Context, t time.Time) (DoorState, time.Time) {
	log := log.From(ctx)
	// if we have an active overwrite we need to return it
	// together with it's end time.
	if overwrite := dc.getManualOverwrite(); overwrite != nil && overwrite.until.After(t) {
		log.Infof("using manual door overwrite %q by %q until %s", overwrite.state, overwrite.sessionUser, overwrite.until)
		return overwrite.state, overwrite.until
	}

	// we need one frame because we might be in the middle
	// of it or before it.
	upcoming := dc.UpcomingFrames(ctx, t, 1)
	if len(upcoming) == 0 {
		return Locked, time.Time{} // forever locked as there are no frames ...
	}

	f := upcoming[0]

	// if we are t is covered by f than should be unlocked
	// until the end of f.
	if f.Covers(t) {
		return Unlocked, f.To
	}

	// Otherwise there's no active frame so we are locked until
	// f starts.
	return Locked, f.From
}

func (dc *DoorController) getManualOverwrite() *stateOverwrite {
	dc.overwriteLock.Lock()
	defer dc.overwriteLock.Unlock()

	return dc.manualOverwrite
}

func sortAndValidate(os []OpeningHour) error {
	sort.Sort(OpeningHourSlice(os))

	// it's already guaranteed that each To is after the respective From
	// value (see utils.ParseDayTime) and the slice is sorted by asc From
	// time. Therefore, we only need to check if there's a To time that's
	// after the From time of the next time range.
	for i := 0; i < len(os)-1; i++ {
		current := os[i]
		next := os[i+1]

		if current.EffectiveClose() >= next.EffectiveOpen() {
			return fmt.Errorf("overlapping time frames %s and %s", current, next)
		}
	}

	return nil
}

func isValidState(state DoorState) error {
	switch state {
	case Locked, Unlocked:
		return nil
	}

	return fmt.Errorf("invalid door state: %s", state)
}
