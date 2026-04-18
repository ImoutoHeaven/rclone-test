package drive

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var errNoAvailableAccount = errors.New("drive: no available account")

type operationPathKind int

const (
	operationPathRead operationPathKind = iota
	operationPathWrite
)

type accountPool struct {
	policy        string
	rrCursor      uint64
	rrMu          sync.Mutex
	accounts      []*accountRuntime
	randMu        sync.Mutex
	randSource    *rand.Rand
	nowFn         func() time.Time
	waitForWakeFn func(context.Context, time.Duration) error
}

func newAccountPool(policy string, accounts []*accountRuntime) *accountPool {
	normalizedPolicy := policy
	if normalizedPolicy == "" {
		normalizedPolicy = "round_robin"
	}
	return &accountPool{
		policy:     normalizedPolicy,
		accounts:   accounts,
		randSource: rand.New(rand.NewSource(time.Now().UnixNano())),
		nowFn: func() time.Time {
			return time.Now().UTC()
		},
		waitForWakeFn: sleepWithContext,
	}
}

func (p *accountPool) nowUTC() time.Time {
	if p != nil && p.nowFn != nil {
		return p.nowFn().UTC()
	}
	return time.Now().UTC()
}

func (p *accountPool) waitForWake(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if p != nil && p.waitForWakeFn != nil {
		return p.waitForWakeFn(ctx, d)
	}
	return sleepWithContext(ctx, d)
}

func (p *accountPool) selectAccount(ctx context.Context, pathKind operationPathKind) (*accountRuntime, error) {
	for {
		now := p.nowUTC()
		switch p.policy {
		case "random":
			eligible := p.eligibleAccounts(pathKind, now)
			if len(eligible) > 0 {
				return p.selectRandom(eligible)
			}
		default:
			if runtime, ok := p.selectRoundRobin(pathKind, now); ok {
				return runtime, nil
			}
		}

		if pathKind != operationPathWrite {
			return nil, errNoAvailableAccount
		}

		wait, canWait, disabledCount := p.waitDurationUntilEarliestWriteWake(now)
		if canWait {
			if err := p.waitForWake(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}
		if disabledCount == len(p.accounts) {
			return nil, errNoAvailableAccount
		}
		return nil, errNoAvailableAccount
	}
}

func (p *accountPool) selectRoundRobin(pathKind operationPathKind, now time.Time) (*accountRuntime, bool) {
	if p == nil || len(p.accounts) == 0 {
		return nil, false
	}

	p.rrMu.Lock()
	defer p.rrMu.Unlock()

	accountCount := len(p.accounts)
	start := int(atomic.LoadUint64(&p.rrCursor) % uint64(accountCount))
	for offset := 0; offset < accountCount; offset++ {
		index := (start + offset) % accountCount
		runtime := p.accounts[index]
		if !p.isEligible(runtime, pathKind, now) {
			continue
		}
		atomic.StoreUint64(&p.rrCursor, uint64((index+1)%accountCount))
		return runtime, true
	}

	return nil, false
}

func (p *accountPool) selectRandom(eligible []*accountRuntime) (*accountRuntime, error) {
	if len(eligible) == 0 {
		return nil, errNoAvailableAccount
	}
	p.randMu.Lock()
	if p.randSource == nil {
		p.randSource = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	idx := p.randSource.Intn(len(eligible))
	p.randMu.Unlock()
	selected := eligible[idx]
	if selected == nil {
		return nil, errNoAvailableAccount
	}
	return selected, nil
}

func (p *accountPool) eligibleAccounts(pathKind operationPathKind, now time.Time) []*accountRuntime {
	if p == nil || len(p.accounts) == 0 {
		return nil
	}
	eligible := make([]*accountRuntime, 0, len(p.accounts))
	for _, account := range p.accounts {
		if p.isEligible(account, pathKind, now) {
			eligible = append(eligible, account)
		}
	}
	return eligible
}

func (p *accountPool) isEligible(account *accountRuntime, pathKind operationPathKind, now time.Time) bool {
	if account == nil {
		return false
	}
	switch pathKind {
	case operationPathWrite:
		return account.isWriteEligible(now)
	default:
		return account.isReadEligible()
	}
}

func (p *accountPool) waitDurationUntilEarliestWriteWake(now time.Time) (time.Duration, bool, int) {
	if p == nil || len(p.accounts) == 0 {
		return 0, false, 0
	}

	disabledCount := 0
	hasSleeping := false
	var earliestWake time.Time

	for _, account := range p.accounts {
		if account == nil {
			disabledCount++
			continue
		}
		if account.isDisabled() {
			disabledCount++
			continue
		}
		wake := account.uploadSleepUntilUTC()
		if wake.IsZero() || !now.UTC().Before(wake) {
			continue
		}
		if !hasSleeping || wake.Before(earliestWake) {
			hasSleeping = true
			earliestWake = wake
		}
	}

	if !hasSleeping {
		return 0, false, disabledCount
	}

	d := earliestWake.Sub(now.UTC())
	if d <= 0 {
		return 0, true, disabledCount
	}
	return d, true, disabledCount
}

func (p *accountPool) String() string {
	if p == nil {
		return "accountPool<nil>"
	}
	return fmt.Sprintf("accountPool{policy=%s accounts=%d}", p.policy, len(p.accounts))
}
