package drive

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"golang.org/x/oauth2"
	drive_v2 "google.golang.org/api/drive/v2"
	drive "google.golang.org/api/drive/v3"
)

type refreshErrorClass int

const (
	refreshErrorNone refreshErrorClass = iota
	refreshErrorTransient
	refreshErrorFatal
)

var fatalRefreshErrorReasons = []string{
	"invalid_grant",
	"invalid_client",
	"unauthorized_client",
	"unsupported_grant_type",
	"invalid_scope",
}

type accountRuntimeState struct {
	mu                               sync.RWMutex
	active                           bool
	disabled                         bool
	dayStartUTC                      time.Time
	usedBytes                        int64
	uploadSleepUntilUTC              time.Time
	resetQuotaOnNextSuccessfulUpload bool
}

type accountRuntime struct {
	index                int
	name                 string
	client               *http.Client
	svc                  *drive.Service
	v2Svc                *drive_v2.Service
	pacer                *fs.Pacer
	uploadDailyLimit     fs.SizeSuffix
	state                accountRuntimeState
	namespaceAccessCheck func(context.Context, namespaceTarget) error
}

func newAccountRuntime(index int, name string, client *http.Client, svc *drive.Service, v2Svc *drive_v2.Service, pacer *fs.Pacer, uploadDailyLimit fs.SizeSuffix) *accountRuntime {
	now := time.Now().UTC()
	return &accountRuntime{
		index:            index,
		name:             name,
		client:           client,
		svc:              svc,
		v2Svc:            v2Svc,
		pacer:            pacer,
		uploadDailyLimit: uploadDailyLimit,
		state: accountRuntimeState{
			active:      true,
			dayStartUTC: utcDayStart(now),
		},
	}
}

func (rt *accountRuntime) isDisabled() bool {
	if rt == nil {
		return true
	}
	rt.state.mu.RLock()
	defer rt.state.mu.RUnlock()
	return rt.state.disabled
}

func (rt *accountRuntime) setDisabled(disabled bool) {
	if rt == nil {
		return
	}
	rt.state.mu.Lock()
	rt.state.disabled = disabled
	if disabled {
		rt.state.active = false
	}
	rt.state.mu.Unlock()
}

func (rt *accountRuntime) setUploadSleepUntilUTC(wake time.Time) {
	if rt == nil {
		return
	}
	rt.state.mu.Lock()
	rt.state.uploadSleepUntilUTC = wake.UTC()
	rt.state.mu.Unlock()
}

func (rt *accountRuntime) markQuotaResetOnNextSuccess() {
	if rt == nil {
		return
	}
	rt.state.mu.Lock()
	rt.state.resetQuotaOnNextSuccessfulUpload = true
	rt.state.mu.Unlock()
}

func (rt *accountRuntime) consumeQuotaResetOnNextSuccess() bool {
	if rt == nil {
		return false
	}
	rt.state.mu.Lock()
	defer rt.state.mu.Unlock()
	shouldReset := rt.state.resetQuotaOnNextSuccessfulUpload
	rt.state.resetQuotaOnNextSuccessfulUpload = false
	return shouldReset
}

func (rt *accountRuntime) markUploadLimitSleep(wake time.Time) {
	if rt == nil {
		return
	}
	rt.state.mu.Lock()
	rt.state.uploadSleepUntilUTC = wake.UTC()
	rt.state.resetQuotaOnNextSuccessfulUpload = true
	rt.state.mu.Unlock()
}

func (rt *accountRuntime) uploadSleepUntilUTC() time.Time {
	if rt == nil {
		return time.Time{}
	}
	rt.state.mu.RLock()
	defer rt.state.mu.RUnlock()
	return rt.state.uploadSleepUntilUTC
}

func (rt *accountRuntime) isWriteEligible(now time.Time) bool {
	if rt == nil {
		return false
	}
	rt.state.mu.RLock()
	defer rt.state.mu.RUnlock()
	if rt.state.disabled {
		return false
	}
	if rt.state.uploadSleepUntilUTC.IsZero() {
		return true
	}
	return !now.UTC().Before(rt.state.uploadSleepUntilUTC)
}

func (rt *accountRuntime) isReadEligible() bool {
	if rt == nil {
		return false
	}
	rt.state.mu.RLock()
	defer rt.state.mu.RUnlock()
	return !rt.state.disabled
}

func classifyRefreshError(err error) refreshErrorClass {
	if err == nil {
		return refreshErrorNone
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return refreshErrorTransient
	}

	lowerErr := strings.ToLower(err.Error())
	for _, reason := range fatalRefreshErrorReasons {
		if strings.Contains(lowerErr, reason) {
			return refreshErrorFatal
		}
	}

	retrieveErr := &oauth2.RetrieveError{}
	if errors.As(err, &retrieveErr) {
		return refreshErrorTransient
	}

	if !(strings.Contains(lowerErr, "token") || strings.Contains(lowerErr, "oauth") || strings.Contains(lowerErr, "refresh")) {
		return refreshErrorNone
	}

	return refreshErrorTransient
}

func (rt *accountRuntime) disableOnFatalRefresh(err error) refreshErrorClass {
	classification := classifyRefreshError(err)
	if classification == refreshErrorFatal {
		rt.setDisabled(true)
	}
	return classification
}
