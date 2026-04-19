package drive

import (
	"context"
	"errors"
	"sync/atomic"
)

type accountRuntimeContextKey struct{}

type uploadProbeStateContextKey struct{}

type uploadProbeState struct {
	resetQuotaOnSuccess atomic.Bool
}

var accountRuntimeKey accountRuntimeContextKey
var uploadProbeStateKey uploadProbeStateContextKey

func bindAccountForWriteObject(ctx context.Context, f *Fs) (context.Context, *accountRuntime, error) {
	if f == nil {
		return contextOrBackground(ctx), nil, errors.New("drive: fs is nil")
	}

	ctx = contextOrBackground(ctx)
	if runtime := contextBoundRuntime(ctx); runtime != nil && f.runtimeBelongsToPool(runtime) {
		return withUploadProbeState(ctx), runtime, nil
	}

	if f.accountPool == nil {
		return ctx, nil, nil
	}

	runtime, err := f.accountPool.selectAccount(ctx, operationPathWrite)
	if err != nil {
		return ctx, nil, err
	}
	ctx = context.WithValue(ctx, accountRuntimeKey, runtime)
	ctx = withFreshUploadProbeState(ctx)
	return ctx, runtime, nil
}

func bindAccountForReadCall(ctx context.Context, f *Fs) (context.Context, *accountRuntime, error) {
	if f == nil {
		return contextOrBackground(ctx), nil, errors.New("drive: fs is nil")
	}

	ctx = contextOrBackground(ctx)
	if f.accountPool == nil {
		return ctx, nil, nil
	}

	runtime, err := f.accountPool.selectAccount(ctx, operationPathRead)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, accountRuntimeKey, runtime), runtime, nil
}

func runtimeFromContext(ctx context.Context, f *Fs) (*accountRuntime, error) {
	if runtime := contextBoundRuntime(ctx); runtime != nil && f.runtimeBelongsToPool(runtime) {
		return runtime, nil
	}

	if f == nil {
		return nil, errors.New("drive: fs is nil")
	}
	if f.accountPool == nil {
		return nil, nil
	}

	return f.accountPool.selectAccount(contextOrBackground(ctx), operationPathRead)
}

func (f *Fs) runtimeBelongsToPool(runtime *accountRuntime) bool {
	if f == nil || f.accountPool == nil || runtime == nil {
		return false
	}
	for _, candidate := range f.accountPool.accounts {
		if candidate == runtime {
			return true
		}
	}
	return false
}

func contextBoundRuntime(ctx context.Context) *accountRuntime {
	if ctx == nil {
		return nil
	}
	runtime, _ := ctx.Value(accountRuntimeKey).(*accountRuntime)
	return runtime
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func withUploadProbeState(ctx context.Context) context.Context {
	ctx = contextOrBackground(ctx)
	if uploadProbeStateFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, uploadProbeStateKey, &uploadProbeState{})
}

func withFreshUploadProbeState(ctx context.Context) context.Context {
	ctx = contextOrBackground(ctx)
	return context.WithValue(ctx, uploadProbeStateKey, &uploadProbeState{})
}

func uploadProbeStateFromContext(ctx context.Context) *uploadProbeState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(uploadProbeStateKey).(*uploadProbeState)
	return state
}

func markUploadProbeQuotaReset(ctx context.Context) {
	state := uploadProbeStateFromContext(ctx)
	if state == nil {
		return
	}
	state.resetQuotaOnSuccess.Store(true)
}

func consumeUploadProbeQuotaReset(ctx context.Context) bool {
	state := uploadProbeStateFromContext(ctx)
	if state == nil {
		return false
	}
	return state.resetQuotaOnSuccess.Swap(false)
}
