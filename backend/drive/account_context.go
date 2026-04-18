package drive

import (
	"context"
	"errors"
)

type accountRuntimeContextKey struct{}

var accountRuntimeKey accountRuntimeContextKey

func bindAccountForWriteObject(ctx context.Context, f *Fs) (context.Context, *accountRuntime, error) {
	if f == nil {
		return contextOrBackground(ctx), nil, errors.New("drive: fs is nil")
	}

	ctx = contextOrBackground(ctx)
	if runtime := contextBoundRuntime(ctx); runtime != nil && f.runtimeBelongsToPool(runtime) {
		return ctx, runtime, nil
	}

	if f.accountPool == nil {
		return ctx, nil, nil
	}

	runtime, err := f.accountPool.selectAccount(ctx, operationPathWrite)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, accountRuntimeKey, runtime), runtime, nil
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
