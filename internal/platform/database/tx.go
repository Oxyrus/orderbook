package database

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
)

type txKey struct{}

func txFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// InTx reports whether ctx carries an open transaction.
func InTx(ctx context.Context) bool {
	_, ok := txFromContext(ctx)
	return ok
}

// TxOptions controls how a transaction is started.
type TxOptions struct {
	IsoLevel   pgx.TxIsoLevel
	AccessMode pgx.TxAccessMode
	// MaxRetries is the number of times the whole transaction function is
	// retried after a serialization failure or deadlock. Only effective for
	// the outermost transaction. Your fn MUST be idempotent with respect to
	// side effects outside the database when MaxRetries > 0.
	MaxRetries int
}

// Transactor runs functions inside a database transaction. Services depend on
// this interface so they can be unit-tested with a fake.
type Transactor interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

// WithTx runs fn in a READ COMMITTED transaction. See WithTxOptions.
func (db *DB) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return db.WithTxOptions(ctx, TxOptions{}, fn)
}

// WithTxOptions runs fn inside a transaction bound to the context passed to
// fn. Every repository call made with that context participates in the
// transaction.
//
//   - fn returns nil  -> commit
//   - fn returns err  -> rollback, err is returned
//   - fn panics       -> rollback, panic is re-raised
//
// If ctx already carries a transaction, fn simply joins it (no nested
// transaction / savepoint is created) and the outermost caller decides the
// outcome. Use Savepoint explicitly if you need partial rollbacks.
func (db *DB) WithTxOptions(ctx context.Context, opts TxOptions, fn func(ctx context.Context) error) error {
	if InTx(ctx) {
		return fn(ctx)
	}

	pgxOpts := pgx.TxOptions{IsoLevel: opts.IsoLevel, AccessMode: opts.AccessMode}
	for attempt := 0; ; attempt++ {
		err := db.runTx(ctx, pgxOpts, fn)
		if err == nil {
			return nil
		}
		if attempt >= opts.MaxRetries || !isRetryable(err) || ctx.Err() != nil {
			return err
		}
		// Jittered exponential backoff: 10ms, 20ms, 40ms ... capped at ~640ms.
		backoff := time.Duration(10<<min(attempt, 6))*time.Millisecond +
			time.Duration(rand.IntN(10))*time.Millisecond //nolint:gosec // jitter, not security sensitive
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(backoff):
		}
	}
}

func (db *DB) runTx(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context) error) (err error) {
	tx, err := db.Pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// Rollback must run even if ctx was cancelled.
	rollbackCtx := context.WithoutCancel(ctx)
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(rollbackCtx)
			panic(p)
		}
	}()

	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		if rbErr := tx.Rollback(rollbackCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// Savepoint runs fn inside a nested transaction (SAVEPOINT) of the
// transaction bound to ctx. If fn fails only the work done inside fn is
// rolled back. It returns an error if ctx carries no transaction.
func Savepoint(ctx context.Context, fn func(ctx context.Context) error) error {
	outer, ok := txFromContext(ctx)
	if !ok {
		return errors.New("savepoint: no transaction in context")
	}
	return pgx.BeginFunc(ctx, outer, func(inner pgx.Tx) error {
		return fn(context.WithValue(ctx, txKey{}, inner))
	})
}

func isRetryable(err error) bool {
	return IsCode(err, CodeSerializationFailure) || IsCode(err, CodeDeadlockDetected)
}
