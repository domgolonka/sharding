package sharding

import (
	"context"
	"database/sql"
	"errors"
	"gorm.io/gorm"
)

// ConnPool implements a ConnPool to replace db.Statement.ConnPool in GORM
type ConnPool struct {
	sharding *Sharding
	ConnPool gorm.ConnPool
}

func (pool *ConnPool) String() string {
	return "gorm:sharding:conn_pool"
}

func (pool *ConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return pool.ConnPool.PrepareContext(ctx, query)
}

func (pool *ConnPool) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	// Resolve the query using the sharding plugin
	_, stQuery, _, err := pool.sharding.resolve(query, args...)
	if err != nil {
		if errors.Is(err, ErrMissingShardingKey) {
			stQuery = query
		} else {
			return nil, err
		}
	}

	// Store the modified query
	pool.sharding.querys.Store("last_query", stQuery)

	// Execute the modified query
	result, err := pool.ConnPool.ExecContext(ctx, stQuery, args...)
	return result, err
}

func (pool *ConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	_, stQuery, _, err := pool.sharding.resolve(query, args...)
	if err != nil {
		if errors.Is(err, ErrMissingShardingKey) {
			stQuery = query
		} else {
			return nil, err
		}
	}

	pool.sharding.querys.Store("last_query", stQuery)

	rows, err := pool.ConnPool.QueryContext(ctx, stQuery, args...)
	return rows, err
}

func (pool *ConnPool) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	_, stQuery, _, err := pool.sharding.resolve(query, args...)
	if err != nil {
		if errors.Is(err, ErrMissingShardingKey) {
			// Proceed with the original query if sharding key is missing
			stQuery = query
		} else {
			// Log the error and proceed with the original query
			pool.sharding.Logger.Error(ctx, "sharding resolve error: %v", err)
			stQuery = query
		}
	}

	pool.sharding.querys.Store("last_query", stQuery)

	return pool.ConnPool.QueryRowContext(ctx, stQuery, args...)
}

// BeginTx implements ConnPoolBeginner.BeginTx
func (pool *ConnPool) BeginTx(ctx context.Context, opt *sql.TxOptions) (gorm.ConnPool, error) {
	if basePool, ok := pool.ConnPool.(gorm.ConnPoolBeginner); ok {
		return basePool.BeginTx(ctx, opt)
	}

	return pool, nil
}

// Commit implements TxCommitter.Commit
func (pool *ConnPool) Commit() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Commit()
	}

	return nil
}

// Rollback implements TxCommitter.Rollback
func (pool *ConnPool) Rollback() error {
	if _, ok := pool.ConnPool.(*sql.Tx); ok {
		return nil
	}

	if basePool, ok := pool.ConnPool.(gorm.TxCommitter); ok {
		return basePool.Rollback()
	}

	return nil
}

func (pool *ConnPool) Ping() error {
	return nil
}
