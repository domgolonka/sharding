package sharding

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/longbridgeapp/sqlparser"
	"gorm.io/gorm"
)

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("can not insert different suffix table in one query ")
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

type ASTNode interface{}

type SQLParser interface {
	Parse(query string) (ASTNode, error)
	ReplaceTableNames(ast ASTNode, replacements map[string]string) (string, error)
}

type Sharding struct {
	*gorm.DB
	ConnPool       *ConnPool
	configs        map[string]Config
	querys         sync.Map
	snowflakeNodes []*snowflake.Node

	_config Config
	_tables []any

	mutex sync.RWMutex
}

// Config specifies the configuration for sharding.
type Config struct {
	// When DoubleWrite enabled, data will double write to both main table and sharding table.
	DoubleWrite bool

	// ShardingKey specifies the table column you want to used for sharding the table rows.
	// For example, for a product order table, you may want to split the rows by `user_id`.
	ShardingKey string

	// NumberOfShards specifies how many tables you want to sharding.
	NumberOfShards uint

	// tableFormat specifies the sharding table suffix format.
	tableFormat string

	// ShardingAlgorithm specifies a function to generate the sharding
	// table's suffix by the column value.
	// For example, this function implements a mod sharding algorithm.
	//
	// 	func(value any) (suffix string, err error) {
	//		if uid, ok := value.(int64);ok {
	//			return fmt.Sprintf("_%02d", user_id % 64), nil
	//		}
	//		return "", errors.New("invalid user_id")
	// 	}
	ShardingAlgorithm func(columnValue any) (suffix string, err error)

	// ShardingSuffixs specifies a function to generate all table's suffix.
	// Used to support Migrator and generate PrimaryKey.
	// For example, this function get a mod all sharding suffixs.
	//
	// func () (suffixs []string) {
	// 	numberOfShards := 5
	// 	for i := 0; i < numberOfShards; i++ {
	// 		suffixs = append(suffixs, fmt.Sprintf("_%02d", i%numberOfShards))
	// 	}
	// 	return
	// }
	ShardingSuffixs func() (suffixs []string)

	// ShardingAlgorithmByPrimaryKey specifies a function to generate the sharding
	// table's suffix by the primary key. Used when no sharding key specified.
	// For example, this function use the Snowflake library to generate the suffix.
	//
	// 	func(id int64) (suffix string) {
	//		return fmt.Sprintf("_%02d", snowflake.ParseInt64(id).Node())
	//	}
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)

	// PrimaryKeyGenerator specifies the primary key generate algorithm.
	// Used only when insert and the record does not contains an id field.
	// Options are PKSnowflake, PKPGSequence and PKCustom.
	// When use PKCustom, you should also specify PrimaryKeyGeneratorFn.
	PrimaryKeyGenerator int

	// PrimaryKeyGeneratorFn specifies a function to generate the primary key.
	// When use auto-increment like generator, the tableIdx argument could ignored.
	// For example, this function use the Snowflake library to generate the primary key.
	// If you don't want to auto-fill the `id` or use a primary key that isn't called `id`, just return 0.
	//
	// 	func(tableIdx int64) int64 {
	//		return nodes[tableIdx].Generate().Int64()
	//	}
	PrimaryKeyGeneratorFn func(tableIdx int64) int64
}

func Register(config Config, tables ...any) *Sharding {
	return &Sharding{
		_config: config,
		_tables: tables,
	}
}

func (s *Sharding) compile() error {
	if s.configs == nil {
		s.configs = make(map[string]Config)
	}
	for _, table := range s._tables {
		if t, ok := table.(string); ok {
			s.configs[t] = s._config
		} else {
			stmt := &gorm.Statement{DB: s.DB}
			if err := stmt.Parse(table); err == nil {
				s.configs[stmt.Table] = s._config
			} else {
				return err
			}
		}
	}

	for t, c := range s.configs {
		if c.NumberOfShards > 1024 && c.PrimaryKeyGenerator == PKSnowflake {
			panic("Snowflake NumberOfShards should less than 1024")
		}

		if c.PrimaryKeyGenerator == PKSnowflake {
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		} else if c.PrimaryKeyGenerator == PKPGSequence {

			// Execute SQL to CREATE SEQUENCE for this table if not exist
			err := s.createPostgreSQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genPostgreSQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.createMySQLSequenceKeyIfNotExist(t)
			if err != nil {
				return err
			}

			c.PrimaryKeyGeneratorFn = func(index int64) int64 {
				return s.genMySQLSequenceKey(t, index)
			}
		} else if c.PrimaryKeyGenerator == PKCustom {
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when use PKCustom")
			}
		} else {
			return errors.New("PrimaryKeyGenerator can only be one of PKSnowflake, PKPGSequence, PKMySQLSequence and PKCustom")
		}

		if c.ShardingAlgorithm == nil {
			if c.NumberOfShards == 0 {
				return errors.New("specify NumberOfShards or ShardingAlgorithm")
			}
			if c.NumberOfShards < 10 {
				c.tableFormat = "_%01d"
			} else if c.NumberOfShards < 100 {
				c.tableFormat = "_%02d"
			} else if c.NumberOfShards < 1000 {
				c.tableFormat = "_%03d"
			} else if c.NumberOfShards < 10000 {
				c.tableFormat = "_%04d"
			}
			c.ShardingAlgorithm = func(value any) (suffix string, err error) {
				id := 0
				switch value := value.(type) {
				case int:
					id = value
				case int64:
					id = int(value)
				case string:
					id, err = strconv.Atoi(value)
					if err != nil {
						id = int(crc32.ChecksumIEEE([]byte(value)))
					}
				default:
					return "", fmt.Errorf("default algorithm only support integer and string column," +
						"if you use other type, specify you own ShardingAlgorithm")
				}

				return fmt.Sprintf(c.tableFormat, id%int(c.NumberOfShards)), nil
			}
		}

		if c.ShardingSuffixs == nil {
			c.ShardingSuffixs = func() (suffixs []string) {
				for i := 0; i < int(c.NumberOfShards); i++ {
					suffix, err := c.ShardingAlgorithm(i)
					if err != nil {
						return nil
					}
					suffixs = append(suffixs, suffix)
				}
				return
			}
		}

		if c.ShardingAlgorithmByPrimaryKey == nil {
			if c.PrimaryKeyGenerator == PKSnowflake {
				c.ShardingAlgorithmByPrimaryKey = func(id int64) (suffix string) {
					return fmt.Sprintf(c.tableFormat, snowflake.ParseInt64(id).Node())
				}
			}
		}
		s.configs[t] = c
	}

	return nil
}

// Name plugin name for Gorm plugin interface
func (s *Sharding) Name() string {
	return "gorm:sharding"
}

// LastQuery get last SQL query
func (s *Sharding) LastQuery() string {
	if query, ok := s.querys.Load("last_query"); ok {
		return query.(string)
	}

	return ""
}

// Initialize implement for Gorm plugin interface
func (s *Sharding) Initialize(db *gorm.DB) error {
	db.Dialector = NewShardingDialector(db.Dialector, s)

	s.DB = db
	s.registerCallbacks(db)

	for t, c := range s.configs {
		if c.PrimaryKeyGenerator == PKPGSequence {
			err := s.DB.Exec("CREATE SEQUENCE IF NOT EXISTS " + pgSeqName(t)).Error
			if err != nil {
				return fmt.Errorf("init postgresql sequence error, %w", err)
			}
		}
		if c.PrimaryKeyGenerator == PKMySQLSequence {
			err := s.DB.Exec("CREATE TABLE IF NOT EXISTS " + mySQLSeqName(t) + " (id INT NOT NULL)").Error
			if err != nil {
				return fmt.Errorf("init mysql create sequence error, %w", err)
			}
			err = s.DB.Exec("INSERT INTO " + mySQLSeqName(t) + " VALUES (0)").Error
			if err != nil {
				return fmt.Errorf("init mysql insert sequence error, %w", err)
			}
		}
	}

	s.snowflakeNodes = make([]*snowflake.Node, 1024)
	for i := int64(0); i < 1024; i++ {
		n, err := snowflake.NewNode(i)
		if err != nil {
			return fmt.Errorf("init snowflake node error, %w", err)
		}
		s.snowflakeNodes[i] = n
	}

	return s.compile()
}

func (s *Sharding) registerCallbacks(db *gorm.DB) {
	s.Callback().Create().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Query().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Update().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Delete().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Row().Before("*").Register("gorm:sharding", s.switchConn)
	s.Callback().Raw().Before("*").Register("gorm:sharding", s.switchConn)
}

func (s *Sharding) switchConn(db *gorm.DB) {
	// Support ignore sharding in some case, like:
	// When DoubleWrite is enabled, we need to query database schema
	// information by table name during the migration.
	if _, ok := db.Get(ShardingIgnoreStoreKey); !ok {
		s.mutex.Lock()
		if db.Statement.ConnPool != nil {
			s.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
			db.Statement.ConnPool = s.ConnPool
		}
		s.mutex.Unlock()
	}

}

type tableNameCollector struct {
	tables []*sqlparser.TableName
}

func (v *tableNameCollector) Visit(node sqlparser.Node) (w sqlparser.Visitor, err error) {
	// Continue traversal
	return v, nil
}

func (v *tableNameCollector) VisitEnd(node sqlparser.Node) error {
	switch n := node.(type) {
	case *sqlparser.TableName:
		v.tables = append(v.tables, n)
	}
	return nil
}

// Collect all table names from the AST
func collectTableNames(node sqlparser.Node) []*sqlparser.TableName {
	collector := &tableNameCollector{}
	sqlparser.Walk(collector, node)
	return collector.tables
}

func (s *Sharding) resolve(query string, args ...interface{}) (ftQuery, stQuery, tableName string, err error) {
	ftQuery = query
	stQuery = query
	if len(s.configs) == 0 {
		return
	}

	// Parse the SQL query into an AST
	var expr sqlparser.Statement
	expr, err = sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		return ftQuery, stQuery, tableName, nil
	}

	// Collect all table names from the AST
	tables := collectTableNames(expr)

	for _, tbl := range tables {
		tableName = tbl.Name.Name
		config, ok := s.configs[tableName]
		if !ok {
			continue
		}

		// Extract sharding key value
		var value interface{}
		var id int64
		var keyFound bool
		value, id, keyFound, err = s.extractShardingKeyValue(config.ShardingKey, tableName, expr, args...)
		if err != nil {
			return
		}

		// Compute table suffix
		var suffix string
		suffix, err = getSuffix(value, id, keyFound, config)
		if err != nil {
			return
		}

		newTableName := tableName + suffix

		// Replace table names in the AST
		replacer := &tableNameReplacer{
			oldName: tableName,
			newName: newTableName,
		}
		sqlparser.Walk(replacer, expr)
	}

	// Convert the modified AST back into a query string
	stQuery = expr.String()
	return
}

type tableNameReplacer struct {
	oldName string
	newName string
}

func (v *tableNameReplacer) Visit(node sqlparser.Node) (w sqlparser.Visitor, err error) {
	// Continue traversal
	return v, nil
}

func (v *tableNameReplacer) VisitEnd(node sqlparser.Node) error {
	switch n := node.(type) {
	case *sqlparser.TableName:
		if n.Name.Name == v.oldName {
			n.Name.Name = v.newName
		}
	case *sqlparser.QualifiedRef:
		if n.Table != nil && n.Table.Name == v.oldName {
			n.Table.Name = v.newName
		}
	}
	return nil
}

type shardingKeyExtractor struct {
	key       string
	tableName string
	args      []interface{}
	value     interface{}
	id        int64
	keyFound  bool
	err       error
}

func (v *shardingKeyExtractor) Visit(node sqlparser.Node) (w sqlparser.Visitor, err error) {
	return v, nil
}

func (v *shardingKeyExtractor) VisitEnd(node sqlparser.Node) error {
	switch n := node.(type) {
	case *sqlparser.BinaryExpr:
		if n.Op == sqlparser.EQ {
			var colName, colTable string
			// Handle left side of the expression
			switch col := n.X.(type) {
			case *sqlparser.QualifiedRef:
				colTable = col.Table.Name
				colName = col.Column.Name
			case *sqlparser.Ident:
				colName = col.Name
			default:
				return nil
			}
			if colName == v.key && (v.tableName == "" || colTable == v.tableName) {
				// Extract value from the right side
				switch val := n.Y.(type) {
				case *sqlparser.BindExpr:
					if val.Pos < len(v.args) {
						v.value = v.args[val.Pos]
						v.keyFound = true
						return io.EOF // Stop traversal
					} else {
						v.err = fmt.Errorf("argument index out of range")
						return io.EOF
					}
				case *sqlparser.NumberLit:
					v.value, _ = strconv.ParseInt(val.Value, 10, 64)
					v.keyFound = true
					return io.EOF // Stop traversal
				case *sqlparser.StringLit:
					v.value = val.Value
					v.keyFound = true
					return io.EOF // Stop traversal
				default:
					// Unsupported value type
					return nil
				}
			}
		}
	}
	return nil
}

func (s *Sharding) extractShardingKeyValue(key string, tableName string, stmt sqlparser.Statement, args ...interface{}) (value interface{}, id int64, keyFound bool, err error) {
	extractor := &shardingKeyExtractor{
		key:       key,
		tableName: tableName,
		args:      args,
	}
	// Walk the AST
	err = sqlparser.Walk(extractor, stmt)
	if err != nil && err != io.EOF {
		return nil, 0, false, err
	}
	if extractor.err != nil {
		return nil, 0, false, extractor.err
	}
	if !extractor.keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}
	return extractor.value, extractor.id, true, nil
}

func getSuffix(value interface{}, id int64, keyFound bool, r Config) (suffix string, err error) {
	if keyFound {
		suffix, err = r.ShardingAlgorithm(value)
		if err != nil {
			return
		}
	} else {
		if r.ShardingAlgorithmByPrimaryKey == nil {
			err = fmt.Errorf("no sharding key and ShardingAlgorithmByPrimaryKey is not configured")
			return
		}
		suffix = r.ShardingAlgorithmByPrimaryKey(id)
	}
	return
}

func (s *Sharding) insertValue(key string, names []*sqlparser.Ident, exprs []sqlparser.Expr, args ...any) (value any, id int64, keyFind bool, err error) {
	if len(names) != len(exprs) {
		return nil, 0, keyFind, errors.New("column names and expressions mismatch")
	}

	for i, name := range names {
		if name.Name == key {
			switch expr := exprs[i].(type) {
			case *sqlparser.BindExpr:
				value = args[expr.Pos]
			case *sqlparser.StringLit:
				value = expr.Value
			case *sqlparser.NumberLit:
				value = expr.Value
			default:
				return nil, 0, keyFind, sqlparser.ErrNotImplemented
			}
			keyFind = true
			break
		}
	}
	if !keyFind {
		return nil, 0, keyFind, ErrMissingShardingKey
	}

	return
}
