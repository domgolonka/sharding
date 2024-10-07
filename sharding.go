// sharding.go

package sharding

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"

	"github.com/bwmarrin/snowflake"
	pg_query "github.com/pganalyze/pg_query_go/v4"
	"gorm.io/gorm"
)

type Table struct {
	Name  string
	Alias string
}

var (
	ErrMissingShardingKey = errors.New("sharding key or id required, and use operator =")
	ErrInvalidID          = errors.New("invalid id format")
	ErrInsertDiffSuffix   = errors.New("cannot insert different suffix tables in one query")
)

var (
	ShardingIgnoreStoreKey = "sharding_ignore"
)

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
	DoubleWrite                   bool
	ShardingKey                   string
	NumberOfShards                uint
	tableFormat                   string
	ShardingAlgorithm             func(columnValue any) (suffix string, err error)
	ShardingSuffixs               func() (suffixes []string)
	ShardingAlgorithmByPrimaryKey func(id int64) (suffix string)
	PrimaryKeyGenerator           int
	PrimaryKeyGeneratorFn         func(tableIdx int64) int64
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
			return errors.New("Snowflake NumberOfShards should be less than 1024")
		}

		if c.PrimaryKeyGenerator == PKSnowflake {
			c.PrimaryKeyGeneratorFn = s.genSnowflakeKey
		} else if c.PrimaryKeyGenerator == PKPGSequence {
			// Handle PostgreSQL sequence setup here
		} else if c.PrimaryKeyGenerator == PKCustom {
			if c.PrimaryKeyGeneratorFn == nil {
				return errors.New("PrimaryKeyGeneratorFn is required when using PKCustom")
			}
		} else {
			return errors.New("Invalid PrimaryKeyGenerator")
		}

		if c.ShardingAlgorithm == nil {
			if c.NumberOfShards == 0 {
				return errors.New("Specify NumberOfShards or ShardingAlgorithm")
			}
			c.tableFormat = getTableFormat(c.NumberOfShards)
			c.ShardingAlgorithm = defaultShardingAlgorithm(c)
		}

		if c.ShardingSuffixs == nil {
			c.ShardingSuffixs = defaultShardingSuffixes(c)
		}

		if c.ShardingAlgorithmByPrimaryKey == nil {
			if c.PrimaryKeyGenerator == PKSnowflake {
				c.ShardingAlgorithmByPrimaryKey = func(id int64) string {
					nodeID := snowflake.ParseInt64(id).Node()
					return fmt.Sprintf(c.tableFormat, nodeID%int64(c.NumberOfShards))
				}
			}
		}
		s.configs[t] = c
	}

	return nil
}

func (s *Sharding) Initialize(db *gorm.DB) error {
	db.Dialector = NewShardingDialector(db.Dialector, s)
	s.DB = db
	s.registerCallbacks(db)

	// Initialize Snowflake nodes
	s.snowflakeNodes = make([]*snowflake.Node, s._config.NumberOfShards)
	for i := int64(0); i < int64(s._config.NumberOfShards); i++ {
		n, err := snowflake.NewNode(i)
		if err != nil {
			return fmt.Errorf("init snowflake node error: %w", err)
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

// LastQuery get last SQL query
func (s *Sharding) LastQuery() string {
	if query, ok := s.querys.Load("last_query"); ok {
		return query.(string)
	}

	return ""
}

func (s *Sharding) switchConn(db *gorm.DB) {
	s.mutex.Lock()
	if db.Statement.ConnPool != nil {
		s.ConnPool = &ConnPool{ConnPool: db.Statement.ConnPool, sharding: s}
		db.Statement.ConnPool = s.ConnPool
	}
	s.mutex.Unlock()
}

// resolve parses and rewrites the query to include sharded table names.
func (s *Sharding) resolve(query string, args ...any) (string, string, string, error) {
	ftQuery := query // Original query
	stQuery := query // Sharded query

	if len(s.configs) == 0 {
		return ftQuery, stQuery, "", nil
	}

	parsedResult, err := pg_query.Parse(query)
	if err != nil {
		return ftQuery, stQuery, "", nil
	}

	stmt := parsedResult.Stmts[0].Stmt

	// Extract tables from the query
	tables := s.extractTables(stmt)

	if len(tables) == 0 {
		return ftQuery, stQuery, "", nil
	}

	for _, table := range tables {
		tableName := table.Name

		config, ok := s.configs[tableName]
		if !ok {
			continue
		}

		var suffix string

		// Determine sharding key value
		keyValue, id, keyFound, err := s.getShardingKeyValue(stmt, table, config, args)
		if err != nil {
			return ftQuery, stQuery, tableName, err
		}

		if keyFound {
			suffix, err = config.ShardingAlgorithm(keyValue)
			if err != nil {
				return ftQuery, stQuery, tableName, err
			}
		} else if id != 0 {
			if config.ShardingAlgorithmByPrimaryKey == nil {
				return ftQuery, stQuery, tableName, errors.New("ShardingAlgorithmByPrimaryKey is not configured")
			}
			suffix = config.ShardingAlgorithmByPrimaryKey(id)
		} else {
			return ftQuery, stQuery, tableName, ErrMissingShardingKey
		}

		// Replace table name with sharded table name
		shardedTableName := fmt.Sprintf("%s%s", tableName, suffix)
		s.replaceTableName(stmt, tableName, shardedTableName)
	}

	// Rebuild the query
	shardedQuery, err := pg_query.Deparse(&pg_query.ParseResult{Stmts: []*pg_query.RawStmt{&pg_query.RawStmt{Stmt: stmt}}})
	if err != nil {
		return ftQuery, stQuery, "", fmt.Errorf("failed to rebuild query: %v", err)
	}

	return ftQuery, shardedQuery, "", nil
}

// extractTables traverses the AST and collects all table names involved in the query.
func (s *Sharding) extractTables(node *pg_query.Node) []Table {
	var tables []Table
	s.extractTablesRecursive(node, &tables)
	return tables
}

func (s *Sharding) extractTablesRecursive(node *pg_query.Node, tables *[]Table) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		tableName := n.RangeVar.Relname
		alias := ""
		if n.RangeVar.Alias != nil {
			alias = n.RangeVar.Alias.Aliasname
		}
		*tables = append(*tables, Table{Name: tableName, Alias: alias})
	}

	for _, child := range s.getChildNodes(node) {
		s.extractTablesRecursive(child, tables)
	}
}

func (s *Sharding) getChildNodes(node *pg_query.Node) []*pg_query.Node {
	var children []*pg_query.Node

	// Recursively extract child nodes based on the node type
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		children = append(children, n.SelectStmt.FromClause...)
		if n.SelectStmt.WhereClause != nil {
			children = append(children, n.SelectStmt.WhereClause)
		}
		if n.SelectStmt.TargetList != nil {
			children = append(children, n.SelectStmt.TargetList...)
		}
	case *pg_query.Node_InsertStmt:
		// Wrap Relation (*RangeVar) into a *pg_query.Node
		if n.InsertStmt.Relation != nil {
			children = append(children, &pg_query.Node{
				Node: &pg_query.Node_RangeVar{
					RangeVar: n.InsertStmt.Relation,
				},
			})
		}
		if n.InsertStmt.SelectStmt != nil {
			children = append(children, n.InsertStmt.SelectStmt)
		}
		if n.InsertStmt.WithClause != nil {
			children = append(children, n.InsertStmt.WithClause.Ctes...)
		}
	case *pg_query.Node_UpdateStmt:
		children = append(children, n.UpdateStmt.FromClause...)
		if n.UpdateStmt.WhereClause != nil {
			children = append(children, n.UpdateStmt.WhereClause)
		}
	case *pg_query.Node_DeleteStmt:
		children = append(children, n.DeleteStmt.UsingClause...)
		if n.DeleteStmt.WhereClause != nil {
			children = append(children, n.DeleteStmt.WhereClause)
		}
	case *pg_query.Node_JoinExpr:
		children = append(children, n.JoinExpr.Larg)
		children = append(children, n.JoinExpr.Rarg)
		if n.JoinExpr.Quals != nil {
			children = append(children, n.JoinExpr.Quals)
		}
	case *pg_query.Node_RangeSubselect:
		if n.RangeSubselect.Subquery != nil {
			children = append(children, n.RangeSubselect.Subquery)
		}
	case *pg_query.Node_CommonTableExpr:
		if n.CommonTableExpr.Ctequery != nil {
			children = append(children, n.CommonTableExpr.Ctequery)
		}
		// Add more cases as necessary
	}

	return children
}

// getShardingKeyValue extracts the sharding key value from the query.
func (s *Sharding) getShardingKeyValue(stmt *pg_query.Node, table Table, config Config, args []any) (any, int64, bool, error) {
	var keyValue any
	var id int64
	var keyFound bool

	// Traverse the WHERE clause to find the sharding key
	conditions := s.extractConditions(stmt, table)

	for _, cond := range conditions {
		if cond.ColumnName == config.ShardingKey {
			if cond.Operator == "=" {
				keyValue = cond.Value
				keyFound = true
				break
			}
		} else if cond.ColumnName == "id" {
			if cond.Operator == "=" {
				switch v := cond.Value.(type) {
				case int64:
					id = v
				case int32:
					id = int64(v)
				case int:
					id = int64(v)
				case string:
					var err error
					id, err = strconv.ParseInt(v, 10, 64)
					if err != nil {
						return nil, 0, false, ErrInvalidID
					}
				default:
					return nil, 0, false, ErrInvalidID
				}
			}
		}
	}

	if !keyFound && id == 0 {
		return nil, 0, false, ErrMissingShardingKey
	}

	return keyValue, id, keyFound, nil
}

type Condition struct {
	ColumnName string
	Operator   string
	Value      any
}

func (s *Sharding) extractConditions(node *pg_query.Node, table Table) []Condition {
	var conditions []Condition
	s.extractConditionsRecursive(node, table, &conditions)
	return conditions
}

func (s *Sharding) extractConditionsRecursive(node *pg_query.Node, table Table, conditions *[]Condition) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_AExpr:
		if n.AExpr.Kind == pg_query.A_Expr_Kind_AEXPR_OP {
			if len(n.AExpr.Name) == 1 {
				operatorNode := n.AExpr.Name[0].Node.(*pg_query.Node_String_)
				operator := operatorNode.String_.Sval

				// Left operand
				var columnName string
				if col, ok := n.AExpr.Lexpr.Node.(*pg_query.Node_ColumnRef); ok {
					if len(col.ColumnRef.Fields) > 0 {
						fieldNode := col.ColumnRef.Fields[len(col.ColumnRef.Fields)-1]
						if fieldStr, ok := fieldNode.Node.(*pg_query.Node_String_); ok {
							columnName = fieldStr.String_.Sval
						}
					}
				}

				// Right operand
				var value any
				if constNode, ok := n.AExpr.Rexpr.Node.(*pg_query.Node_AConst); ok {
					value = s.extractConstValue(constNode)
				}

				// Add condition if the column is from the target table
				if columnName != "" {
					*conditions = append(*conditions, Condition{
						ColumnName: columnName,
						Operator:   operator,
						Value:      value,
					})
				}
			}
		}
	}

	for _, child := range s.getChildNodes(node) {
		s.extractConditionsRecursive(child, table, conditions)
	}
}

// extractConstValue extracts the value from an A_Const node
func (s *Sharding) extractConstValue(constNode *pg_query.Node_AConst) any {
	valNode := constNode.AConst
	//switch v := valNode.Val.(type) {
	//case *pg_query.A_Const_Ival:
	//	return v.Ival
	//case *pg_query.A_Const_Fval:
	//	return v.Fval
	//case *pg_query.Node_String_:
	//	return v.String_.Sval
	//case *pg_query.NullIfExpr:
	//	return nil
	//default:
	//	return nil
	//}
	return valNode.GetVal()
}

// replaceTableName replaces table names in the AST with sharded table names.
func (s *Sharding) replaceTableName(node *pg_query.Node, oldName, newName string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		if n.RangeVar.Relname == oldName {
			n.RangeVar.Relname = newName
		}
	case *pg_query.Node_ColumnRef:
		// Replace table name in column references if necessary
		fields := n.ColumnRef.Fields
		if len(fields) > 1 {
			if tblNode, ok := fields[0].Node.(*pg_query.Node_String_); ok {
				if tblNode.String_.Sval == oldName {
					tblNode.String_.Sval = newName
				}
			}
		}
	}

	for _, child := range s.getChildNodes(node) {
		s.replaceTableName(child, oldName, newName)
	}
}

// getTableFormat determines the table format string based on the number of shards.
func getTableFormat(numberOfShards uint) string {
	switch {
	case numberOfShards < 10:
		return "_%01d"
	case numberOfShards < 100:
		return "_%02d"
	case numberOfShards < 1000:
		return "_%03d"
	case numberOfShards < 10000:
		return "_%04d"
	default:
		return "_%d"
	}
}

// defaultShardingAlgorithm provides a default sharding algorithm using modulus.
func defaultShardingAlgorithm(c Config) func(any) (string, error) {
	return func(value any) (string, error) {
		var id int
		var err error
		switch v := value.(type) {
		case int:
			id = v
		case int64:
			id = int(v)
		case int32:
			id = int(v)
		case int16:
			id = int(v)
		case uint:
			id = int(v)
		case uint64:
			id = int(v)
		case uint32:
			id = int(v)
		case uint16:
			id = int(v)
		case string:
			id, err = strconv.Atoi(v)
			if err != nil {
				id = int(crc32Checksum(v))
			}
		default:
			return "", errors.New("unsupported sharding key type")
		}
		suffix := fmt.Sprintf(c.tableFormat, id%int(c.NumberOfShards))
		return suffix, nil
	}
}

// defaultShardingSuffixes generates all possible sharding suffixes.
func defaultShardingSuffixes(c Config) func() []string {
	return func() []string {
		suffixes := make([]string, c.NumberOfShards)
		for i := 0; i < int(c.NumberOfShards); i++ {
			suffixes[i] = fmt.Sprintf(c.tableFormat, i)
		}
		return suffixes
	}
}

// crc32Checksum computes the CRC32 checksum of a string.
func crc32Checksum(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
