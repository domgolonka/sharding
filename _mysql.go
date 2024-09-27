package sharding

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

func (s *Sharding) resolveMySQL(astNode ASTNode, args ...any) (ftQuery, stQuery, tableName string, err error) {
	stmtNode, ok := astNode.(ast.StmtNode)
	if !ok {
		return "", "", "", fmt.Errorf("invalid ASTNode type for MySQLParser")
	}

	var tableName string
	var condition ast.ExprNode
	var isInsert bool
	var insertStmt *ast.InsertStmt

	switch stmt := stmtNode.(type) {
	case *ast.SelectStmt:
		tableName, err = s.getMySQLTableName(stmt.From)
		if err != nil {
			return
		}
		condition = stmt.Where
	case *ast.InsertStmt:
		isInsert = true
		insertStmt = stmt
		tableName, err = s.getMySQLTableName(stmt.Table)
		if err != nil {
			return
		}
	case *ast.UpdateStmt:
		tableName, err = s.getMySQLTableName(stmt.TableRefs)
		if err != nil {
			return
		}
		condition = stmt.Where
	case *ast.DeleteStmt:
		tableName, err = s.getMySQLTableName(stmt.TableRefs)
		if err != nil {
			return
		}
		condition = stmt.Where
	default:
		return ftQuery, stQuery, "", fmt.Errorf("unsupported statement type")
	}

	r, ok := s.configs[tableName]
	if !ok {
		// The table is not configured for sharding
		return
	}

	var suffix string
	if isInsert {
		value, id, keyFound, err := s.insertValueMySQL(r.ShardingKey, insertStmt, args...)
		if err != nil {
			return "", "", "", err
		}

		suffix, err = getSuffix(value, id, keyFound, r)
		if err != nil {
			return "", "", "", err
		}

		// Modify the table name in the AST
		replacements := map[string]string{tableName: tableName + suffix}
		stQuery, err = s.parser.ReplaceTableNames(astNode, replacements)
		if err != nil {
			return "", "", "", err
		}

		ftQuery = query // Original query remains the same

	} else {
		// Handle SELECT, UPDATE, DELETE statements
		value, id, keyFound, err := s.nonInsertValueMySQL(r.ShardingKey, condition, args...)
		if err != nil {
			return "", "", "", err
		}

		suffix, err = getSuffix(value, id, keyFound, r)
		if err != nil {
			return "", "", "", err
		}

		// Modify the table name in the AST
		replacements := map[string]string{tableName: tableName + suffix}
		stQuery, err = s.parser.ReplaceTableNames(astNode, replacements)
		if err != nil {
			return "", "", "", err
		}

		ftQuery = query // Original query remains the same
	}

	return
}

func (s *Sharding) getMySQLTableName(resultSetNode ast.ResultSetNode) (string, error) {
	switch node := resultSetNode.(type) {
	case *ast.TableSource:
		if tableName, ok := node.Source.(*ast.TableName); ok {
			return tableName.Name.O, nil
		}
	case *ast.Join:
		return s.getMySQLTableName(node.Left)
	}
	return "", fmt.Errorf("unable to extract table name")
}

func (s *Sharding) insertValueMySQL(key string, insertStmt *ast.InsertStmt, args ...any) (value any, id int64, keyFound bool, err error) {
	columnNames := insertStmt.Columns
	valueLists := insertStmt.Lists

	if len(valueLists) == 0 || len(valueLists[0]) == 0 {
		return nil, 0, false, fmt.Errorf("no values provided in insert statement")
	}

	for idx, col := range columnNames {
		if col.Name.O == key {
			valueExpr := valueLists[0][idx]
			value, err = s.extractValueFromMySQLExpr(valueExpr, args)
			if err != nil {
				return nil, 0, false, err
			}
			keyFound = true
			break
		}
	}

	if !keyFound {
		return nil, 0, false, ErrMissingShardingKey
	}

	return
}

func (s *Sharding) nonInsertValueMySQL(key string, condition ast.ExprNode, args ...any) (value any, id int64, keyFound bool, err error) {
	var visitor MySQLConditionVisitor
	visitor.key = key
	visitor.args = args
	condition.Accept(&visitor)

	if visitor.err != nil {
		return nil, 0, false, visitor.err
	}
	if !visitor.keyFound && visitor.id == 0 {
		return nil, 0, false, ErrMissingShardingKey
	}

	return visitor.value, visitor.id, visitor.keyFound, nil
}

type MySQLConditionVisitor struct {
	key      string
	args     []any
	value    any
	id       int64
	keyFound bool
	err      error
}

func (v *MySQLConditionVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.BinaryOperationExpr:
		if col, ok := node.L.(*ast.ColumnNameExpr); ok {
			if col.Name.Name.O == v.key && node.Op == opcode.EQ {
				v.value, v.err = s.extractValueFromMySQLExpr(node.R, v.args)
				if v.err != nil {
					return in, true
				}
				v.keyFound = true
				return in, true
			} else if col.Name.Name.O == "id" && node.Op == opcode.EQ {
				v.value, v.err = s.extractValueFromMySQLExpr(node.R, v.args)
				if v.err != nil {
					return in, true
				}
				if id, ok := v.value.(int64); ok {
					v.id = id
				} else {
					v.err = fmt.Errorf("ID should be int64 type")
				}
				return in, true
			}
		}
	}
	return in, false
}

func (v *MySQLConditionVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (s *Sharding) extractValueFromMySQLExpr(expr ast.ExprNode, args []any) (any, error) {
	switch e := expr.(type) {
	case *driver.ParamMarkerExpr:
		pos := e.Order - 1
		if pos < 0 || pos >= len(args) {
			return nil, fmt.Errorf("parameter index out of range")
		}
		return args[pos], nil
	case *driver.ValueExpr:
		return e.GetValue(), nil
	default:
		return nil, fmt.Errorf("unsupported expression type")
	}
}
