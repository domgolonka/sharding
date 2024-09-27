package sharding

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"strings"
)

type MySQLParser struct{}

func (p *MySQLParser) Parse(query string) (ASTNode, error) {
	prs := parser.New()
	stmtNodes, _, err := prs.Parse(query, "", "")
	if err != nil {
		return nil, err
	}
	return stmtNodes[0], nil
}

func (p *MySQLParser) ReplaceTableNames(astNode ASTNode, replacements map[string]string) (string, error) {
	stmtNode, ok := astNode.(ast.StmtNode)
	if !ok {
		return "", fmt.Errorf("invalid ASTNode type for MySQLParser")
	}
	replacer := &MySQLTableNameReplacer{replacements: replacements}
	stmtNode.Accept(replacer)

	// Restore the AST back to a SQL string
	var buf strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buf)
	err := stmtNode.Restore(restoreCtx)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

type MySQLTableNameReplacer struct {
	replacements map[string]string
}

func (v *MySQLTableNameReplacer) Enter(in ast.Node) (ast.Node, bool) {
	if node, ok := in.(*ast.TableName); ok {
		if newName, exists := v.replacements[node.Name.L]; exists {
			node.Name.L = newName
			node.Name.O = newName
		}
	}
	return in, false
}

func (v *MySQLTableNameReplacer) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
