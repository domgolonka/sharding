package sharding

import (
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"reflect"
)

type PostgreSQLParser struct{}

func (p *PostgreSQLParser) Parse(query string) (ASTNode, error) {
	tree, err := pg_query.Parse(query)
	if err != nil {
		return nil, err
	}
	return tree, nil
}

func (p *PostgreSQLParser) ReplaceTableNames(ast ASTNode, replacements map[string]string) (string, error) {

	// Type assertion to *pg_query.ParseResult
	astNode, ok := ast.(*pg_query.ParseResult)
	if !ok {
		return "", fmt.Errorf("invalid ASTNode type for PostgreSQLParser")
	}
	// Traverse and modify the AST
	for _, stmt := range astNode.Stmts {
		replaceTableNamesInNode(stmt.Stmt, replacements)
	}

	// Convert the modified AST back to a SQL string
	query, err := pg_query.Deparse(astNode)
	if err != nil {
		return "", err
	}
	return query, nil
}

func replaceTableNamesInNode(node *pg_query.Node, replacements map[string]string) {
	if node == nil {
		return
	}

	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		rangeVar := n.RangeVar
		originalName := rangeVar.Relname
		if newName, exists := replacements[originalName]; exists {
			rangeVar.Relname = newName
		}
		// No need to traverse further in this node
	case *pg_query.Node_SelectStmt:
		replaceTableNamesInSelectStmt(n.SelectStmt, replacements)
	case *pg_query.Node_JoinExpr:
		replaceTableNamesInNode(n.JoinExpr.Larg, replacements)
		replaceTableNamesInNode(n.JoinExpr.Rarg, replacements)
		replaceTableNamesInNode(n.JoinExpr.Quals, replacements)
	case *pg_query.Node_RangeSubselect:
		replaceTableNamesInNode(n.RangeSubselect.Subquery, replacements)
	case *pg_query.Node_RangeFunction:
		// Functions in FROM clause
		for _, funcNode := range n.RangeFunction.Functions {
			funcExprNode, ok := funcNode.Node.(*pg_query.Node_FuncCall)
			if ok {
				replaceTableNamesInFuncCall(funcExprNode.FuncCall, replacements)
			}
		}
	case *pg_query.Node_CommonTableExpr:
		replaceTableNamesInNode(n.CommonTableExpr.Ctequery, replacements)
	case *pg_query.Node_UpdateStmt:
		replaceTableNamesInRangeVar(n.UpdateStmt.Relation, replacements)
		replaceTableNamesInNode(n.UpdateStmt.WhereClause, replacements)
		if n.UpdateStmt.FromClause != nil {
			for _, item := range n.UpdateStmt.FromClause {
				replaceTableNamesInNode(item, replacements)
			}
		}
	case *pg_query.Node_DeleteStmt:
		replaceTableNamesInRangeVar(n.DeleteStmt.Relation, replacements)
		replaceTableNamesInNode(n.DeleteStmt.WhereClause, replacements)
		if n.DeleteStmt.UsingClause != nil {
			for _, item := range n.DeleteStmt.UsingClause {
				replaceTableNamesInNode(item, replacements)
			}
		}
	case *pg_query.Node_InsertStmt:
		replaceTableNamesInRangeVar(n.InsertStmt.Relation, replacements)
		replaceTableNamesInNode(n.InsertStmt.SelectStmt, replacements)
	// Handle other statement types as needed
	default:
		// Recursively traverse any child nodes
		traverseChildNodes(node, replacements)
	}
}

// Helper function to traverse child nodes
func traverseChildNodes(node *pg_query.Node, replacements map[string]string) {
	val := reflect.ValueOf(node.Node)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		switch field.Kind() {
		case reflect.Ptr:
			if field.IsNil() {
				continue
			}
			childNode, ok := field.Interface().(*pg_query.Node)
			if ok {
				replaceTableNamesInNode(childNode, replacements)
			}
		case reflect.Slice:
			for j := 0; j < field.Len(); j++ {
				elem := field.Index(j)
				if elem.Kind() == reflect.Ptr {
					childNode, ok := elem.Interface().(*pg_query.Node)
					if ok {
						replaceTableNamesInNode(childNode, replacements)
					}
				}
			}
		}
	}
}

func replaceTableNamesInFuncCall(funcCall *pg_query.FuncCall, replacements map[string]string) {
	if funcCall == nil {
		return
	}

	// Function names are represented as a list of identifiers (e.g., schema, function name)
	// Usually, the function name is the last element in the list
	// We generally don't need to replace the function name itself unless you're sharding functions (unlikely)

	// However, function arguments may contain expressions that reference tables
	if funcCall.Args != nil {
		for _, arg := range funcCall.Args {
			replaceTableNamesInNode(arg, replacements)
		}
	}

	// Handle aggregate functions with 'agg_order' (ORDER BY within aggregate functions)
	if funcCall.AggOrder != nil {
		for _, sortByItem := range funcCall.AggOrder {
			if sortByItem == nil {
				continue
			}

			sortByNode, ok := sortByItem.Node.(*pg_query.Node_SortBy)
			if !ok {
				continue
			}
			sortBy := sortByNode.SortBy

			replaceTableNamesInSortBy(sortBy, replacements)
		}
	}

	// Handle aggregate filter (FILTER clause)
	if funcCall.AggFilter != nil {
		replaceTableNamesInNode(funcCall.AggFilter, replacements)
	}

	// Handle window functions with 'over' clauses
	if funcCall.Over != nil {
		replaceTableNamesInWindowDef(funcCall.Over, replacements)
	}
}

func replaceTableNamesInWindowDef(windowDef *pg_query.WindowDef, replacements map[string]string) {
	if windowDef == nil {
		return
	}

	// Partition clauses
	if windowDef.PartitionClause != nil {
		for _, expr := range windowDef.PartitionClause {
			replaceTableNamesInNode(expr, replacements)
		}
	}

	// Order clauses
	if windowDef.OrderClause != nil {
		for _, sortByItem := range windowDef.OrderClause {
			if sortByItem == nil {
				continue
			}

			// Each sortByItem is a *pg_query.Node
			// Extract the *pg_query.SortBy from it
			sortByNode, ok := sortByItem.Node.(*pg_query.Node_SortBy)
			if !ok {
				continue
			}
			sortBy := sortByNode.SortBy

			// Access the expression to sort by
			replaceTableNamesInSortBy(sortBy, replacements)
		}
	}
}

func replaceTableNamesInSortBy(sortBy *pg_query.SortBy, replacements map[string]string) {
	if sortBy == nil {
		return
	}

	// The expression to sort by is in sortBy.Node
	// Call replaceTableNamesInNode on it
	replaceTableNamesInNode(sortBy.Node, replacements)

}

// Helper function to handle SelectStmt nodes
func replaceTableNamesInSelectStmt(stmt *pg_query.SelectStmt, replacements map[string]string) {
	if stmt == nil {
		return
	}
	// FROM clause
	if stmt.FromClause != nil {
		for _, item := range stmt.FromClause {
			replaceTableNamesInNode(item, replacements)
		}
	}
	// WHERE clause
	replaceTableNamesInNode(stmt.WhereClause, replacements)
	// JOINs are handled via the FromClause nodes
	// Handle sub-selects in target list (SELECT expressions)
	if stmt.TargetList != nil {
		for _, resTarget := range stmt.TargetList {
			replaceTableNamesInNode(resTarget, replacements)
		}
	}
	// Handle GROUP BY, HAVING, ORDER BY, etc.
	if stmt.GroupClause != nil {
		for _, groupItem := range stmt.GroupClause {
			replaceTableNamesInNode(groupItem, replacements)
		}
	}
	if stmt.HavingClause != nil {
		replaceTableNamesInNode(stmt.HavingClause, replacements)
	}
	if stmt.SortClause != nil {
		for _, sortItem := range stmt.SortClause {
			replaceTableNamesInNode(sortItem, replacements)
		}
	}
}

func replaceTableNamesInRangeVar(rangeVar *pg_query.RangeVar, replacements map[string]string) {
	if rangeVar == nil {
		return
	}
	originalName := rangeVar.Relname
	if newName, exists := replacements[originalName]; exists {
		rangeVar.Relname = newName
	}
	// Note: If you need to handle schemas, you can also check and replace `rangeVar.Schemaname`
}
