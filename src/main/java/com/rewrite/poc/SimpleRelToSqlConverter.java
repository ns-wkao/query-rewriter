package com.rewrite.poc;

import com.rewrite.poc.ir.*; // IR interfaces/classes
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger; // For alias generation

/**
 * Converts a SimpleRelNode IR plan back into a pseudo-SQL string for demonstration.
 * This is a basic converter and may not produce perfectly valid or optimal SQL,
 * especially regarding identifier quoting and complex expressions.
 * Uses derived tables (subqueries) to handle nesting.
 */
public class SimpleRelToSqlConverter implements SimpleNodeVisitor<String, AtomicInteger> { // Context is alias counter

    /**
     * Public entry point for converting a plan tree to SQL.
     * Ensures the final output is wrapped in a SELECT statement if necessary.
     * @param node The root of the SimpleRelNode plan.
     * @return A SQL string representation.
     */
    public String convert(SimpleRelNode node) {
        // Start visiting with indentation level 0 and a fresh alias counter
        String rootSql = node.accept(this, new AtomicInteger(0));

        // If the result is just a table name (from a root Scan), wrap it in SELECT *
        if (isSimpleIdentifier(rootSql)) {
            //System.out.println("SQLConverter: Wrapping root Scan with SELECT *");
            return "SELECT *\nFROM " + rootSql;
        }
        // If it doesn't start with SELECT (e.g., error comment), return as is
         if (rootSql == null || !rootSql.trim().toUpperCase().startsWith("SELECT")) {
             //System.out.println("SQLConverter: Root node did not produce SELECT, returning raw: " + rootSql);
             return rootSql; // Return comment or potentially raw table name if not identifier
         }
        // Otherwise, assume accept produced a valid SELECT statement
        return rootSql;
    }

    // Helper to check if a string is likely just a simple identifier (table name)
    // Basic check for POC - allows alphanumeric and underscore, starting with letter or underscore.
    private boolean isSimpleIdentifier(String s) {
        return s != null && s.matches("[a-zA-Z_][a-zA-Z0-9_]*");
    }


    // Helper for indentation
    private String indent(int level) {
        // Use 2 spaces for indentation
        return "  ".repeat(level);
    }

    @Override
    public String visitScan(SimpleScan node, AtomicInteger aliasCounter) {
        // Return just the table name. The top-level 'convert' method or
        // parent nodes handle wrapping it in SELECT/subquery if needed.
        //System.out.println("SQLConverter: Visiting Scan(" + node.getTableName() + ") -> returns table name");
        return node.getTableName();
    }

    @Override
    public String visitFilter(SimpleFilter node, AtomicInteger aliasCounter) {
        //System.out.println("SQLConverter: Visiting Filter(" + node.getCondition() + ")");
        // Recursively convert the input node first
        String inputSql = node.getInput().accept(this, aliasCounter);
        String condition = node.getCondition().getExpressionString();

        // Basic check: if inputSql looks like just a table name, create simple SELECT
        if (isSimpleIdentifier(inputSql)) {
             //System.out.println("SQLConverter: Filter input is simple table: " + inputSql);
             // Input is likely just a table name, form a simple SELECT ... FROM ... WHERE
             return String.format("SELECT *\nFROM %s\nWHERE %s",
                                  inputSql, // Table name
                                  condition);
        } else if (inputSql != null && inputSql.trim().toUpperCase().startsWith("SELECT")) {
             //System.out.println("SQLConverter: Filter input is complex (subquery), wrapping.");
             // Input is complex (subquery), wrap input in a derived table
             String alias = "t" + aliasCounter.incrementAndGet();
             // Select all columns from the derived table's alias
             return String.format("SELECT %s.*\nFROM (\n%s\n) AS %s\nWHERE %s",
                                  alias,
                                  indentSQLBlock(inputSql, 1), // Indent the subquery block
                                  alias,
                                  condition);
        } else {
            // Input was something else (e.g., error comment)
            System.err.println("SQLConverter: WARN - Unexpected input format for Filter: " + inputSql);
            return String.format("-- Unexpected Filter input start --\n%s\n-- Unexpected Filter input end --\nWHERE %s", inputSql, condition);
        }
    }

    @Override
    public String visitProject(SimpleProject node, AtomicInteger aliasCounter) {
        //System.out.println("SQLConverter: Visiting Project(" + node.getProjections() + ")");
        String inputSql = node.getInput().accept(this, aliasCounter);
        String projections = node.getProjections().stream()
                // Use expression string. Real system might need alias handling from schema.
                .map(SimpleExpression::getExpressionString)
                .collect(Collectors.joining(", "));

         // Basic check: if inputSql looks like just a table name, create simple SELECT
        if (isSimpleIdentifier(inputSql)) {
             //System.out.println("SQLConverter: Project input is simple table: " + inputSql);
             return String.format("SELECT %s\nFROM %s",
                                  projections,
                                  inputSql); // Table name
        } else if (inputSql != null && inputSql.trim().toUpperCase().startsWith("SELECT")) {
              //System.out.println("SQLConverter: Project input is complex (subquery), wrapping.");
              // Input is complex (subquery), wrap input in a derived table
              String alias = "t" + aliasCounter.incrementAndGet();
              return String.format("SELECT %s\nFROM (\n%s\n) AS %s",
                                   projections,
                                   indentSQLBlock(inputSql, 1), // Indent subquery
                                   alias);
         } else {
            // Input was something else (e.g., error comment)
            System.err.println("SQLConverter: WARN - Unexpected input format for Project: " + inputSql);
            return String.format("SELECT %s\nFROM\n-- Unexpected Project input start --\n%s\n-- Unexpected Project input end --", projections, inputSql);
        }
    }

     // Fallback for other node types if added later
     @Override
    public String visitNode(SimpleRelNode node, AtomicInteger aliasCounter) {
         System.err.println("SQLConverter: WARN - Unsupported node type for SQL conversion: " + node.getClass().getSimpleName());
          // Try converting inputs and wrapping? Or just return comment?
          String inputsStr = node.getInputs().stream()
                               .map(n -> n.accept(this, aliasCounter))
                               .collect(Collectors.joining("\n---\n"));
         return String.format("/* SQLConverter: Unsupported Node '%s' */\n-- Input(s):\n%s",
                             node.getNodeType(), inputsStr);
    }

    @Override
    public String visitSimpleJoin(SimpleJoin node, AtomicInteger aliasCounter) {
        //System.out.println("SQLConverter: Visiting Join(" + node.getJoinType() + ", " + node.getCondition() + ")");

        String leftSql = node.getLeftInput().accept(this, aliasCounter);
        String rightSql = node.getRightInput().accept(this, aliasCounter);
        String condition = node.getCondition().getExpressionString();
        SimpleJoin.JoinType joinType = node.getJoinType();

        String joinKeyword = joinType.toString(); // Assuming toString() returns "INNER", "LEFT", etc.

        // Handle different input types (table names vs. subqueries)
        boolean leftIsSimple = isSimpleIdentifier(leftSql);
        boolean rightIsSimple = isSimpleIdentifier(rightSql);

        String leftTable = leftSql;
        String rightTable = rightSql;

        if (!leftIsSimple) {
            String alias = "lt" + aliasCounter.incrementAndGet();
            leftTable = String.format("(\n%s\n) AS %s", indentSQLBlock(leftSql, 1), alias);
        }

        if (!rightIsSimple) {
            String alias = "rt" + aliasCounter.incrementAndGet();
            rightTable = String.format("(\n%s\n) AS %s", indentSQLBlock(rightSql, 1), alias);
        }

        return String.format("SELECT *\nFROM %s\n%s JOIN %s ON %s",
                             leftTable,
                             joinKeyword,
                             rightTable,
                             condition);
    }

    /**
     * Helper to indent each line of a potentially multi-line SQL block.
     */
    private String indentSQLBlock(String sql, int indentLevel) {
        if (sql == null) return "";
        String indentation = indent(indentLevel);
        return sql.lines()
                  .map(line -> indentation + line)
                  .collect(Collectors.joining("\n"));
    }
}
