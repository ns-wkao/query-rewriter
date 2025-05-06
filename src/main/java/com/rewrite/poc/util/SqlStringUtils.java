package com.rewrite.poc.util;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Statement;
import io.trino.sql.SqlFormatter;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility methods for parsing and formatting SQL strings using the Trino parser.
 */
public final class SqlStringUtils {

    private SqlStringUtils() {}

    private static final SqlParser SQL_PARSER = new SqlParser();

    /**
     * Parses a full SQL statement string into a Trino Statement AST node.
     * Uses default parsing options.
     * @param sql The SQL statement string.
     * @return The parsed Statement object.
     * @throws RuntimeException wrapping the ParseException if parsing fails.
     */
    public static Statement parseSqlStatement(String sql) {
        Objects.requireNonNull(sql, "SQL statement string cannot be null");
        try {
            // --- Use method without ParsingOptions ---
            return SQL_PARSER.createStatement(sql);
        } catch (io.trino.sql.parser.ParsingException e) {
            System.err.println("Failed to parse statement:\n" + sql);
            throw new RuntimeException("SQL Statement Parsing Error: " + e.getMessage(), e);
        } catch (Exception e) {
             System.err.println("Unexpected error parsing statement:\n" + sql);
             throw new RuntimeException("Unexpected SQL Statement Parsing Error", e);
        }
    }

    /**
     * Parses a SQL expression string into a Trino Expression AST node.
     * Uses default parsing options.
     * Note: This expects *only* an expression, not a full statement.
     * @param expressionSql The SQL expression string (e.g., "a > 10", "b + c").
     * @return The parsed Expression object.
     * @throws RuntimeException wrapping the ParseException if parsing fails.
     */
    public static Expression parseSqlExpression(String expressionSql) {
         Objects.requireNonNull(expressionSql, "SQL expression string cannot be null");
         try {
            // --- Use method without ParsingOptions ---
            return SQL_PARSER.createExpression(expressionSql);
        } catch (io.trino.sql.parser.ParsingException e) {
            System.err.println("Failed to parse expression: " + expressionSql);
             throw new RuntimeException("SQL Expression Parsing Error: " + e.getMessage() + " [Expression: " + expressionSql + "]", e);
         } catch (Exception e) {
              System.err.println("Unexpected error parsing expression: " + expressionSql);
             throw new RuntimeException("Unexpected SQL Expression Parsing Error", e);
         }
    }

    /**
     * Formats a Trino AST Node back into a canonical SQL string representation.
     * @param node The AST Node (Statement, Expression, etc.).
     * @return Formatted SQL String. Returns node.toString() as fallback on error.
     */
    public static String formatSql(Node node) {
        if (node == null) {
            return "NULL";
        }
        try {
            // --- Use the simpler formatSql(Node) method ---
            return SqlFormatter.formatSql(node);
        } catch (Exception e) {
            System.err.println("Warning: Failed to format node using SqlFormatter (" + e.getMessage() + "). Returning node.toString(): " + node);
            return node.toString();
        }
    }
}