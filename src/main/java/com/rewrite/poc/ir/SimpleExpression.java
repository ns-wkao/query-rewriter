package com.rewrite.poc.ir;

import com.rewrite.poc.util.SqlStringUtils;
import io.trino.sql.tree.*; // Base AST nodes
import io.trino.sql.tree.LogicalExpression;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Wrapper for a Trino Expression AST node within our IR.
 * Stores the original node and a canonical string representation.
 * Enhanced with splitFilter logic (using LogicalExpression).
 */
public class SimpleExpression {
    private final Expression trinoExpression;
    private final String expressionString;

    public static final SimpleExpression TRUE_RESIDUAL = new SimpleExpression(BooleanLiteral.TRUE_LITERAL);


    public SimpleExpression(Expression trinoExpression) {
        this.trinoExpression = Objects.requireNonNull(trinoExpression, "trinoExpression is null");
        this.expressionString = SqlStringUtils.formatSql(trinoExpression);
    }

    public static SimpleExpression fromString(String exprString) {
        return new SimpleExpression(SqlStringUtils.parseSqlExpression(exprString));
    }

    public Expression getTrinoExpression() {
        return trinoExpression;
    }

    public String getExpressionString() {
        return expressionString;
    }

    /**
     * POC Level "splitFilter" using LogicalExpression. Checks if the query condition logically implies the target condition.
     * Handles: Exact equality, Query = Target AND Residual, Query = Residual AND Target.
     * Assumes AND is represented by LogicalExpression with Operator.AND and 2 terms.
     *
     * @param queryCondition  The condition from the user query.
     * @param targetCondition The condition from the MV definition.
     * @return - SimpleExpression.TRUE_RESIDUAL: If queryCondition is equivalent to targetCondition.
     * - A new SimpleExpression instance: Representing the residual condition if query implies target.
     * - null: If queryCondition does not imply targetCondition based on the implemented logic.
     */
    public static SimpleExpression splitFilter(SimpleExpression queryCondition, SimpleExpression targetCondition) {
        Objects.requireNonNull(queryCondition, "queryCondition cannot be null");
        Objects.requireNonNull(targetCondition, "targetCondition cannot be null");

        System.out.println("splitFilter: Comparing Query(" + queryCondition + ") vs Target(" + targetCondition + ")");

        // 1. Check for exact equality
        if (queryCondition.equals(targetCondition)) {
            //System.out.println("splitFilter: Conditions are equivalent.");
            return TRUE_RESIDUAL;
        }

        // 2. Check if query is an AND expression using LogicalExpression
        Expression queryExpr = queryCondition.getTrinoExpression();

        // Check if it's the expected type AND the operator is AND
        if (queryExpr instanceof LogicalExpression &&
            ((LogicalExpression) queryExpr).getOperator() == LogicalExpression.Operator.AND) {

            LogicalExpression andExpr = (LogicalExpression) queryExpr;
            List<Expression> terms = andExpr.getTerms();

            // For our simple split logic, we assume the AND has exactly two terms
            if (terms.size() == 2) {
                SimpleExpression term1 = new SimpleExpression(terms.get(0));
                SimpleExpression term2 = new SimpleExpression(terms.get(1));
                 //System.out.println("splitFilter: Query is an AND expression. Term1=" + term1 + ", Term2=" + term2);


                if (term1.equals(targetCondition)) {
                     // Query = Target AND Residual (term2)
                     //System.out.println("splitFilter: Query implies Target. Residual: " + term2);
                     return term2;
                } else if (term2.equals(targetCondition)) {
                     // Query = Residual (term1) AND Target
                     //System.out.println("splitFilter: Query implies Target. Residual: " + term1);
                     return term1;
                }
                 //System.out.println("splitFilter: AND terms do not match target condition exactly.");

            } else {
                 System.out.println("splitFilter: AND expression does not have exactly two terms (found " + terms.size() + "). Cannot apply simple split logic.");
            }
        } else {
             System.out.println("splitFilter: Query expression is not a top-level AND LogicalExpression.");
        }


        // 3. No implication found with current logic
        System.out.println("splitFilter: Query does NOT imply Target (or implication not recognized).");
        return null;
    }


    // --- Helper methods ---
     public boolean isIdentifier() { /* ... unchanged ... */
         return trinoExpression instanceof Identifier;
     }
     public String getIdentifier() { /* ... unchanged ... */
         return isIdentifier() ? ((Identifier)trinoExpression).getValue() : null;
     }
     public List<String> getReferencedColumns() { /* ... unchanged ... */
         // System.err.println("WARN: SimpleExpression.getReferencedColumns() not fully implemented.");
         if (isIdentifier()) return List.of(getIdentifier());
         return Collections.emptyList();
     }

    // --- Standard Overrides (handle TRUE_RESIDUAL) ---
     @Override
     public boolean equals(Object o) { /* ... unchanged ... */
         if (this == o) return true;
         if (this == TRUE_RESIDUAL || o == TRUE_RESIDUAL) return this == o;
         if (o == null || getClass() != o.getClass()) return false;
         SimpleExpression that = (SimpleExpression) o;
         return Objects.equals(expressionString, that.expressionString);
     }
     @Override
     public int hashCode() { /* ... unchanged ... */
          if (this == TRUE_RESIDUAL) return System.identityHashCode(TRUE_RESIDUAL);
         return Objects.hash(expressionString);
     }
      @Override
     public String toString() { /* ... unchanged ... */
          if (this == TRUE_RESIDUAL) return "TRUE_RESIDUAL";
         return expressionString;
     }


} // End SimpleExpression class