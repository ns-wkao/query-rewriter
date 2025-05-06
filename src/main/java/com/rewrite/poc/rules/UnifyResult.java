package com.rewrite.poc.rules;

import com.rewrite.poc.ir.SimpleExpression;
import com.rewrite.poc.ir.SimpleFilter; // Need Filter/Project for buildPlanFragment
import com.rewrite.poc.ir.SimpleProject;
import com.rewrite.poc.ir.SimpleRelNode;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the successful result of applying a UnifyRule.
 * Contains the rewritten query node (often an MV scan) and any
 * residual operations needed.
 */
public class UnifyResult {
    private final SimpleRelNode rewrittenNode; // The base rewritten node (e.g., MV Scan)
    private final SimpleExpression residualFilter; // Filter to apply *on top* of rewrittenNode (null if none)
    private final List<SimpleExpression> residualProjection; // Projections to apply *on top* (null if none/match)

    /**
     * Constructor for a successful unification.
     * @param rewrittenNode The base node resulting from the rewrite (typically the MV scan).
     * @param residualFilter The filter expression to apply on top, or null if none needed.
     * @param residualProjection The list of projection expressions to apply on top, or null if none needed.
     */
    public UnifyResult(SimpleRelNode rewrittenNode, SimpleExpression residualFilter, List<SimpleExpression> residualProjection) {
        this.rewrittenNode = Objects.requireNonNull(rewrittenNode, "rewrittenNode cannot be null");
        this.residualFilter = residualFilter; // Can be null
        this.residualProjection = residualProjection; // Can be null or empty
    }

    public SimpleRelNode getRewrittenNode() {
        return rewrittenNode;
    }

    public SimpleExpression getResidualFilter() {
        return residualFilter;
    }

    public List<SimpleExpression> getResidualProjection() {
        return residualProjection;
    }

    /**
         * Constructs the final plan fragment based on the result by layering
         * residual operations on top of the rewritten base node.
         * @return The SimpleRelNode representing the complete rewritten fragment.
         */
        public SimpleRelNode buildPlanFragment() {
            SimpleRelNode current = rewrittenNode;

            // 1. Apply residual filter (if any)
            if (residualFilter != null) {
                System.out.println("UnifyResult: Applying residual filter: " + residualFilter);
                current = new SimpleFilter(current, residualFilter);
            }

            // 2. Apply residual projection (if any)
            if (residualProjection != null && !residualProjection.isEmpty()) {
                 System.out.println("UnifyResult: Applying residual projection: " + residualProjection);
                 Map<String, String> inputSchema = current.getSchema(); // Schema coming into projection
                 Map<String, String> derivedSchema = new java.util.LinkedHashMap<>();

                 for (SimpleExpression projExpr : residualProjection) {
                     String colName;
                     String colType = "UNKNOWN_POC"; // Default type

                     // We only have the Expression here. We cannot reliably get the original alias
                     // without more complex tracking from the AST->IR conversion.
                     // For POC: If it's an Identifier, use that. Otherwise, use formatted string.
                     if (projExpr.isIdentifier()) {
                         colName = projExpr.getIdentifier();
                         // Try to get type from the schema of the node *before* this projection
                         colType = inputSchema.getOrDefault(colName, colType);
                     } else {
                         // Use the formatted expression string as the column name fallback
                         colName = projExpr.getExpressionString();
                         // Type inference for complex expressions is hard - stick to UNKNOWN
                         // TODO: Could add basic inference e.g. based on operators later if needed
                     }

                     // Ensure colName is not null (shouldn't happen with above logic)
                     if (colName == null) colName = "expr_" + derivedSchema.size();

                     derivedSchema.put(colName, colType);
                 }
                 System.out.println("UnifyResult: Derived output schema for residual project: " + derivedSchema.keySet());
                 current = new SimpleProject(current, residualProjection, derivedSchema);
            }
            return current;
        }
}