package com.rewrite.poc.rules;

import com.rewrite.poc.SimpleSubstitutionVisitor;
import com.rewrite.poc.ir.*; // Import IR nodes

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Rule: Matches Query(Project -> Join -> [InL, InR]) against
 * Target(Project -> Join -> [InL, InR]).
 * Handles residual projections. Assumes Inner Joins for now.
 */
public class ProjectJoinMatchRule implements UnifyRule {

    @Override
    public UnifyResult apply(SimpleRelNode query, SimpleRelNode target, SimpleRelNode replacement, SimpleSubstitutionVisitor visitor) {

        // --- Pattern Matching: Project -> Join ---
        if (!(query instanceof SimpleProject && target instanceof SimpleProject)) return null;
        SimpleProject queryProj = (SimpleProject) query;
        SimpleProject targetProj = (SimpleProject) target;

        if (!(queryProj.getInput() instanceof SimpleJoin && targetProj.getInput() instanceof SimpleJoin)) return null;
        SimpleJoin queryJoin = (SimpleJoin) queryProj.getInput();
        SimpleJoin targetJoin = (SimpleJoin) targetProj.getInput();
         System.out.println("Rule(ProjJoin): Checking Query(Proj->Join) vs Target(Proj->Join)"); // Log rule activation

        // --- Join Matching ---
        // 1. Compare Join Types (Expand later if needed)
        if (queryJoin.getJoinType() != targetJoin.getJoinType() || queryJoin.getJoinType() != SimpleJoin.JoinType.INNER) {
             System.out.println("Rule(ProjJoin): Join types do not match or not INNER. Query=" + queryJoin.getJoinType() + ", Target=" + targetJoin.getJoinType());
            return null;
        }

        // 2. Compare Join Conditions (String equality via SimpleExpression.equals)
        if (!Objects.equals(queryJoin.getCondition(), targetJoin.getCondition())) {
             System.out.println("Rule(ProjJoin): Join conditions do not match. Query=" + queryJoin.getCondition() + ", Target=" + targetJoin.getCondition());
            return null;
        }

        // 3. Compare Join Inputs (Structurally) - Assumes JoinMatchRule fix was applied (using structurallyEquals)
        // If not, copy that logic here. We'll assume structurallyEquals is available/correct.
        if (!queryJoin.getLeftInput().structurallyEquals(targetJoin.getLeftInput())) {
             System.out.println("Rule(ProjJoin): Left inputs do not match structurally.");
            return null;
        }
        if (!queryJoin.getRightInput().structurallyEquals(targetJoin.getRightInput())) {
             System.out.println("Rule(ProjJoin): Right inputs do not match structurally.");
            return null;
        }
         System.out.println("Rule(ProjJoin): Underlying Joins match structurally.");

        // --- Projection Matching ---
        Map<String, String> targetOutputSchema = targetProj.getSchema(); // MV's final output schema

        // 4a. Check if columns referenced by query projection are available in target output
        boolean columnsAvailable = true;
        List<String> unavailableColumns = new ArrayList<>();
        // Check columns needed by the query's SELECT list
        List<SimpleExpression> queryProjections = queryProj.getProjections();
        for (SimpleExpression qProjExpr : queryProjections) {
            List<String> referenced = qProjExpr.getReferencedColumns(); // Basic implementation
            for (String refCol : referenced) {
                // Does the MV's output schema contain the column needed by the query?
                if (!targetOutputSchema.containsKey(refCol)) {
                    // Simple case-sensitive check for POC
                    columnsAvailable = false;
                    unavailableColumns.add(refCol);
                }
            }
        }

        if (!columnsAvailable) {
             System.out.println("Rule(ProjJoin): Projection mismatch - Query references columns not available in target output: " + unavailableColumns);
             System.out.println("  Target Output Schema: " + targetOutputSchema.keySet());
            return null;
        }
         System.out.println("Rule(ProjJoin): All columns referenced by query projection are available in target output.");

        // 4b. Determine if a residual projection is needed.
        List<SimpleExpression> targetProjections = targetProj.getProjections();
        List<SimpleExpression> residualProjection = null;

        // Compare the query's SELECT list to the MV's SELECT list
        // Uses SimpleExpression.equals (string compare), so syntax must be identical
        if (!queryProjections.equals(targetProjections)) {
             System.out.println("Rule(ProjJoin): Query projection differs from target projection. Residual projection required.");
             // Residual projection IS the query's original projection list
             residualProjection = queryProjections;
        } else {
             System.out.println("Rule(ProjJoin): Query projection is identical to target projection. No residual projection needed.");
        }

        // --- Match Found ---
        System.out.println("Rule(ProjJoin): >>> SUCCESSFUL MATCH FOUND <<<");
        // Return the replacement scan, null residual filter, and residual projection (if any)
        return new UnifyResult(replacement, null, residualProjection);
    }

    @Override
    public String toString() {
        return "ProjectJoinMatchRule";
    }
}