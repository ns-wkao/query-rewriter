package com.rewrite.poc.rules;

import com.rewrite.poc.SimpleSubstitutionVisitor;
import com.rewrite.poc.ir.*;

import java.util.ArrayList; // Added import
import java.util.List;
import java.util.Map; // Added import
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Enhanced Rule: Matches Query(Project -> Filter -> Scan) against Target(Project -> Filter -> Scan).
 * Uses splitFilter for filters and handles residual projections.
 */
public class ProjectFilterScanMatchRule implements UnifyRule {

    @Override
    public UnifyResult apply(SimpleRelNode query, SimpleRelNode target, SimpleRelNode replacement, SimpleSubstitutionVisitor visitor) {

        // --- Pattern Matching ---
        if (!(query instanceof SimpleProject && target instanceof SimpleProject)) return null;
        SimpleProject queryProj = (SimpleProject) query;
        SimpleProject targetProj = (SimpleProject) target;
        if (!(queryProj.getInput() instanceof SimpleFilter && targetProj.getInput() instanceof SimpleFilter)) return null;
        SimpleFilter queryFilter = (SimpleFilter) queryProj.getInput();
        SimpleFilter targetFilter = (SimpleFilter) targetProj.getInput();
        if (!(queryFilter.getInput() instanceof SimpleScan && targetFilter.getInput() instanceof SimpleScan)) return null;
        SimpleScan queryScan = (SimpleScan) queryFilter.getInput();
        SimpleScan targetScan = (SimpleScan) targetFilter.getInput();
         //System.out.println("Rule(ProjFilterScan): Checking Query(Proj->Filter->Scan) vs Target(Proj->Filter->Scan)");


        // --- Condition Matching ---

        // 1. Check base tables
        if (!queryScan.structurallyEquals(targetScan)) {
             //System.out.println("Rule(ProjFilterScan): Base scans do not match structurally.");
            return null;
        }
        // System.out.println("Rule(ProjFilterScan): Base scans match.");

        // 2. Check filter implication using splitFilter
        SimpleExpression queryCondition = queryFilter.getCondition();
        SimpleExpression targetCondition = targetFilter.getCondition();
        SimpleExpression residualFilter = SimpleExpression.splitFilter(queryCondition, targetCondition);

        if (residualFilter == null) {
             //System.out.println("Rule(ProjFilterScan): splitFilter returned null (No implication). Rule fails.");
            return null; // No implication
        }
        SimpleExpression actualResidualFilter = (residualFilter == SimpleExpression.TRUE_RESIDUAL) ? null : residualFilter;
         if(actualResidualFilter == null) System.out.println("Rule(ProjFilterScan): Filter conditions equivalent.");
         else System.out.println("Rule(ProjFilterScan): Query filter implies target. Residual = " + actualResidualFilter);


        // 3. Check projection compatibility
        Map<String, String> targetOutputSchema = targetProj.getSchema(); // Schema provided by the MV definition plan

        // 3a. Check if all columns *referenced* in the query projection are available from the target's output
         boolean columnsAvailable = true;
         List<String> unavailableColumns = new ArrayList<>();
         for (SimpleExpression qProjExpr : queryProj.getProjections()) {
             List<String> referenced = qProjExpr.getReferencedColumns(); // Basic implementation for now
             for (String refCol : referenced) {
                 if (!targetOutputSchema.containsKey(refCol)) {
                     // Handle case-insensitivity? Assume exact match for POC.
                     columnsAvailable = false;
                     unavailableColumns.add(refCol);
                 }
             }
         }

         if (!columnsAvailable) {
              //System.out.println("Rule(ProjFilterScan): Projection mismatch - Query references columns not available in target output: " + unavailableColumns);
              //System.out.println("  Target Output Schema: " + targetOutputSchema.keySet());
              return null;
         }
         //System.out.println("Rule(ProjFilterScan): All columns referenced by query projection are available in target output.");


        // 3b. Determine if a residual projection is needed.
        // It's needed if the query's projection list is different from the target's list.
        List<SimpleExpression> queryProjections = queryProj.getProjections();
        List<SimpleExpression> targetProjections = targetProj.getProjections();
        List<SimpleExpression> residualProjection = null; // Default to no residual needed

         // Use List.equals, which relies on SimpleExpression.equals (comparing canonical strings)
        if (!queryProjections.equals(targetProjections)) {
             //System.out.println("Rule(ProjFilterScan): Query projection differs from target projection. Residual projection required.");
             // The residual projection is simply the projection requested by the original query
             residualProjection = queryProjections;
        } else {
              //System.out.println("Rule(ProjFilterScan): Query projection is identical to target projection. No residual projection needed.");
        }

        // --- Match Found ---
        System.out.println("Rule(ProjFilterScan): SUCCESSFUL match found.");
        // Return the replacement scan, the residual filter (if any), and the residual projection (if any)
        return new UnifyResult(replacement, actualResidualFilter, residualProjection);
    }
}