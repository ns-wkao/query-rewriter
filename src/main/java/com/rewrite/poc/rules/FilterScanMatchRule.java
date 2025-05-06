package com.rewrite.poc.rules;

import com.rewrite.poc.SimpleSubstitutionVisitor;
import com.rewrite.poc.ir.SimpleExpression;
import com.rewrite.poc.ir.SimpleFilter;
import com.rewrite.poc.ir.SimpleRelNode;
import com.rewrite.poc.ir.SimpleScan;

/**
 * Enhanced Rule: Matches Query(Filter -> Scan) against Target(Filter -> Scan).
 * Uses splitFilter to handle exact matches and cases where Query filter implies Target filter.
 */
public class FilterScanMatchRule implements UnifyRule {

    @Override
    public UnifyResult apply(SimpleRelNode query, SimpleRelNode target, SimpleRelNode replacement, SimpleSubstitutionVisitor visitor) {

        // --- Pattern Matching ---
        if (!(query instanceof SimpleFilter && target instanceof SimpleFilter)) return null;
        SimpleFilter queryFilter = (SimpleFilter) query;
        SimpleFilter targetFilter = (SimpleFilter) target;
        if (!(queryFilter.getInput() instanceof SimpleScan && targetFilter.getInput() instanceof SimpleScan)) return null;
        SimpleScan queryScan = (SimpleScan) queryFilter.getInput();
        SimpleScan targetScan = (SimpleScan) targetFilter.getInput();
        //System.out.println("Rule(FilterScan): Checking Query(Filter->Scan) vs Target(Filter->Scan)");

        // --- Condition Matching ---

        // 1. Check base tables
        if (!queryScan.structurallyEquals(targetScan)) {
             //System.out.println("Rule(FilterScan): Base scans do not match structurally.");
            return null;
        }
        // System.out.println("Rule(FilterScan): Base scans match.");

        // 2. Check filter implication using splitFilter
        SimpleExpression queryCondition = queryFilter.getCondition();
        SimpleExpression targetCondition = targetFilter.getCondition();
        SimpleExpression residualFilter = SimpleExpression.splitFilter(queryCondition, targetCondition);

        // Check the result from splitFilter:
        // - null: No implication -> Rule fails
        // - TRUE_RESIDUAL: Equivalent -> Match success, no residual filter needed.
        // - Other SimpleExpression: Implied -> Match success, use this as residual filter.
        if (residualFilter == null) {
             //System.out.println("Rule(FilterScan): splitFilter returned null (No implication). Rule fails.");
             return null; // No implication found
        }

        // Determine the actual residual to pass to UnifyResult (null if equivalent)
        SimpleExpression actualResidualForUnify = (residualFilter == SimpleExpression.TRUE_RESIDUAL) ? null : residualFilter;

        if(actualResidualForUnify == null) {
             //System.out.println("Rule(FilterScan): Filter conditions are equivalent.");
        } else {
             //System.out.println("Rule(FilterScan): Query filter implies target. Residual = " + actualResidualForUnify);
        }

        // --- Match Found ---
        System.out.println("Rule(FilterScan): SUCCESSFUL match found.");
        // Pass the replacement node and the calculated residual filter (which might be null)
        // This rule doesn't deal with projections, so residual projection is null.
        return new UnifyResult(replacement, actualResidualForUnify, null);
    }
}