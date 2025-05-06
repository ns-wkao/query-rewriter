package com.rewrite.poc.rules;

import com.rewrite.poc.SimpleSubstitutionVisitor; // Needed for apply signature
import com.rewrite.poc.ir.SimpleJoin;
import com.rewrite.poc.ir.SimpleRelNode;
import java.util.Objects;

/**
 * Unification rule to match a SimpleJoin node in the query against
 * a SimpleJoin node in the target (MV definition).
 * Matches if join types are the same, conditions are equivalent,
 * and inputs match *exactly* (using equals()).
 * Returns a UnifyResult with the replacement node if the match is successful.
 */
public class JoinMatchRule implements UnifyRule {

    @Override
    public UnifyResult apply(SimpleRelNode query, SimpleRelNode target, SimpleRelNode replacement, SimpleSubstitutionVisitor visitor) {
        // Check if both nodes are SimpleJoin instances
        if (!(query instanceof SimpleJoin) || !(target instanceof SimpleJoin)) {
            // System.out.println("JoinMatchRule: Nodes are not both SimpleJoin. Query=" + query.getClass().getSimpleName() + ", Target=" + target.getClass().getSimpleName());
            return null; // Rule doesn't apply
        }

        SimpleJoin queryJoin = (SimpleJoin) query;
        SimpleJoin targetJoin = (SimpleJoin) target;
        // System.out.println("JoinMatchRule: Applying to Query Join and Target Join.");

        // 1. Compare Join Types
        if (queryJoin.getJoinType() != targetJoin.getJoinType()) {
             // System.out.println("JoinMatchRule: Join types do not match. Query=" + queryJoin.getJoinType() + ", Target=" + targetJoin.getJoinType());
            return null;
        }

        // 2. Compare Join Conditions
        // Using simple equals() on SimpleExpression, which compares the formatted SQL string.
        if (!Objects.equals(queryJoin.getCondition(), targetJoin.getCondition())) {
             // System.out.println("JoinMatchRule: Join conditions do not match. Query=" + queryJoin.getCondition() + ", Target=" + targetJoin.getCondition());
            return null;
        }

        // 3. Compare Inputs
        // The visitor ensures inputs are already processed/rewritten.
        // We need to check if the query's inputs *exactly match* the target's inputs.
        // Using .structurallyEquals() for a structural match, not strict object equality.
        if (!queryJoin.getLeftInput().structurallyEquals(targetJoin.getLeftInput())) {
            // System.out.println("JoinMatchRule: Left inputs do not match structurally.");
            // System.out.println("  Query Left : " + queryJoin.getLeftInput().toString("    "));
            // System.out.println("  Target Left: " + targetJoin.getLeftInput().toString("    "));
        return null;
        }
        if (!queryJoin.getRightInput().structurallyEquals(targetJoin.getRightInput())) {
            // System.out.println("JoinMatchRule: Right inputs do not match structurally.");
            // System.out.println("  Query Right : " + queryJoin.getRightInput().toString("    "));
            // System.out.println("  Target Right: " + targetJoin.getRightInput().toString("    "));
        return null;
        }

        // If all checks pass, the query join structure matches the target join structure.
        // Return a UnifyResult containing the replacement node (MV scan).
        // For this basic rule, assume no residual filter or projection is needed *from the join itself*.
        // Residuals might be introduced by rules matching nodes *above* this join.
        // System.out.println("JoinMatchRule: Match successful! Returning replacement node.");
        return new UnifyResult(replacement, null, null); // No residuals from this rule
    }

    @Override
    public String toString() {
        return "JoinMatchRule";
    }
}
