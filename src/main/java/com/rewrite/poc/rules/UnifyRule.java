package com.rewrite.poc.rules;

// Now import the actual visitor class we are about to create
import com.rewrite.poc.SimpleSubstitutionVisitor;
import com.rewrite.poc.ir.SimpleRelNode;

@FunctionalInterface
public interface UnifyRule {

    /**
     * Attempts to match the query node structure against the target node structure.
     * If a match is found that corresponds to the *entire* target definition,
     * it returns a UnifyResult containing the replacement node (MV scan) and any
     * necessary residual operations (filters, projections).
     *
     * @param query The current node/subtree in the query being processed by the visitor.
     * @param target The root node of the materialized view definition's IR plan (the pattern to match).
     * @param replacement The node representing the scan of the materialized view table (to be used on success).
     * @param visitor The calling visitor instance. Use for context or potential recursive calls.
     * @return A UnifyResult if a full match covering the entire target is found, otherwise null.
     */
    // --- Change parameter type from Object to SimpleSubstitutionVisitor ---
    UnifyResult apply(SimpleRelNode query, SimpleRelNode target, SimpleRelNode replacement, SimpleSubstitutionVisitor visitor);
}