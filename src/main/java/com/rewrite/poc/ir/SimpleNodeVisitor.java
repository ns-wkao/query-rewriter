package com.rewrite.poc.ir;

/**
 * Visitor pattern interface for traversing the SimpleRelNode IR tree.
 *
 * @param <R> Return type of the visit methods.
 * @param <C> Type of the context object passed during traversal.
 */
public interface SimpleNodeVisitor<R, C> {

    // --- Visit methods for each concrete node type ---

    R visitScan(SimpleScan node, C context);

    R visitFilter(SimpleFilter node, C context);

    R visitProject(SimpleProject node, C context);

    // Potentially add visitAggregate, visitJoin etc. later

    /**
     * Default handler for visiting nodes if a specific method isn't overridden.
     * Often used to implement recursive traversal of inputs.
     * Should generally not be called directly, use node.accept(visitor, context).
     *
     * @param node The node being visited.
     * @param context The context.
     * @return Typically null or some aggregated result from children.
     */
    default R visitNode(SimpleRelNode node, C context) {
         // Default behavior: visit children, return null or last result (adjust as needed)
         R lastResult = null;
         for (SimpleRelNode input : node.getInputs()) {
             lastResult = input.accept(this, context);
         }
         return lastResult; // Or throw UnsupportedOperationException if all nodes must be handled
    }
}