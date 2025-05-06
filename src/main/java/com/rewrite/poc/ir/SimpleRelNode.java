package com.rewrite.poc.ir;

import java.util.List;
import java.util.Map;

/**
 * Base interface for nodes in our simplified Relational Intermediate Representation.
 */
public interface SimpleRelNode {

    /**
     * Get the child input nodes of this operator.
     * @return A list of input nodes (empty for leaf nodes like Scan).
     */
    List<SimpleRelNode> getInputs();

    /**
     * Creates a new instance of this node with replaced inputs.
     * Used for reconstructing the tree during transformation.
     * Implementations should ensure they return the correct specific type.
     * @param inputs The new list of input nodes.
     * @return A new SimpleRelNode instance of the same type with updated inputs.
     * @throws IllegalArgumentException if the number of inputs is incorrect for the node type.
     */
    SimpleRelNode withInputs(List<SimpleRelNode> inputs);

    /**
     * Get a string representation of the node type (e.g., "Scan", "Filter").
     */
    String getNodeType();

    /**
     * Get the schema produced by this node.
     * @return A map where keys are column names and values are type strings (e.g., "BIGINT", "VARCHAR").
     * Should preserve column order if possible (e.g., using LinkedHashMap).
     */
    Map<String, String> getSchema();

    /**
     * Generates a string representation of the node and its subtree, with indentation.
     * @param indent Indentation string (e.g., "  ") to apply for the current level.
     * @return A formatted string representing the plan subtree rooted at this node.
     */
    String toString(String indent);

    /**
     * Performs a basic structural comparison with another node.
     * Used for simple matching in the POC. Does not compare input nodes recursively here,
     * recursive comparison happens during visitor traversal.
     * @param other The node to compare against.
     * @return True if the nodes are of the same type and have structurally equivalent
     * properties (e.g., same table name, same filter expression string), false otherwise.
     */
    boolean structurallyEquals(SimpleRelNode other);

    /**
     * Accepts a visitor according to the visitor pattern.
     * @param visitor The visitor implementation.
     * @param context A context object to pass to the visitor method (can be null).
     * @return The result returned by the visitor's specific visit method.
     * @param <R> The return type of the visitor.
     * @param <C> The type of the context object.
     */
    <R, C> R accept(SimpleNodeVisitor<R, C> visitor, C context);

    // Default toString implementation
    @Override
    String toString(); // Ensure implementations override this, perhaps calling toString("")
}