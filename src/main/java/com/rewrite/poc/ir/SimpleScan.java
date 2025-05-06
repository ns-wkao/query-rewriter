package com.rewrite.poc.ir;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap; // Preserve schema order

/**
 * Represents a scan of a base table. Leaf node in the plan tree.
 */
public class SimpleScan implements SimpleRelNode {
    private final String tableName;
    private final Map<String, String> schema; // Output schema = table schema

    public SimpleScan(String tableName, Map<String, String> tableSchema) {
        this.tableName = Objects.requireNonNull(tableName, "tableName is null");
        // Store an immutable copy, preserving order
        this.schema = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(tableSchema, "tableSchema is null")
        ));
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public List<SimpleRelNode> getInputs() {
        return Collections.emptyList(); // Leaf node has no inputs
    }

    @Override
    public SimpleRelNode withInputs(List<SimpleRelNode> inputs) {
        if (!inputs.isEmpty()) {
            throw new IllegalArgumentException("SimpleScan expects 0 inputs, got " + inputs.size());
        }
        return this; // Return self as inputs cannot change
    }

    @Override
    public String getNodeType() {
        return "Scan";
    }

    @Override
    public Map<String, String> getSchema() {
        return schema; // Already unmodifiable
    }

    @Override
    public String toString(String indent) {
        return indent + "Scan: " + tableName + " " + schema.keySet();
    }

    @Override
    public String toString() {
         return toString(""); // Default toString with no indent
    }

    @Override
    public boolean structurallyEquals(SimpleRelNode other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        SimpleScan that = (SimpleScan) other;
        // Compare table name and schema (key set might be enough for POC)
        return Objects.equals(tableName, that.tableName) &&
               Objects.equals(schema.keySet(), that.schema.keySet()); // Basic check
    }

    @Override
    public <R, C> R accept(SimpleNodeVisitor<R, C> visitor, C context) {
        return visitor.visitScan(this, context);
    }
}