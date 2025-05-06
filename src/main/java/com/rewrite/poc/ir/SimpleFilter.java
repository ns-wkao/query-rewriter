package com.rewrite.poc.ir;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a filter operation (WHERE clause).
 */
public class SimpleFilter implements SimpleRelNode {
    private final SimpleRelNode input;
    private final SimpleExpression condition;

    public SimpleFilter(SimpleRelNode input, SimpleExpression condition) {
        this.input = Objects.requireNonNull(input, "input is null");
        this.condition = Objects.requireNonNull(condition, "condition is null");
    }

    public SimpleRelNode getInput() {
        return input;
    }

    public SimpleExpression getCondition() {
        return condition;
    }

    @Override
    public List<SimpleRelNode> getInputs() {
        return Collections.singletonList(input);
    }

    @Override
    public SimpleRelNode withInputs(List<SimpleRelNode> inputs) {
        if (inputs.size() != 1) {
            throw new IllegalArgumentException("SimpleFilter expects 1 input, got " + inputs.size());
        }
        // Return new instance only if input actually changed
        if (inputs.get(0) == this.input) {
            return this;
        }
        return new SimpleFilter(inputs.get(0), condition);
    }

    @Override
    public String getNodeType() {
        return "Filter";
    }

    @Override
    public Map<String, String> getSchema() {
        // Filter does not change the schema
        return input.getSchema();
    }

    @Override
    public String toString(String indent) {
        return indent + "Filter: " + condition.getExpressionString() + "\n" +
               input.toString(indent + "  "); // Indent child
    }

     @Override
    public String toString() {
         return toString("");
    }

    @Override
    public boolean structurallyEquals(SimpleRelNode other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        SimpleFilter that = (SimpleFilter) other;
        // Compare condition (using SimpleExpression's equals, which uses string for now)
        return Objects.equals(condition, that.condition);
    }

    @Override
    public <R, C> R accept(SimpleNodeVisitor<R, C> visitor, C context) {
        return visitor.visitFilter(this, context);
    }
}