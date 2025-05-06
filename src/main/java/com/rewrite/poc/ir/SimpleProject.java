package com.rewrite.poc.ir;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.LinkedHashMap;
import java.util.ArrayList;

/**
 * Represents a projection operation (SELECT clause).
 */
public class SimpleProject implements SimpleRelNode {
    private final SimpleRelNode input;
    private final List<SimpleExpression> projections;
    private final Map<String, String> schema; // Output schema defined by projections

    public SimpleProject(SimpleRelNode input, List<SimpleExpression> projections, Map<String, String> outputSchema) {
        this.input = Objects.requireNonNull(input, "input is null");
        this.projections = Collections.unmodifiableList(new ArrayList<>(
                Objects.requireNonNull(projections, "projections is null")
        ));
         // Store an immutable copy, preserving order
        this.schema = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(outputSchema, "outputSchema is null")
        ));
         if (projections.isEmpty()) {
             throw new IllegalArgumentException("Projections cannot be empty for SimpleProject");
         }
    }

    public SimpleRelNode getInput() {
        return input;
    }

    public List<SimpleExpression> getProjections() {
        return projections; // Already unmodifiable
    }

    @Override
    public List<SimpleRelNode> getInputs() {
        return Collections.singletonList(input);
    }

    @Override
    public SimpleRelNode withInputs(List<SimpleRelNode> inputs) {
         if (inputs.size() != 1) {
            throw new IllegalArgumentException("SimpleProject expects 1 input, got " + inputs.size());
        }
        // Return new instance only if input actually changed
        if (inputs.get(0) == this.input) {
            return this;
        }
         // Schema derivation is complex if input schema changes.
         // For POC, we assume the output schema definition remains valid.
         // A real system would re-derive the schema here.
        return new SimpleProject(inputs.get(0), projections, schema);
    }

    @Override
    public String getNodeType() {
        return "Project";
    }

    @Override
    public Map<String, String> getSchema() {
        return schema; // Already unmodifiable
    }

    @Override
    public String toString(String indent) {
        String projStr = projections.stream()
                .map(SimpleExpression::getExpressionString)
                .collect(Collectors.joining(", "));
        return indent + "Project: [" + projStr + "] Schema: " + schema.keySet() + "\n" +
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
        SimpleProject that = (SimpleProject) other;
        // Compare projection expressions (using SimpleExpression's equals) and output schema
        return Objects.equals(projections, that.projections) &&
               Objects.equals(schema.keySet(), that.schema.keySet()); // Basic check
    }

    @Override
    public <R, C> R accept(SimpleNodeVisitor<R, C> visitor, C context) {
        return visitor.visitProject(this, context);
    }
}