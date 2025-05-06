package com.rewrite.poc.ir;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Represents a Join operation in the simplified Relational Intermediate Representation.
 * Initially supports only INNER joins.
 */
public class SimpleJoin implements SimpleRelNode {

    public enum JoinType {
        INNER // Add other types like LEFT, RIGHT, FULL later if needed
    }

    private final SimpleRelNode leftInput;
    private final SimpleRelNode rightInput;
    private final JoinType joinType;
    private final SimpleExpression condition;
    private final Map<String, String> schema; // Combined schema

    public SimpleJoin(SimpleRelNode leftInput, SimpleRelNode rightInput, JoinType joinType, SimpleExpression condition) {
        this.leftInput = Objects.requireNonNull(leftInput, "leftInput is null");
        this.rightInput = Objects.requireNonNull(rightInput, "rightInput is null");
        this.joinType = Objects.requireNonNull(joinType, "joinType is null");
        this.condition = Objects.requireNonNull(condition, "condition is null");

        // Combine schemas. Using LinkedHashMap to preserve order.
        // Basic combination: assumes no name collisions for now.
        // A more robust implementation might handle collisions (e.g., prefixing).
        this.schema = Stream.concat(
                        leftInput.getSchema().entrySet().stream(),
                        rightInput.getSchema().entrySet().stream()
                )
                // Simple duplicate handling: keep the first occurrence (from left input)
                // This is a simplification and might not be correct for all SQL semantics.
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> v1, // Keep existing value on merge conflict
                        LinkedHashMap::new
                ));
    }

    public SimpleRelNode getLeftInput() {
        return leftInput;
    }

    public SimpleRelNode getRightInput() {
        return rightInput;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public SimpleExpression getCondition() {
        return condition;
    }

    @Override
    public List<SimpleRelNode> getInputs() {
        return List.of(leftInput, rightInput);
    }

    @Override
    public SimpleRelNode withInputs(List<SimpleRelNode> inputs) {
        if (inputs == null || inputs.size() != 2) {
            throw new IllegalArgumentException("SimpleJoin requires exactly two inputs.");
        }
        return new SimpleJoin(inputs.get(0), inputs.get(1), this.joinType, this.condition);
    }

    @Override
    public String getNodeType() {
        return "Join";
    }

    @Override
    public Map<String, String> getSchema() {
        // Return an immutable copy? For now, return the calculated one.
        return new LinkedHashMap<>(schema); // Return a copy
    }

    @Override
    public String toString(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append(indent).append(getNodeType()).append(" [").append(joinType).append("]\n");
        sb.append(indent).append("  Condition: ").append(condition).append("\n");
        sb.append(indent).append("  Left:\n").append(leftInput.toString(indent + "    "));
        sb.append(indent).append("  Right:\n").append(rightInput.toString(indent + "    "));
        return sb.toString();
    }

    @Override
    public boolean structurallyEquals(SimpleRelNode other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        SimpleJoin that = (SimpleJoin) other;
        // Compare type and condition. Input comparison is handled by the unification visitor/rules.
        return joinType == that.joinType && Objects.equals(condition, that.condition);
    }

    @Override
    public <R, C> R accept(SimpleNodeVisitor<R, C> visitor, C context) {
        return visitor.visitSimpleJoin(this, context);
    }

    @Override
    public String toString() {
        return toString("");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleJoin that = (SimpleJoin) o;
        // Considers inputs for full equality, unlike structurallyEquals
        return joinType == that.joinType &&
               Objects.equals(leftInput, that.leftInput) &&
               Objects.equals(rightInput, that.rightInput) &&
               Objects.equals(condition, that.condition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftInput, rightInput, joinType, condition);
    }
}
