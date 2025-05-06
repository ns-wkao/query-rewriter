package com.rewrite.poc;

import com.rewrite.poc.ir.*; // Import our IR classes
import com.rewrite.poc.util.SqlStringUtils;
import io.trino.sql.tree.*; // Import Trino AST nodes

import java.util.ArrayList;
import java.util.LinkedHashMap; // Preserve column order
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Converts a Trino Statement AST into a SimpleRelNode IR plan.
 * Limited implementation for POC: Handles basic SELECT [cols] FROM [table] WHERE [cond].
 * Does NOT handle joins, aggregates, group by, order by, etc.
 */
public class TrinoAstToSimpleRelConverter extends AstVisitor<SimpleRelNode, Void> { // Result is SimpleRelNode, no context needed (Void)

    private final Config config;

    /**
     * Constructor requiring configuration for schema lookups.
     * @param config The loaded application configuration.
     */
    public TrinoAstToSimpleRelConverter(Config config) {
        this.config = Objects.requireNonNull(config, "config cannot be null");
    }

    /**
     * Public entry point to start the conversion.
     * @param statement The parsed Trino Statement.
     * @return The root SimpleRelNode of the converted plan.
     * @throws UnsupportedOperationException if the statement structure is not supported.
     */
    public SimpleRelNode convert(Statement statement) {
        // Start the visiting process on the statement node.
        // The second argument to process is the initial context (null here).
        return process(statement, null);
    }

    // --- Core Visitor Methods ---

    @Override
    protected SimpleRelNode visitQuery(Query node, Void context) {
        // We only handle QuerySpecification in this POC
        if (node.getQueryBody() instanceof QuerySpecification) {
            // Delegate to the specific visitor for QuerySpecification
            return process(node.getQueryBody(), context);
        } else {
            throw new UnsupportedOperationException("Unsupported query body type: " + node.getQueryBody().getClass().getSimpleName() + ". Only QuerySpecification is handled.");
        }
    }

    @Override
    protected SimpleRelNode visitQuerySpecification(QuerySpecification node, Void context) {
        //System.out.println("AST->IR: Visiting QuerySpecification");

        // 1. Process FROM clause - Expect single table for POC
        SimpleRelNode currentPlan = processFromClause(node);

        // 2. Process WHERE clause (if present)
        currentPlan = processWhereClause(node, currentPlan);

        // 3. Process SELECT clause (projections)
        currentPlan = processSelectClause(node, currentPlan);

        //System.out.println("AST->IR: Finished QuerySpecification");
        return currentPlan;
    }

    // --- Helper methods for processing clauses ---

    private SimpleRelNode processFromClause(QuerySpecification node) {
        if (node.getFrom().isEmpty() || !(node.getFrom().get() instanceof Table)) {
             throw new UnsupportedOperationException("POC only supports single table FROM clause (No Joins, Subqueries, etc. in FROM)");
        }
        Table table = (Table) node.getFrom().get();
        // Handle potential qualified names (schema.table) -> get suffix for table name
        String tableName = table.getName().getSuffix();
        //System.out.println("AST->IR: Found FROM table: " + tableName);

        // Look up schema from config
        Map<String, String> baseSchema = config.getTableSchema(tableName); // Throws if table not found
         if (baseSchema == null || baseSchema.isEmpty()) {
             // Config handles not found, but check for empty schema just in case
             throw new RuntimeException("Schema found but is empty for table in config: " + tableName);
         }
         //System.out.println("AST->IR: Creating SimpleScan with schema: " + baseSchema.keySet());
         // Important: Use a mutable copy for the schema if needed downstream, though SimpleScan makes it unmodifiable
         return new SimpleScan(tableName, new LinkedHashMap<>(baseSchema));
    }

    private SimpleRelNode processWhereClause(QuerySpecification node, SimpleRelNode inputPlan) {
         if (node.getWhere().isPresent()) {
            Expression conditionExpr = node.getWhere().get();
            //System.out.println("AST->IR: Found WHERE clause: " + SqlStringUtils.formatSql(conditionExpr));
            SimpleExpression condition = new SimpleExpression(conditionExpr);
            return new SimpleFilter(inputPlan, condition);
        } else {
             //System.out.println("AST->IR: No WHERE clause found.");
            return inputPlan; // No filter to add
        }
    }

    private SimpleRelNode processSelectClause(QuerySpecification node, SimpleRelNode inputPlan) {
        Select select = node.getSelect();
        if (select.isDistinct()) {
             throw new UnsupportedOperationException("DISTINCT not supported in POC");
        }

         //System.out.println("AST->IR: Processing SELECT clause");
         List<SimpleExpression> projections = new ArrayList<>();
         Map<String, String> outputSchema = new LinkedHashMap<>(); // Preserves select order
         Map<String, String> inputSchema = inputPlan.getSchema();

        for (SelectItem item : select.getSelectItems()) {
            if (item instanceof AllColumns) {
                // Handle SELECT *
                 if (((AllColumns) item).getTarget().isPresent()) {
                      throw new UnsupportedOperationException("SELECT target.* not supported in POC");
                 }
                  //System.out.println("AST->IR:   Processing SELECT *");
                  // Add all columns from input schema
                 inputSchema.forEach((name, type) -> {
                      // Create Identifier expression for each column name
                      projections.add(new SimpleExpression(new Identifier(name)));
                      outputSchema.put(name, type);
                 });

            } else if (item instanceof SingleColumn) {
                SingleColumn singleColumn = (SingleColumn) item;
                Expression rawExpression = singleColumn.getExpression();
                SimpleExpression projExpr = new SimpleExpression(rawExpression); // Wrap the raw Trino expression
                String exprStr = projExpr.getExpressionString(); // Formatted string
                 //System.out.println("AST->IR:   Processing SELECT item: " + exprStr);

                projections.add(projExpr);

                String alias = singleColumn.getAlias().map(Identifier::getValue).orElse(null);
                String colName;
                String colType = "UNKNOWN_POC"; // Default type if inference fails

                if (alias != null) {
                    colName = alias;
                     //System.out.println("AST->IR:     Alias found: " + colName);
                    // Basic type inference: if original expr is identifier, use its input type
                    if (rawExpression instanceof Identifier) {
                        colType = inputSchema.getOrDefault(((Identifier) rawExpression).getValue(), colType);
                    } // TODO: Add more complex type inference based on expression structure if needed
                } else if (rawExpression instanceof Identifier) {
                    // No alias, expression is a simple column identifier
                    colName = ((Identifier) rawExpression).getValue();
                    colType = inputSchema.getOrDefault(colName, colType);
                } else {
                    // No alias, complex expression - use formatted expression string as name
                    colName = exprStr;
                     //System.out.println("AST->IR:     Complex expression without alias, using expression string as name.");
                     // TODO: Infer type based on expression (e.g., arithmetic -> numeric, comparison -> boolean)
                }
                outputSchema.put(colName, colType);

            } else {
                 throw new UnsupportedOperationException("Unsupported SELECT item type: " + item.getClass().getSimpleName());
            }
        }

        // Determine if a projection node is actually needed.
        // It's needed if the projections aren't simply selecting all columns
        // from the input *in the same order*.
        boolean needsProjection = true; // Assume needed unless proven otherwise
        if (projections.size() == inputSchema.size()) {
             boolean allMatchInOrder = true;
             int i = 0;
             for (String inputColName : inputSchema.keySet()) {
                 SimpleExpression p = projections.get(i++);
                 // Check if projection is an identifier and matches the input column name
                 if (!(p.isIdentifier() && p.getIdentifier().equalsIgnoreCase(inputColName))) { // Use equalsIgnoreCase for flexibility?
                     allMatchInOrder = false;
                     break;
                 }
             }
             if (allMatchInOrder) {
                 needsProjection = false; // SELECT * equivalent, no projection node needed
                  //System.out.println("AST->IR: SELECT clause matches input schema exactly, skipping Project node.");
             }
        }

        if (needsProjection) {
             //System.out.println("AST->IR: Adding Project node with output schema: " + outputSchema.keySet());
             return new SimpleProject(inputPlan, projections, outputSchema);
        } else {
            return inputPlan; // Return input plan directly
        }
    }


    // --- Fallback Visitor Methods ---

    @Override
    protected SimpleRelNode visitNode(Node node, Void context) {
        // This method is called if no more specific visit method matches.
        // For statements we don't support, throw an error.
         if (node instanceof Statement) {
             // Allow visitQuery to handle Query nodes, otherwise unsupported.
             if (!(node instanceof Query)) {
                 throw new UnsupportedOperationException("Unsupported SQL statement type: " + node.getClass().getSimpleName());
             }
         }
         // For other node types encountered during traversal (like expressions within clauses),
         // the specific clause processing methods should handle them.
         // If we reach here for an unexpected node type, it's likely an error or unsupported structure.
        throw new UnsupportedOperationException("Unhandled AST node type during conversion: " + node.getClass().getSimpleName());
    }

    // Override other visit methods (visitTable, visitJoin, visitGroupBy, etc.)
    // to throw UnsupportedOperationException for this POC.

    @Override
    protected SimpleRelNode visitTable(Table node, Void context) {
         throw new UnsupportedOperationException("visitTable should not be called directly in this converter logic.");
    }

     @Override
    protected SimpleRelNode visitJoin(Join node, Void context) {
         throw new UnsupportedOperationException("JOIN operations are not supported in this POC.");
    }

     @Override
    protected SimpleRelNode visitGroupBy(GroupBy node, Void context) {
         throw new UnsupportedOperationException("GROUP BY operations are not supported in this POC.");
    }

     @Override
    protected SimpleRelNode visitOrderBy(OrderBy node, Void context) {
          throw new UnsupportedOperationException("ORDER BY operations are not supported in this POC.");
    }

     @Override
     protected SimpleRelNode visitSelect(Select node, Void context) {
          throw new UnsupportedOperationException("visitSelect should not be called directly in this converter logic.");
     }

     // Allow visiting expressions, handled within clause processors
     @Override
     protected SimpleRelNode visitExpression(Expression node, Void context) {
          // This shouldn't be directly called to return a SimpleRelNode.
          // Expressions are processed into SimpleExpression objects within clause handlers.
          throw new IllegalStateException("visitExpression should not be called to produce a SimpleRelNode.");
     }
}