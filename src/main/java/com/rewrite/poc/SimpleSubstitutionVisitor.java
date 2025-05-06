package com.rewrite.poc;

import com.rewrite.poc.ir.*; // IR nodes
import com.rewrite.poc.rules.*; // Rules package
import com.rewrite.poc.util.SqlStringUtils; // For parsing MV defs
import io.trino.sql.tree.Statement; // For parsing MV defs

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap;

/**
 * Visits the query plan IR (SimpleRelNode tree) and attempts substitutions
 * using Materialized Views defined in the configuration.
 * Implements the SimpleNodeVisitor interface to traverse the tree.
 */
public class SimpleSubstitutionVisitor implements SimpleNodeVisitor<SimpleRelNode, Void> { // Returns Node, no Context

    private final Config config;
    private final TrinoAstToSimpleRelConverter converter;
    private final List<UnifyRule> rules;
    private final Map<String, Config.MaterializedViewDefinition> materializedViews;

    // Caches to avoid re-converting MV definitions or creating replacement scans repeatedly
    private final Map<String, SimpleRelNode> mvPlanCache = new HashMap<>();
    private final Map<String, SimpleRelNode> mvReplacementCache = new HashMap<>();

    /**
     * Constructor.
     * @param config The application configuration containing MV definitions and table schemas.
     * @param converter The converter used to turn MV definition SQL into IR plans.
     */
    public SimpleSubstitutionVisitor(Config config, TrinoAstToSimpleRelConverter converter) {
        this.config = Objects.requireNonNull(config, "config is null");
        this.converter = Objects.requireNonNull(converter, "converter is null");
        this.materializedViews = config.getMaterializedViews() != null ? config.getMaterializedViews() : Collections.emptyMap();
        this.rules = List.of(
            new ProjectFilterScanMatchRule(),
            new FilterScanMatchRule()
        );
        // System.out.println("SimpleSubstitutionVisitor initialized with " + rules.size() + " rule(s)."); // Optional
    }

    /**
     * Public entry point: Starts the rewriting process for a given query plan.
     * @param queryPlan The root node of the query plan IR to rewrite.
     * @return The rewritten query plan (or the original if no substitutions were made).
     */
    public SimpleRelNode rewrite(SimpleRelNode queryPlan) {
         //System.out.println("\nVisitor: Starting rewrite process...");
         // Start visiting from the root node, no initial context (null)
         SimpleRelNode result = queryPlan.accept(this, null);
         //System.out.println("Visitor: Rewrite process finished.");
         return result;
    }

    // --- Visitor Methods ---
    // General pattern:
    // 1. Visit children recursively.
    // 2. Rebuild the current node with potentially rewritten children.
    // 3. Try to match the rebuilt node against MVs using rules.
    // 4. Return the matched/rewritten node or the rebuilt node if no match.

    @Override
    public SimpleRelNode visitScan(SimpleScan node, Void context) {
         //System.out.println("Visitor: Visiting Scan: " + node.getTableName());
        // Scans are leaf nodes, no children to visit.
        // Try to match this scan directly against MVs (might match an MV that is just 'SELECT * FROM base').
        return tryMatchAndReplace(node);
    }

    @Override
    public SimpleRelNode visitFilter(SimpleFilter node, Void context) {
         //System.out.println("Visitor: Visiting Filter: " + node.getCondition());
        // 1. Visit child input
        SimpleRelNode originalInput = node.getInput();
        SimpleRelNode rewrittenInput = originalInput.accept(this, context); // Recursive call

        // 2. Rebuild filter node if input changed, otherwise reuse original node
        SimpleRelNode candidateNode = (rewrittenInput == originalInput)
                                      ? node // No change in child, reuse original node
                                      : new SimpleFilter(rewrittenInput, node.getCondition());

        // 3. Try to match the potentially rebuilt Filter node against MVs
        return tryMatchAndReplace(candidateNode);
    }

    @Override
    public SimpleRelNode visitProject(SimpleProject node, Void context) {
         //System.out.println("Visitor: Visiting Project: " + node.getProjections());
        // 1. Visit child input
        SimpleRelNode originalInput = node.getInput();
        SimpleRelNode rewrittenInput = originalInput.accept(this, context); // Recursive call

        // 2. Rebuild project node if input changed, otherwise reuse original node
        SimpleRelNode candidateNode = (rewrittenInput == originalInput)
                                      ? node
                                      : new SimpleProject(rewrittenInput, node.getProjections(), node.getSchema());
                                      // Note: Assumes output schema doesn't change if only input node changes.

        // 3. Try to match the potentially rebuilt Project node against MVs
        return tryMatchAndReplace(candidateNode);
    }

    // --- Default/Fallback Visitor Method ---
    @Override
    public SimpleRelNode visitNode(SimpleRelNode node, Void context) {
        // Handle nodes other than Scan, Filter, Project if they are added later (e.g., Agg, Join)
        // Default behavior: rewrite children, rebuild node, then try matching.
         //System.out.println("Visitor: Visiting generic Node: " + node.getNodeType());

         // 1. Rewrite inputs
         boolean changed = false;
         List<SimpleRelNode> newInputs = new ArrayList<>();
         for (SimpleRelNode input : node.getInputs()) {
             SimpleRelNode newIn = input.accept(this, context);
             if (newIn != input) {
                 changed = true;
             }
             newInputs.add(newIn);
         }

         // 2. Rebuild node if inputs changed
         SimpleRelNode candidateNode = changed ? node.withInputs(newInputs) : node;

         // 3. Try to match
         return tryMatchAndReplace(candidateNode);
    }


    // --- Core Matching Logic ---

/**
     * Tries to match the given queryNode against all configured materialized views using the available rules.
     * @param queryNode The current node (potentially rebuilt with rewritten children) to attempt matching.
     * @return The rewritten plan fragment if a match is found, otherwise the original queryNode passed in.
     */
    private SimpleRelNode tryMatchAndReplace(SimpleRelNode queryNode) {
        //System.out.println("Visitor: Attempting match for Query Node Type: " + queryNode.getNodeType());

        if (materializedViews.isEmpty()) {
            return queryNode; // No MVs to match against
        }

       for (Map.Entry<String, Config.MaterializedViewDefinition> mvEntry : materializedViews.entrySet()) {
           String mvName = mvEntry.getKey();
           Config.MaterializedViewDefinition mvDef = mvEntry.getValue();

           // ++ ADDED: Log which MV is being checked and its definition ++
           //System.out.println("    -> Checking against MV: '" + mvName + "'");
           //System.out.println("       MV Target Table : " + mvDef.getTargetTable());
           //System.out.println("       MV Definition SQL:\n" + mvDef.getDefinition().indent(8)); // Indent SQL
           // ++ END ADDED LOGGING ++


           // Get (or convert and cache) the MV definition plan (target plan)
           SimpleRelNode targetPlan = getMvDefinitionPlan(mvName, mvDef);
            if (targetPlan == null) {
                 //System.out.println("       Skipping MV '" + mvName + "' due to conversion error.");
                 continue; // Skip MV if definition fails to convert
            }

            // Get (or create and cache) the MV replacement scan node
            SimpleRelNode replacementPlan = getMvReplacementPlan(mvName, mvDef, targetPlan);
            if (replacementPlan == null) {
                System.out.println("       Skipping MV '" + mvName + "' due to error creating replacement plan.");
                continue; // Skip MV if replacement can't be determined
            }

           // Apply rules to see if queryNode matches the targetPlan structure
           for (UnifyRule rule : rules) {
                // System.out.println("       Applying Rule: " + rule.getClass().getSimpleName()); // Optional rule log
               try {
                   UnifyResult result = rule.apply(queryNode, targetPlan, replacementPlan, this);

                   if (result != null) {
                       // Match found!
                       System.out.println("INFO: Match occurred while processing Query Node Type: " + queryNode.getNodeType());
                       System.out.println(">>> MATCH FOUND by rule " + rule.getClass().getSimpleName() + " for MV: '" + mvName + "' <<<");
                       SimpleRelNode rewrittenFragment = result.buildPlanFragment();
                        // System.out.println(">>> Visitor: Rewritten Fragment:\n" + rewrittenFragment.toString("  ")); // Optional fragment log
                       // Return the rewritten part immediately.
                       // A query node can only be replaced by one MV match.
                       return rewrittenFragment;
                   }
                   // else: Rule didn't match, continue to next rule
               } catch (Exception e) {
                    // System.err.println("ERROR applying rule " + rule.getClass().getSimpleName() + " for MV '" + mvName + "': " + e.getMessage());
                    // Optionally print stack trace for debugging: e.printStackTrace();
               }
           } // End loop through rules
            //System.out.println("       No matching rule found for MV '" + mvName + "' for this query node.");

       } // End loop through MVs

       // No rule matched this query node against any MV after checking all of them
       // System.out.println("Visitor: No MV match found for Query Node Type: " + queryNode.getNodeType()); // Optional
       return queryNode; // Return the original node (or the one with rewritten children)
   }

    // --- Helper methods for MV Plan Caching ---

    private SimpleRelNode getMvDefinitionPlan(String mvName, Config.MaterializedViewDefinition mvDef) {
        return mvPlanCache.computeIfAbsent(mvName, k -> {
            try {
                //System.out.println("Visitor: Converting MV definition to IR: " + mvName);
                Statement mvStatement = SqlStringUtils.parseSqlStatement(mvDef.getDefinition());
                SimpleRelNode plan = converter.convert(mvStatement);
                 //System.out.println("Visitor:   MV Definition IR ("+mvName+"):\n" + plan.toString("    "));
                return plan;
            } catch (Exception e) {
                System.err.println("Visitor: ERROR converting MV definition '" + mvName + "' to IR: " + e.getMessage());
                // Return null to indicate failure, preventing further use of this MV in this run
                return null;
            }
        });
    }

     private SimpleRelNode getMvReplacementPlan(String mvName, Config.MaterializedViewDefinition mvDef, SimpleRelNode targetPlan) {
         // Requires targetPlan to determine the schema for the replacement scan
         if (targetPlan == null) return null; // Cannot determine replacement if target failed

         return mvReplacementCache.computeIfAbsent(mvName, k -> {
             try {
                 String replacementTableName = mvDef.getTargetTable();
                 // Schema for the replacement scan is the output schema of the MV definition plan (targetPlan)
                 Map<String, String> mvSchema = targetPlan.getSchema();
                 if (mvSchema == null || mvSchema.isEmpty()) {
                     System.err.println("Visitor: WARN - MV definition plan for '" + mvName + "' has null/empty output schema. Replacement scan schema may be incorrect.");
                     // Attempt to look up replacement table schema from config as fallback? Requires targetTable to be in config.tables
                     // Map<String, String> fallbackSchema = config.getTableSchema(replacementTableName); // This might fail if MV table not listed
                     // For POC, proceed with empty schema if needed, though likely indicates an issue.
                     mvSchema = new LinkedHashMap<>();
                 }
                  //System.out.println("Visitor:   Creating MV replacement: Scan(" + replacementTableName + ") Schema: " + mvSchema.keySet());
                  // Create the SimpleScan node representing the precomputed MV table
                 return new SimpleScan(replacementTableName, mvSchema);
             } catch (Exception e) {
                  System.err.println("Visitor: ERROR creating replacement plan for MV '" + mvName + "': " + e.getMessage());
                  return null;
             }
         });
     }
}