package com.rewrite.poc;

import com.rewrite.poc.ir.SimpleRelNode;
import com.rewrite.poc.util.SqlStringUtils;
import io.trino.sql.tree.Statement;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public class Main {

    public static void main(String[] args) {
        printHeader();

        // --- Load Config ---
        Config config;
        try {
            config = Config.loadFromResources("config.yaml");
            System.out.println("INFO: Configuration loaded successfully.");
            System.out.println("Loaded Tables: " + config.getTables().keySet());
            System.out.println("Loaded MVs: " + config.getMaterializedViews().keySet());
        } catch (Exception e) {
            System.err.println("FATAL: Configuration loading failed: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // --- Setup Converters/Visitor ---
        TrinoAstToSimpleRelConverter converter = new TrinoAstToSimpleRelConverter(config);
        // NOTE: Comment out verbose logging inside visitor/rules/converter for cleaner output
        SimpleSubstitutionVisitor visitor = new SimpleSubstitutionVisitor(config, converter);
        SimpleRelToSqlConverter sqlConverter = new SimpleRelToSqlConverter();

        // --- Test Cases ---
        String queryExactMatch = "SELECT ss_item_sk, ss_ticket_number, ss_quantity, ss_store_sk, ss_net_paid "
                               + "FROM store_sales WHERE ss_quantity > 10";
        String queryResidualFilter = "SELECT ss_item_sk, ss_ticket_number, ss_quantity, ss_store_sk, ss_net_paid "
                                   + "FROM store_sales WHERE ss_quantity > 10 AND ss_store_sk = 123";
        String queryResidualProj = "SELECT ss_item_sk, ss_net_paid "
                                  + "FROM store_sales WHERE ss_quantity > 10";
        String queryResidualBoth = "SELECT ss_item_sk, ss_net_paid "
                                  + "FROM store_sales WHERE ss_quantity > 10 AND ss_store_sk = 123";
        String queryNoMatchFilter = "SELECT ss_item_sk, ss_net_paid FROM store_sales WHERE ss_store_sk = 5";
        String queryJoinSelectsAvailableCol = "SELECT i.i_category FROM store_sales s JOIN items i ON s.ss_item_sk = i.i_item_sk";


        // --- Run Tests ---
        runRewriteTest("Exact Match", converter, visitor, sqlConverter, queryExactMatch);
        runRewriteTest("Residual Filter", converter, visitor, sqlConverter, queryResidualFilter);
        runRewriteTest("Residual Projection", converter, visitor, sqlConverter, queryResidualProj);
        runRewriteTest("Residual Filter & Projection", converter, visitor, sqlConverter, queryResidualBoth);
        runRewriteTest("No Match (Filter)", converter, visitor, sqlConverter, queryNoMatchFilter);
        runRewriteTest("Join Rewrite (Select Category)", converter, visitor, sqlConverter, queryJoinSelectsAvailableCol);

        System.out.println("\n--- POC Run Complete ---");
    }

    /**
     * Runs a full rewrite test for a given SQL query and prints key information.
     */
    private static void runRewriteTest(String testName,
                                       TrinoAstToSimpleRelConverter converter,
                                       SimpleSubstitutionVisitor visitor,
                                       SimpleRelToSqlConverter sqlConverter,
                                       String sql) {

        System.out.println("\n========================================================");
        System.out.println("TEST CASE: " + testName);
        System.out.println("========================================================");
        System.out.println("1. Original Query:");
        System.out.println(sql);
        System.out.println("--------------------------------------------------------");

        try {
            // 2. Parse to AST
            Statement ast = SqlStringUtils.parseSqlStatement(sql);
            System.out.println("2. Full AST: " + ast);

            // 3. Convert to Initial IR
            SimpleRelNode initialPlan = converter.convert(ast);
            System.out.println("--------------------------------------------------------");
            System.out.println("3. Initial IR Plan:");
            System.out.println(initialPlan.toString("  ")); // Indented IR plan

            // 4. Run Rewriter (Visitor)
            System.out.println("--------------------------------------------------------");
            System.out.println("4. Applying Rewrite Rules...");
            SimpleRelNode finalPlan = visitor.rewrite(initialPlan); // Visitor logs "MATCH FOUND" internally if successful

            // 5. Check if Rewritten
            // Using simple object identity and basic structural check for POC
            boolean wasRewritten = (finalPlan != initialPlan);
            System.out.println("--------------------------------------------------------");
            System.out.println("5. Rewritten: " + (wasRewritten ? "Yes" : "No"));

            // 6. Final IR Plan
            System.out.println("--------------------------------------------------------");
            System.out.println("6. Final IR Plan:");
            System.out.println(finalPlan.toString("  "));

            // 7. Generated Final SQL
            System.out.println("--------------------------------------------------------");
            System.out.println("7. Generated Final SQL:");
            String finalSql = sqlConverter.convert(finalPlan);
            System.out.println(finalSql);

        } catch (UnsupportedOperationException e) {
             System.out.println("--------------------------------------------------------");
             System.out.println("INFO: Query structure not supported by this POC's converter/rules.");
             System.out.println("      Reason: " + e.getMessage());
        } catch (Exception e) {
             System.out.println("--------------------------------------------------------");
             System.err.println("ERROR: An unexpected error occurred during processing:");
             e.printStackTrace();
        }
        System.out.println("========================================================");
    }

    /**
     * Prints a header with current timestamp.
     */
    private static void printHeader() {
        // Use current context time if available, otherwise system default
        ZoneId zoneId = ZoneId.of("Asia/Taipei"); // Correct based on context

        // ++ Use ZonedDateTime instead of LocalDateTime ++
        ZonedDateTime zdtNow = ZonedDateTime.now(zoneId);

        // Formatter remains the same
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");

        System.out.println("--- Trino MV Rewrite POC ---");
        // ++ Format the ZonedDateTime object ++
        System.out.println("Run Time: " + zdtNow.format(formatter));
        System.out.println("----------------------------");
    }
}