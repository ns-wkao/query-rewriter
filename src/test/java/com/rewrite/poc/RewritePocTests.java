package com.rewrite.poc;

import com.rewrite.poc.ir.SimpleRelNode;
import com.rewrite.poc.util.SqlStringUtils;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@TestInstance(TestInstance.Lifecycle.PER_CLASS) // To allow @BeforeAll on non-static method
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // To run tests in a defined order if needed
public class RewritePocTests {

    private Config config;
    private TrinoAstToSimpleRelConverter converter;
    private SimpleSubstitutionVisitor visitor;
    private SimpleRelToSqlConverter sqlConverter;

    // Test Queries
    private static final String QUERY_EXACT_MATCH = "SELECT ss_item_sk, ss_ticket_number, ss_quantity, ss_store_sk, ss_net_paid "
                                                  + "FROM store_sales WHERE ss_quantity > 10";
    private static final String QUERY_RESIDUAL_FILTER = "SELECT ss_item_sk, ss_ticket_number, ss_quantity, ss_store_sk, ss_net_paid "
                                                      + "FROM store_sales WHERE ss_quantity > 10 AND ss_store_sk = 123";
    private static final String QUERY_RESIDUAL_PROJ = "SELECT ss_item_sk, ss_net_paid "
                                                     + "FROM store_sales WHERE ss_quantity > 10";
    private static final String QUERY_RESIDUAL_BOTH = "SELECT ss_item_sk, ss_net_paid "
                                                     + "FROM store_sales WHERE ss_quantity > 10 AND ss_store_sk = 123";
    private static final String QUERY_NO_MATCH_FILTER = "SELECT ss_item_sk, ss_net_paid FROM store_sales WHERE ss_store_sk = 5";
    private static final String QUERY_JOIN_SELECTS_AVAILABLE_COL = "SELECT i.i_category FROM store_sales s JOIN items i ON s.ss_item_sk = i.i_item_sk";


    @BeforeAll
    void setup() {
        printHeader(); // Keep the header for test run context

        // --- Load Config ---
        try {
            config = Config.loadFromResources("config.yaml");
            System.out.println("INFO: Configuration loaded successfully.");
            System.out.println("Loaded Tables: " + config.getTables().keySet());
            System.out.println("Loaded MVs: " + config.getMaterializedViews().keySet());
        } catch (Exception e) {
            System.err.println("FATAL: Configuration loading failed: " + e.getMessage());
            e.printStackTrace();
            // Fail fast if config doesn't load
            throw new RuntimeException("Failed to load configuration", e);
        }

        // --- Setup Converters/Visitor ---
        converter = new TrinoAstToSimpleRelConverter(config);
        visitor = new SimpleSubstitutionVisitor(config, converter);
        sqlConverter = new SimpleRelToSqlConverter();
    }

    @Test
    @Order(1)
    @DisplayName("Exact Match Test")
    void testExactMatch() {
        runRewriteTest("Exact Match", QUERY_EXACT_MATCH);
    }

    @Test
    @Order(2)
    @DisplayName("Residual Filter Test")
    void testResidualFilter() {
        runRewriteTest("Residual Filter", QUERY_RESIDUAL_FILTER);
    }

    @Test
    @Order(3)
    @DisplayName("Residual Projection Test")
    void testResidualProjection() {
        runRewriteTest("Residual Projection", QUERY_RESIDUAL_PROJ);
    }

    @Test
    @Order(4)
    @DisplayName("Residual Filter & Projection Test")
    void testResidualFilterAndProjection() {
        runRewriteTest("Residual Filter & Projection", QUERY_RESIDUAL_BOTH);
    }

    @Test
    @Order(5)
    @DisplayName("No Match (Filter) Test")
    void testNoMatchFilter() {
        runRewriteTest("No Match (Filter)", QUERY_NO_MATCH_FILTER);
    }

    @Test
    @Order(6)
    @DisplayName("Join Rewrite (Select Category) Test")
    void testJoinRewriteSelectCategory() {
        runRewriteTest("Join Rewrite (Select Category)", QUERY_JOIN_SELECTS_AVAILABLE_COL);
    }

    /**
     * Runs a full rewrite test for a given SQL query and prints key information.
     * Reuses the existing converters and visitor initialized in @BeforeAll.
     */
    private void runRewriteTest(String testName, String sql) {
        System.out.println("\n========================================================");
        System.out.println("TEST CASE: " + testName);
        System.out.println("========================================================");
        System.out.println("1. Original Query:");
        System.out.println(sql);
        System.out.println("--------------------------------------------------------");

        try {
            Statement ast = SqlStringUtils.parseSqlStatement(sql);
            System.out.println("2. Full AST: " + ast);

            SimpleRelNode initialPlan = converter.convert(ast);
            System.out.println("--------------------------------------------------------");
            System.out.println("3. Initial IR Plan:");
            System.out.println(initialPlan.toString("  "));

            System.out.println("--------------------------------------------------------");
            System.out.println("4. Applying Rewrite Rules...");
            SimpleRelNode finalPlan = visitor.rewrite(initialPlan);

            boolean wasRewritten = (finalPlan != initialPlan);
            System.out.println("--------------------------------------------------------");
            System.out.println("5. Rewritten: " + (wasRewritten ? "Yes" : "No"));

            System.out.println("--------------------------------------------------------");
            System.out.println("6. Final IR Plan:");
            System.out.println(finalPlan.toString("  "));

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
        ZoneId zoneId = ZoneId.of("Asia/Taipei");
        ZonedDateTime zdtNow = ZonedDateTime.now(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
        System.out.println("--- Trino MV Rewrite POC Test Run ---");
        System.out.println("Run Time: " + zdtNow.format(formatter));
        System.out.println("-------------------------------------");
    }
}
