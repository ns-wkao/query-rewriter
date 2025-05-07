package com.rewrite.poc;

// Imports will be reduced as test-specific ones are removed
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

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

        // --- Setup Converters/Visitor (if needed for non-test operations) ---
        // TrinoAstToSimpleRelConverter converter = new TrinoAstToSimpleRelConverter(config);
        // SimpleSubstitutionVisitor visitor = new SimpleSubstitutionVisitor(config, converter);
        // SimpleRelToSqlConverter sqlConverter = new SimpleRelToSqlConverter();

        // The main application logic would go here if it's more than just tests.
        // For now, it just loads config and prints a completion message.

        System.out.println("\n--- POC Application Initialized (Tests are in RewritePocTests.java) ---");
    }

    /**
     * Prints a header with current timestamp.
     */
    private static void printHeader() {
        ZoneId zoneId = ZoneId.of("Asia/Taipei");
        ZonedDateTime zdtNow = ZonedDateTime.now(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
        System.out.println("--- Trino MV Rewrite POC ---");
        System.out.println("Run Time: " + zdtNow.format(formatter));
        System.out.println("----------------------------");
    }
}
