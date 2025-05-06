package com.rewrite.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.LinkedHashMap; // Preserve definition order

/**
 * Loads and holds configuration for table schemas and materialized views
 * from the config.yaml file.
 */
public class Config {

    // These field names must match the top-level keys in config.yaml
    private Map<String, TableDefinition> tables;
    private Map<String, MaterializedViewDefinition> materializedViews;

    // Getters are needed for Jackson deserialization
    public Map<String, TableDefinition> getTables() {
        return tables;
    }

    public void setTables(Map<String, TableDefinition> tables) {
        this.tables = tables;
    }

    public Map<String, MaterializedViewDefinition> getMaterializedViews() {
        return materializedViews;
    }

    public void setMaterializedViews(Map<String, MaterializedViewDefinition> materializedViews) {
        this.materializedViews = materializedViews;
    }

    // --- Inner classes representing the structure in YAML ---

    public static class TableDefinition {
        // Field name 'schema' matches the key in YAML
        private List<Map<String, String>> schema;
        // Processed map for easier lookup
        private transient Map<String, String> schemaMap; // transient: don't serialize if needed

        public List<Map<String, String>> getSchema() {
            return schema;
        }

        public void setSchema(List<Map<String, String>> schema) {
            this.schema = schema;
            // Process the schema list into a map when it's set
            processSchemaList();
        }

        // Helper method to convert the list of single-entry maps to a map
        private void processSchemaList() {
            if (schema == null) {
                schemaMap = Collections.emptyMap();
                return;
            }
            // Use LinkedHashMap to preserve column order from YAML
            schemaMap = schema.stream()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> v1, // Handle duplicates if any (keep first)
                    LinkedHashMap::new
                ));
        }

        /**
         * Gets the processed schema as a Map<ColumnName, TypeString>.
         * Ensures the processing happens even if accessed before Jackson setter is fully done.
         * @return An unmodifiable map of the table's schema.
         */
        public Map<String, String> getSchemaMap() {
            if (schemaMap == null) {
                processSchemaList(); // Ensure it's processed
            }
            // Return an unmodifiable view to prevent external changes
            return Collections.unmodifiableMap(schemaMap);
        }
    }

    public static class MaterializedViewDefinition {
        // Field names 'definition' and 'targetTable' match keys in YAML
        private String definition;
        private String targetTable;

        public String getDefinition() {
            return definition;
        }

        public void setDefinition(String definition) {
            this.definition = definition;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }
    }

    // --- Loading Logic ---

    /**
     * Loads configuration from the specified classpath resource path.
     * @param resourcePath Path relative to the classpath root (e.g., "config.yaml")
     * @return Loaded Config object.
     * @throws RuntimeException if loading fails.
     */
    public static Config loadFromResources(String resourcePath) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try (InputStream is = Config.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new RuntimeException("Cannot find configuration file in classpath: " + resourcePath);
            }
            Config config = mapper.readValue(is, Config.class);

            // Ensure schema maps are processed after loading (though setter should handle it)
            if (config.getTables() != null) {
                config.getTables().values().forEach(TableDefinition::getSchemaMap);
            }
             if (config.getMaterializedViews() == null) {
                 config.materializedViews = Collections.emptyMap(); // Avoid NPE later
             }

            return config;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration from " + resourcePath, e);
        }
    }

    // --- Convenience Accessors ---

    /**
     * Gets the schema map for a specific table.
     * @param tableName The name of the table.
     * @return The schema map (ColumnName -> TypeString).
     * @throws IllegalArgumentException if the table or its schema is not found.
     */
    public Map<String, String> getTableSchema(String tableName) {
        Objects.requireNonNull(tableName, "tableName cannot be null");
        TableDefinition tableDef = (tables != null) ? tables.get(tableName) : null;
        if (tableDef == null) {
             // Check case-insensitivity? For POC, assume exact match.
            throw new IllegalArgumentException("Table definition not found in config: " + tableName);
        }
        Map<String, String> schema = tableDef.getSchemaMap();
         if (schema == null || schema.isEmpty()) {
             // This might happen if the YAML has the table but empty/no schema defined
             System.err.println("Warning: Schema map is null or empty for table: " + tableName);
             return Collections.emptyMap(); // Return empty map instead of throwing?
         }
        return schema;
    }

    /**
     * Gets the definition for a specific materialized view.
     * @param mvName The logical name of the materialized view.
     * @return The MaterializedViewDefinition object.
     * @throws IllegalArgumentException if the materialized view is not found.
     */
    public MaterializedViewDefinition getMaterializedView(String mvName) {
        Objects.requireNonNull(mvName, "mvName cannot be null");
        MaterializedViewDefinition mvDef = (materializedViews != null) ? materializedViews.get(mvName) : null;
        if (mvDef == null) {
            throw new IllegalArgumentException("Materialized View definition not found in config: " + mvName);
        }
        return mvDef;
    }
}