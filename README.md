# Trino MV Rewrite POC

## Project Goal

This project is a Proof-of-Concept (POC) demonstrating how SQL queries can be automatically rewritten to leverage pre-computed Materialized Views (MVs) when possible. The goal is to potentially speed up query execution by substituting parts of the query plan with a scan of the relevant MV.

## Core Concepts

*   **Trino SQL Parser:** Leverages Trino's robust SQL parser (`io.trino.sql.parser.SqlParser`) to generate an Abstract Syntax Tree (AST) from the input SQL query.
*   **Custom Intermediate Representation (IR):** Defines a simplified relational algebra IR (`SimpleRelNode` and its implementations: `SimpleScan`, `SimpleFilter`, `SimpleProject`) tailored for this POC. This avoids the complexity of Trino's full `PlanNode` IR. Expressions within the IR (`SimpleExpression`) wrap the original Trino expression nodes.
*   **Configuration-Driven:** Table schemas and MV definitions (including their SQL definition and target storage table) are defined externally in `config.yaml`.
*   **Rule-Based Substitution:** Uses a visitor pattern (`SimpleSubstitutionVisitor`) combined with specific matching rules (`UnifyRule`) to identify parts of the query IR that can be replaced by an MV.

## Workflow

The query rewriting process follows these steps:

1.  **Parse SQL:** The input SQL query string is parsed into a Trino AST (`Statement`) using `SqlStringUtils.parseSqlStatement`.
2.  **Load Configuration:** Table schemas and MV definitions are loaded from `src/main/resources/config.yaml` using the `Config` class and Jackson YAML parser.
3.  **Convert AST to Custom IR:** The `TrinoAstToSimpleRelConverter` traverses the Trino AST and builds the initial custom IR plan (`SimpleRelNode` tree). This POC converter primarily supports simple `SELECT [cols] FROM [table] WHERE [condition]` structures.
4.  **Rewrite IR:**
    *   The `SimpleSubstitutionVisitor` traverses the query IR plan (post-order: children first).
    *   For each node in the query plan, it attempts to match it against the IR plans derived from the configured MV definitions.
    *   Matching is performed by `UnifyRule` implementations (`FilterScanMatchRule`, `ProjectFilterScanMatchRule`). These rules check:
        *   Structural compatibility (e.g., matching `Filter -> Scan` patterns).
        *   Base table equality (`SimpleScan.structurallyEquals`).
        *   Filter implication: Whether the query's filter condition logically implies the MV's filter condition (`SimpleExpression.splitFilter`). This determines if a *residual filter* is needed.
        *   Projection compatibility: Whether the columns required by the query are available in the MV's output, and if the query's projection list differs from the MV's (requiring a *residual projection*).
    *   If a rule finds a match, it returns a `UnifyResult` containing the replacement node (a `SimpleScan` of the MV's `targetTable`) and any necessary residual filter or projection expressions.
    *   The `UnifyResult.buildPlanFragment` method constructs the rewritten IR segment by layering the residual operations on top of the MV scan.
    *   The visitor replaces the original query plan segment with the rewritten one.
5.  **Convert Final IR to SQL:** The `SimpleRelToSqlConverter` traverses the final (potentially rewritten) IR plan and generates a representative SQL string. It uses derived tables (subqueries with aliases) to handle nested operations.

## Key Components

*   **`Main.java`:** Entry point, orchestrates the loading, conversion, rewriting, and testing process. Contains sample queries.
*   **`Config.java`:** Loads and holds configuration from `config.yaml`.
*   **`config.yaml`:** Defines table schemas and MV definitions (`definition` SQL, `targetTable` name).
*   **`TrinoAstToSimpleRelConverter.java`:** Converts Trino AST (`Statement`) to the custom IR (`SimpleRelNode`).
*   **`ir/` package:**
    *   `SimpleRelNode.java`: Base interface for IR nodes.
    *   `SimpleScan.java`: Represents scanning a base table or MV target table.
    *   `SimpleFilter.java`: Represents a filter operation (WHERE clause).
    *   `SimpleProject.java`: Represents a projection operation (SELECT clause).
    *   `SimpleExpression.java`: Wraps a Trino `Expression` AST node, provides string representation and `splitFilter` logic.
    *   `SimpleNodeVisitor.java`: Interface for the visitor pattern used to traverse the IR.
*   **`SimpleSubstitutionVisitor.java`:** Traverses the query IR and attempts MV substitutions using rules. Manages MV plan caching.
*   **`rules/` package:**
    *   `UnifyRule.java`: Interface for matching rules.
    *   `FilterScanMatchRule.java`: Matches `Filter -> Scan` patterns.
    *   `ProjectFilterScanMatchRule.java`: Matches `Project -> Filter -> Scan` patterns.
    *   `UnifyResult.java`: Holds the result of a successful rule match and builds the rewritten plan fragment.
*   **`SimpleRelToSqlConverter.java`:** Converts the final IR plan back into a SQL string representation.
*   **`util/SqlStringUtils.java`:** Helper methods for parsing SQL strings into Trino AST nodes and formatting them back.

## Configuration (`config.yaml`)

The `config.yaml` file has two main sections:

*   **`tables`:** A map where keys are table names. Each table has a `schema` defined as a list of maps, each containing a single column name and its type string (e.g., `- ss_item_sk: BIGINT`).
*   **`materializedViews`:** A map where keys are logical MV names. Each MV has:
    *   `definition`: The SQL query string defining the MV.
    *   `targetTable`: The name of the actual table where the MV's results are stored (e.g., an Iceberg table name).

## Running the POC

1.  Ensure you have a Java Development Kit (JDK) and Maven installed.
2.  Build the project (e.g., using `mvn clean package`).
3.  Run the `com.rewrite.poc.Main` class from your IDE or using Maven:
    ```bash
    mvn exec:java -Dexec.mainClass="com.rewrite.poc.Main"
    ```
4.  Observe the console output. For each test case in `Main.java`, it will print:
    *   The original SQL query.
    *   The initial IR plan derived from the query.
    *   Log messages indicating which MVs and rules are being checked.
    *   A "MATCH FOUND" message if a substitution occurs.
    *   Whether the plan was rewritten ("Rewritten: Yes/No").
    *   The final IR plan (original or rewritten).
    *   The final SQL generated from the final IR plan.

## Limitations (POC)

This is a simplified POC with several limitations:

*   **SQL Support:** Only handles basic `SELECT [cols] FROM [single_table] WHERE [condition]` queries. Joins, aggregations (GROUP BY, COUNT, SUM, etc.), ORDER BY, LIMIT, DISTINCT, subqueries in FROM/WHERE clauses, window functions, etc., are **not** supported by the `TrinoAstToSimpleRelConverter` or the rules.
*   **Rule Coverage:** Only includes rules for `Filter -> Scan` and `Project -> Filter -> Scan` patterns. More complex query/MV patterns would require additional rules.
*   **Expression Handling:** `SimpleExpression` primarily relies on string comparison for equality and has very basic `splitFilter` logic (only handles top-level `AND`). Complex expression canonicalization or equivalence checking is not implemented. `getReferencedColumns` is also basic.
*   **Schema/Type Handling:** Type inference in `TrinoAstToSimpleRelConverter` and `UnifyResult` is minimal ("UNKNOWN_POC"). A real system needs robust type checking and propagation.
*   **SQL Generation:** `SimpleRelToSqlConverter` produces pseudo-SQL using derived tables. It doesn't handle identifier quoting correctly and may produce non-optimal or syntactically incorrect SQL for complex cases.
*   **Error Handling:** Error handling is basic.
