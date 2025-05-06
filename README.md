# Trino MV Rewrite POC

## Project Goal

This project is a Proof-of-Concept (POC) demonstrating how SQL queries can be automatically rewritten to leverage pre-computed Materialized Views (MVs) when possible. The goal is to potentially speed up query execution by substituting parts of the query plan with a scan of the relevant MV.

## Core Concepts

* **Trino SQL Parser:** Leverages Trino's robust SQL parser (`io.trino.sql.parser.SqlParser`) to generate an Abstract Syntax Tree (AST) from the input SQL query.
* **Custom Intermediate Representation (IR):** Defines a simplified relational algebra IR (`SimpleRelNode` and its implementations: `SimpleScan`, `SimpleFilter`, `SimpleProject`, `SimpleJoin`) tailored for this POC. This avoids the complexity of Trino's full `PlanNode` IR. Expressions within the IR (`SimpleExpression`) wrap the original Trino expression nodes.
* **Configuration-Driven:** Table schemas and MV definitions (including their SQL definition and target storage table) are defined externally in `config.yaml`.
* **Rule-Based Substitution:** Uses a visitor pattern (`SimpleSubstitutionVisitor`) combined with specific matching rules (`UnifyRule`) to identify parts of the query IR that can be replaced by an MV.

## Workflow

The query rewriting process follows these steps:

1.  **Parse SQL:** The input SQL query string is parsed into a Trino AST (`Statement`) using `SqlStringUtils.parseSqlStatement`.
2.  **Load Configuration:** Table schemas and MV definitions are loaded from `src/main/resources/config.yaml` using the `Config` class and Jackson YAML parser.
3.  **Convert AST to Custom IR:** The `TrinoAstToSimpleRelConverter` traverses the Trino AST and builds the initial custom IR plan (`SimpleRelNode` tree). This POC converter supports simple `SELECT [cols] FROM [table] WHERE [condition]` structures and basic `SELECT [cols] FROM [table1] INNER JOIN [table2] ON [condition]` structures. Table aliases are handled during conversion.
4.  **Rewrite IR:**
    * The `SimpleSubstitutionVisitor` traverses the query IR plan (post-order: children first).
    * For each node in the query plan, it attempts to match it against the IR plans derived from the configured MV definitions.
    * Matching is performed by `UnifyRule` implementations (`ProjectJoinMatchRule`, `JoinMatchRule`, `ProjectFilterScanMatchRule`, `FilterScanMatchRule`). These rules check:
        * Structural compatibility (e.g., matching `Filter -> Scan`, `Project -> Join` patterns).
        * Base table equality (`SimpleScan.structurallyEquals`) or underlying join input equality (`structurallyEquals`).
        * Join condition equality (based on `SimpleExpression.equals`).
        * Filter implication: Whether the query's filter condition logically implies the MV's filter condition (`SimpleExpression.splitFilter`). This determines if a *residual filter* is needed.
        * Projection compatibility: Whether the columns required by the query are available in the MV's output, and if the query's projection list differs from the MV's (requiring a *residual projection*).
    * If a rule finds a match, it returns a `UnifyResult` containing the replacement node (a `SimpleScan` of the MV's `targetTable`) and any necessary residual filter or projection expressions.
    * The `UnifyResult.buildPlanFragment` method constructs the rewritten IR segment by layering the residual operations on top of the MV scan.
    * The visitor replaces the original query plan segment with the rewritten one.
5.  **Convert Final IR to SQL:** The `SimpleRelToSqlConverter` traverses the final (potentially rewritten) IR plan and generates a representative SQL string. It uses derived tables (subqueries with aliases) to handle nested operations, including joins.

## Key Components

* **`Main.java`:** Entry point, orchestrates the loading, conversion, rewriting, and testing process. Contains sample queries.
* **`Config.java`:** Loads and holds configuration from `config.yaml`.
* **`config.yaml`:** Defines table schemas and MV definitions (`definition` SQL, `targetTable` name).
* **`TrinoAstToSimpleRelConverter.java`:** Converts Trino AST (`Statement`) to the custom IR (`SimpleRelNode`). Handles basic SELECT-FROM-WHERE and SELECT-FROM-INNER_JOIN.
* **`ir/` package:**
    * `SimpleRelNode.java`: Base interface for IR nodes.
    * `SimpleScan.java`: Represents scanning a base table or MV target table.
    * `SimpleFilter.java`: Represents a filter operation (WHERE clause).
    * `SimpleProject.java`: Represents a projection operation (SELECT clause).
    * `SimpleJoin.java`: Represents an INNER JOIN operation.
    * `SimpleExpression.java`: Wraps a Trino `Expression` AST node, provides string representation and `splitFilter` logic. **Crucially, its `equals` method currently relies on string comparison.**
    * `SimpleNodeVisitor.java`: Interface for the visitor pattern used to traverse the IR.
* **`SimpleSubstitutionVisitor.java`:** Traverses the query IR and attempts MV substitutions using rules. Manages MV plan caching.
* **`rules/` package:**
    * `UnifyRule.java`: Interface for matching rules.
    * `FilterScanMatchRule.java`: Matches `Filter -> Scan` patterns.
    * `ProjectFilterScanMatchRule.java`: Matches `Project -> Filter -> Scan` patterns.
    * `JoinMatchRule.java`: Matches exact `Join` patterns (compares inputs using `structurallyEquals`).
    * `ProjectJoinMatchRule.java`: Matches `Project -> Join` patterns.
    * `UnifyResult.java`: Holds the result of a successful rule match and builds the rewritten plan fragment.
* **`SimpleRelToSqlConverter.java`:** Converts the final IR plan back into a SQL string representation.
* **`util/SqlStringUtils.java`:** Helper methods for parsing SQL strings into Trino AST nodes and formatting them back.

## Configuration (`config.yaml`)

The `config.yaml` file has two main sections:

* **`tables`:** A map where keys are table names. Each table has a `schema` defined as a list of maps, each containing a single column name and its type string (e.g., `- ss_item_sk: BIGINT`).
* **`materializedViews`:** A map where keys are logical MV names. Each MV has:
    * `definition`: The SQL query string defining the MV. Can include basic INNER JOINs.
    * `targetTable`: The name of the actual table where the MV's results are stored (e.g., an Iceberg table name).

## Running the POC

1.  Ensure you have a Java Development Kit (JDK) and Maven installed.
2.  Build the project (e.g., using `mvn clean package`).
3.  Run the `com.rewrite.poc.Main` class from your IDE or using Maven:
    ```bash
    mvn exec:java -Dexec.mainClass="com.rewrite.poc.Main"
    ```
4.  Observe the console output. For each test case in `Main.java` (e.g., "Exact Match", "Residual Filter", "Join Rewrite (Select Category)"), it will print:
    * The original SQL query.
    * The initial IR plan derived from the query.
    * Log messages indicating which MVs and rules are being checked (including `Rule(ProjJoin)` etc.).
    * A "MATCH FOUND" message if a substitution occurs.
    * Whether the plan was rewritten ("Rewritten: Yes/No").
    * The final IR plan (original or rewritten).
    * The final SQL generated from the final IR plan.

## Limitations (POC)

This is a simplified POC with several limitations:

* **SQL Support:** Only handles basic `SELECT [cols] FROM [single_table] WHERE [condition]` and `SELECT [cols] FROM [table1] INNER JOIN [table2] ON [condition]` queries. Aggregations (GROUP BY, COUNT, SUM, etc.), ORDER BY, LIMIT, DISTINCT, outer joins, complex subqueries in FROM/WHERE clauses, window functions, etc., are **not** supported by the `TrinoAstToSimpleRelConverter` or the rules.
* **Rule Coverage:** Only includes rules for specific `(Project ->) Filter -> Scan` and `(Project ->) Join` patterns. More complex query/MV patterns would require additional rules.
* **Expression Handling & Equality:**
    * **String-Based Comparison:** `SimpleExpression.equals()` relies on comparing the canonical SQL string generated by `SqlStringUtils.formatSql()`. This means syntactically different but semantically equivalent expressions (e.g., `a > 10` vs `10 < a`, or `s.col = i.col` vs `t.col = j.col` even if `s`/`t` and `i`/`j` refer to the same base tables) will **not** be considered equal.
    * **Impact:** This affects the accuracy of matching join conditions and filter conditions within the rules and `splitFilter`. For the POC to work reliably, query expressions and MV definition expressions often need to be written with identical syntax (including aliases and qualification).
    * **Basic `splitFilter`:** The `splitFilter` logic only handles top-level `AND` expressions with two terms and relies on the brittle `equals` check. It cannot handle complex boolean logic.
    * **Limited `getReferencedColumns`:** The logic to find referenced columns within an expression is very basic.
* **Schema/Type Handling:** Type inference in `TrinoAstToSimpleRelConverter` and `UnifyResult` is minimal ("UNKNOWN_POC"). A real system needs robust type checking and propagation.
* **SQL Generation:** `SimpleRelToSqlConverter` produces pseudo-SQL using derived tables. It doesn't handle identifier quoting correctly and may produce non-optimal or syntactically incorrect SQL for complex cases.
* **Error Handling:** Error handling is basic.
* **No Cost Model:** Rewriting occurs whenever a rule matches, without considering if the rewrite is actually beneficial in terms of performance.