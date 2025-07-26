# SQLite Rules-Based Query Optimization

## Query Rewriting Rules
- **Algebraic Transformations**: SQLite applies mathematical rules to rewrite queries into more efficient equivalent forms by moving WHERE clause terms and normalizing the query structure
- **Subquery Flattening**: The optimizer converts eligible subqueries into joins when certain safety conditions are met, eliminating the overhead of nested query execution
- **Constant Folding**: Expressions that can be computed at compile time are pre-calculated and replaced with their literal values
- **Redundant Expression Elimination**: Duplicate calculations and redundant conditions are identified and removed from the query plan

## Index Selection Rules
- **No indexes available - table scans only**: All queries use sequential table scans since no indexes exist

## Join Ordering Rules
- **Cross Product Avoidance**: The optimizer ensures joins have proper connecting conditions to prevent expensive Cartesian products
- **Nested Loop Preference**: SQLite defaults to nested loop join algorithms and maintains the order specified in the FROM clause
- **Constraint-First Processing**: Tables with equality constraints in the WHERE clause are processed before those with only range conditions (purely based on condition type, not selectivity)

## WHERE Clause Optimization Rules
- **Condition Reordering**: WHERE clause terms are automatically reordered based on structural patterns (equality before ranges, constants before variables)
- **Range Condition Optimization**: Multiple range conditions on the same column are combined logically even without indexes
- **Impossible Condition Elimination**: Conditions that are always false (like WHERE 1=0) are detected and the query is short-circuited

## Subquery Processing Rules
- **EXISTS/IN Conversion**: EXISTS and IN subqueries are converted to semi-joins or anti-joins when beneficial
- **Correlated Subquery Flattening**: Correlated subqueries are flattened into regular joins when they don't change query semantics
- **Scalar Subquery Optimization**: Single-value subqueries are optimized to avoid repeated execution when the result is constant
- **Subquery Condition Pushdown**: WHERE conditions are pushed down into subqueries to reduce intermediate result set sizes

## Expression Simplification Rules
- **Boolean Logic Simplification**: Complex boolean expressions are simplified using standard logical equivalence rules (AND, OR, NOT optimization)
- **Constant Expression Pre-computation**: Mathematical and string operations with constant operands are calculated during query compilation
- **Always-True/False Elimination**: Conditions that are always true or false are simplified or removed entirely
- **Type Coercion Optimization**: Implicit type conversions are optimized and unnecessary casts are eliminated

## Aggregate and GROUP BY Rules
- **MIN/MAX Table Scan Optimization**: MIN and MAX queries still require full table scans but can terminate early when finding extreme values during sequential scan
- **GROUP BY Processing**: Groups are formed during table scan without requiring separate sorting step (hash-based grouping)
- **Aggregate Computation**: Aggregate functions are computed incrementally during the table scan process
- **HAVING Clause Pushdown**: HAVING conditions that can be evaluated as WHERE conditions are moved earlier in the execution plan

## LIMIT and ORDER BY Rules
- **Early Termination**: LIMIT clauses enable early termination of table scans once the required number of rows is found
- **In-Memory Sorting**: ORDER BY clauses require explicit sorting operations using in-memory sort algorithms
- **Top-N Optimization**: LIMIT with ORDER BY uses heap-based algorithms for finding top N results without sorting the entire dataset
- **Memory-Efficient Sorting**: Large result sets use external sorting algorithms when they don't fit in memory