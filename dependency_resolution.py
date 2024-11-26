from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, collect_list

# Step 1: Create a Spark session
spark = SparkSession.builder.appName("NestedExecutionOrder").getOrCreate()

# Step 2: Define the input data
data = [
    ("pd_techm", "phase1", "rd_acc", "abc1"),
    ("pd_techm", "phase1", "rd_acc", "abc2"),
    ("pd_techm", "phase1", "rd_acc", "abc3"),
    ("pd_techm", "phase2", "pd_cts", "test1"),
    ("pd_cts", "test", "rd_bbc", "ad10"),
    ("pd_cts", "test", "rd_bbc", "ad11"),
    ("pd_cts", "test", "rd_bbc", "ad12"),
    ("pd_cts", "test1", "rd_ccb", "abc1"),
    ("pd_cts", "test1", "rd_ccb", "abc2"),
    ("pd_cts", "test1", "rd_ccb", "abc3"),
    ("pd_cts", "test1", "pd_cts", "test1")  # Example of a self-cycle
]

columns = ["db", "tbl", "srcdb", "srctbl"]
df = spark.createDataFrame(data, columns)

# Step 3: Create unique identifiers for tables and their dependencies
df = df.withColumn("table", concat_ws(".", col("db"), col("tbl"))) \
       .withColumn("source_table", concat_ws(".", col("srcdb"), col("srctbl")))

# Step 4: Build a dependencies dictionary
dependencies_df = df.select("table", "source_table").distinct()
dependencies = dependencies_df.groupBy("table") \
    .agg(collect_list("source_table").alias("dependencies")) \
    .collect()

# Convert to dictionary for easy processing
dependencies_dict = {row["table"]: sorted(row["dependencies"]) for row in dependencies}

# Step 5: Function to get execution order and handle cycles
def get_execution_order_with_cycles(dependencies):
    visited = set()  # Tracks all tables that are fully processed
    stack = set()    # Tracks the current path in recursion to detect cycles
    execution_order = []  # Final order of execution
    one_time_load_required = []  # Tables that are marked as one-time load due to cycles

    def visit(table):
        if table in stack:
            # If a cycle is detected, add it to the one-time load list and skip further processing
            if table not in one_time_load_required:
                one_time_load_required.append(table)
            return
        if table not in visited:
            stack.add(table)  # Add the table to the current recursion stack
            # Recursively visit dependencies first
            for dependency in sorted(dependencies.get(table, [])):
                visit(dependency)
            stack.remove(table)  # Remove from the stack after processing
            visited.add(table)   # Mark the table as fully processed
            execution_order.append(table)  # Add the table to the execution order

    # Visit all tables in the dependency graph
    for table in sorted(dependencies):  # Process in sorted order for consistent results
        if table not in visited:
            visit(table)

    return execution_order, one_time_load_required

# Step 6: Generate execution order
try:
    execution_order, one_time_load_required = get_execution_order_with_cycles(dependencies_dict)
    print("Execution Order:")
    for table in execution_order:
        print(table)
    if one_time_load_required:
        print("\nOne-Time Load Required Tables (Cycles Detected):")
        for table in one_time_load_required:
            print(table)
except Exception as e:
    print(str(e))
