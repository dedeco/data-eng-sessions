# Eligibility Calculation with Spark

## Installation

1. **Clone the repository:**
   ```bash
   git clone [invalid URL removed]
   cd YOUR_REPO_NAME
   ```

2. **Set up your Spark environment:**
   * Ensure you have Spark installed and configured. You can download it from the official Apache Spark website ([https://spark.apache.org/](https://spark.apache.org/)).
   * Set the `SPARK_HOME` environment variable to your Spark installation directory.

3. **Create a virtual environment (recommended):**
   ```bash
   python -m venv ,venv 
   source .venv/bin/activate   # On Windows: .\venv\Scripts\activate
   ```

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Define Eligibility Rules**
   * Open the `eligibility/rules.py` file.
   * Implement your eligibility criteria within the functions provided, making use of Spark's data structures (e.g., DataFrames, SQL expressions) to perform calculations efficiently across your distributed data.

2. **Calculate Eligibility with Spark**
   * Open the `calculate_eligibility.ipynb` notebook.
   * Follow the instructions in the notebook to:
      * Connect to your Spark cluster.
      * Load your data (e.g., from a distributed file system, database, or cloud storage).
      * Apply the eligibility rules to your Spark DataFrame.
      * Calculate eligibility metrics or flags.
      * Store the results (e.g., write to a parquet file or database).

3. **Explore and Extend**
   * Use the `templates/data-preparation.ipynb` notebook as a template for common Spark data preparation tasks (filtering, aggregation, etc.).
   * Create additional Spark notebooks to perform more in-depth analysis or visualizations of your eligibility data.

## Testing

Run the unit tests to ensure the eligibility rules are working correctly in a Spark environment:

```bash
# Run unit tests that can be executed outside a Spark context
pytest 
```

