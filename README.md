# PySpark Log Analysis

Optimized distributed log processing using PySpark and Hadoop on the HDFS_v1 dataset.

## Overview

This project implements a scalable log processing system using PySpark and Hadoop to analyze HDFS (Hadoop Distributed File System) logs. The system extracts meaningful insights such as error frequencies, component performance metrics, and system utilization patterns while benchmarking different distributed computing approaches to optimize query performance.

## Technologies Used

- **PySpark**: For distributed data processing and analysis
- **Hadoop**: For distributed storage and computing
- **Python**: Core programming language
- **Jupyter Notebooks**: For interactive analysis and visualization
- **NumPy & Pandas**: For data manipulation and analysis
- **Matplotlib/Seaborn**: For data visualization

## Dataset

The project uses the HDFS_v1 dataset from LogHub, which contains logs from the Hadoop Distributed File System. Key attributes include:

- **Timestamp**: Exact date and time of events
- **Log Level**: Severity of events (INFO, WARN, ERROR)
- **Component**: HDFS component generating the log
- **Message**: Detailed description of events
- **Block ID**: Unique identifier for HDFS blocks

## Project Structure

```
├── main.ipynb              # Main analysis notebook with complete pipeline
├── analysis.ipynb          # Additional analysis and exploration
├── viz.ipynb              # Visualization and plotting notebook
├── utils.py               # Utility functions for data processing
├── Pipe.py                # Pipeline classes for DataFrame and RDD implementations
├── research_paper.pdf     # Detailed research paper on the project
└── visualizations/        # Generated plots and charts
    ├── anomaly_sequences.png
    ├── benchmarking_results.png
    ├── component_latency.png
    ├── error_frequencies.png
    └── log_level_distribution.png
```

## Key Features

### 1. Dual Implementation Approach
- **DataFrame API**: High-level, SQL-like operations for ease of use
- **RDD API**: Low-level transformations for fine-grained control
- Performance comparison between both approaches

### 2. Log Processing Pipeline
- **Data Loading**: Efficient loading of large log files
- **Parsing**: Regex-based extraction of log components
- **Event Matching**: Template-based event identification
- **Grouping**: Block-level aggregation with anomaly detection

### 3. Performance Optimization
- **Partitioning Strategies**: Testing different partition counts
- **Repartitioning Analysis**: Optimal data distribution strategies
- **Memory Usage Monitoring**: Real-time memory consumption tracking
- **Execution Time Benchmarking**: Comprehensive performance metrics

### 4. Anomaly Detection
- Integration with labeled anomaly data
- Block-level anomaly classification
- Performance impact analysis of anomalous vs normal blocks

## Analysis Results

The project generates various insights including:

- **Error Frequency Analysis**: Distribution of error types across components
- **Component Performance**: Latency and throughput metrics per HDFS component
- **Temporal Patterns**: Peak activity times and hourly usage patterns
- **Anomaly Patterns**: Characteristics of anomalous block sequences
- **Performance Benchmarks**: DataFrame vs RDD execution comparisons

## Setup and Installation

### Prerequisites
- Python 3.7+
- Apache Spark 3.0+
- Java 8 or 11
- Jupyter Notebook

### Installation Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/pyspark-log-analysis.git
   cd pyspark-log-analysis
   ```

2. **Install required packages**:
   ```bash
   pip install pyspark pandas numpy matplotlib seaborn psutil findspark
   ```

3. **Set up Spark environment**:
   ```bash
   # Download and extract Spark (if not already installed)
   wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
   tar -xzf spark-3.5.1-bin-hadoop3.tgz
   
   # Set environment variables
   export SPARK_HOME=/path/to/spark-3.5.1-bin-hadoop3
   export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
   ```

4. **Download the HDFS_v1 dataset**:
   ```bash
   # The dataset should be placed in the data/ directory
   # Download from: https://github.com/logpai/loghub/tree/master/HDFS#hdfs_v1
   ```

## Usage

### Running the Complete Pipeline

1. **Start Jupyter Notebook**:
   ```bash
   jupyter notebook
   ```

2. **Open and run `main.ipynb`**:
   - This notebook contains the complete pipeline implementation
   - Includes both DataFrame and RDD approaches
   - Generates performance benchmarks and comparisons

3. **Explore Additional Analysis**:
   - `analysis.ipynb`: Extended exploratory data analysis
   - `viz.ipynb`: Advanced visualizations and plotting

### Key Code Components

#### Pipeline Classes
```python
# DataFrame-based implementation
df_pipe = PipeDF(spark=spark)
df_pipe.pipeline()

# RDD-based implementation  
rdd_pipe = PipeRDD(spark=spark)
rdd_pipe.pipeline()
```

#### Performance Testing
```python
# Create performance testers
df_tester = PerformanceTester(df_pipe, spark)
rdd_tester = PerformanceTester(rdd_pipe, spark)

# Run comprehensive benchmarks
df_metrics = df_tester.run_all_tests()
rdd_metrics = rdd_tester.run_all_tests()
```

## Performance Insights

The project reveals several key performance insights:

1. **DataFrame vs RDD**: DataFrames generally outperform RDDs for complex aggregations due to Catalyst optimizer
2. **Optimal Partitioning**: 8-16 partitions provide the best balance for this dataset size
3. **Memory Usage**: RDD implementations use less memory but require more manual optimization
4. **Repartitioning Strategy**: Repartitioning after parsing improves grouping performance significantly

## Visualizations

The project generates comprehensive visualizations including:

- Log level distribution across components
- Error frequency heatmaps
- Component latency comparisons
- Anomaly detection results
- Performance benchmarking charts
- Temporal activity patterns

## Research Paper

For detailed methodology, results, and analysis, please refer to the [research_paper.pdf](research_paper.pdf) included in this repository.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.

## Acknowledgments

- LogHub project for providing the HDFS_v1 dataset
- Apache Spark community for the excellent documentation
- Drexel University DSCI 632: Applied Cloud Computing course

## Contact

Farzan Mirza - [GitHub](https://github.com/farzanmrz)
