# Real-Time-Fitness-Anlysis
Here's a shorter version of the **README.md** file for your project:

```markdown
# Real-Time Fitness Analysis

This project demonstrates real-time fitness data processing using **Apache Kafka** and **Apache Spark**. It ingests fitness metrics like heart rate, steps, and calories, processes them in **real-time** using **streaming** and **batch** modes, and compares the results.

## Technologies Used

- **Apache Kafka** for real-time data ingestion.
- **Apache Spark** for data processing.
- **Python** for scripting.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/real-time-fitness-analysis.git
   cd real-time-fitness-analysis
   ```

2. Set up a **Python virtual environment**:
   ```bash
   python -m venv env
   source env/bin/activate  # On Windows, use `env\Scripts\activate`
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Project

1. **Start Kafka and Spark** (follow their documentation).
2. **Run the Kafka Producer** to simulate fitness data:
   ```bash
   python fitness_producer.py
   ```
3. **Run the Consumer** to process the data:
   ```bash
   python fitness_consumer.py
   ```
4. For batch processing, run:
   ```bash
   python fitness_batch_processor.py
   ```

## Comparison of Streaming & Batch Modes

- **Streaming Mode**: Processes data in real-time.
- **Batch Mode**: Aggregates data periodically.
