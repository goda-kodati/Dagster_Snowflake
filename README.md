# Dagster + Snowflake Data Pipeline Project

This project was created as part of my learning journey to explore **Dagster**, a modern data orchestration tool, and build a pipeline that interacts with **Snowflake**, a cloud-based data warehouse.

The pipeline is designed to demonstrate asset-based orchestration in Dagster, while managing configurations and logs effectively. It can be run locally and serves as a starting point for more advanced data workflows.

---

## 📌 Project Overview

- **Orchestration:** Built using [Dagster](https://dagster.io/)
- **Data Warehouse:** Integrated with [Snowflake](https://snowflake.com)
- **Structure:** Modular Python code with separate files for assets, config, and logs
- **Goal:** Create a simple, local pipeline that writes and logs data operations using Dagster assets

---

## 📁 Project Structure

```
Dagster_Snowflake/
├── integer_squawk_id_project/
│   ├── assets.py           # Dagster assets are defined here
│   ├── config.py           # Snowflake and environment configuration
│   ├── dagster.txt         # Logs from Dagster runs
│   └── custom_logs.txt     # Additional custom logs
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

---

## 🛠️ Getting Started

Follow these steps to set up the project and run the Dagster + Snowflake integration:

### 1. Create the project folder

```bash
mkdir Dagster_Snowflake
cd Dagster_Snowflake
```

### 2. Create and activate a virtual environment

```bash
virtualenv venv
venv\Scripts\activate    # For Windows
# Or use: source venv/bin/activate  # For macOS/Linux
```

### 3. Install Dagster and initialize the project

```bash
pip install dagster
dagster project scaffold --name integer-squawk-id-project
```

### 4. Install required dependencies

```bash
pip install dagit==1.1.15
pip install dagster-snowflake==0.17.15
```

### 5. Write your logic

Modify the default `assets.py` file located at:

```
integer-squawk-id-project/integer_squawk_id_project/assets.py
```

### 6. Run the Dagster UI

```bash
dagit -f integer-squawk-id-project/integer_squawk_id_project/assets.py
```

### 7. Generate `requirements.txt`

```bash
pip freeze > requirements.txt
```

---

## 🔐 Configuration

Make sure your Snowflake credentials are properly set up in the `config.py` file.  
Avoid hardcoding sensitive credentials directly — use environment variables if possible.

---

## 📄 Logs

- `dagster.txt`: Output from Dagster job executions
- `custom_logs.txt`: Additional logging for debugging or checkpoints

---

## 🧪 Requirements

- Python 3.8 or later
- Dagster
- Dagit
- Dagster-Snowflake
- Snowflake Connector for Python

---

## 🎯 Purpose

This project was developed as part of a self-learning initiative to understand how Dagster works and how to integrate it with Snowflake. It can serve as a basic reference for building more complex pipelines.

---

## 📃 License

This project is licensed under the MIT License.
