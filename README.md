# 📊 End-to-End ETL Employees WorkTracker Project

This project is an **end-to-end data engineering pipeline** designed to extract, transform, and load (ETL) employee work tracking data using **Apache Spark** and store it efficiently in **Delta Lake** format on **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

It simulates a real-world enterprise use case for tracking employees' daily work logs and provides a scalable, fault-tolerant, and cloud-ready data processing architecture.

---

## 🔧 What This Project Does

- ✅ Extracts raw employee work log data in JSON format
- ✅ Transforms and cleans the data using PySpark
- ✅ Loads the processed data into Delta Lake on ADLS Gen2
- ✅ Supports batch and streaming modes (`once=True`)
- ✅ Then use Databricks Jobs & Pipeline to automate shedule workflows
---

## 💡 Technologies Used

| Tech                  | Purpose                                      |
|-----------------------|----------------------------------------------|
| Apache Spark (PySpark)| Distributed data processing engine           | 
| Delta Lake            | Reliable data storage with ACID transactions |
| Azure Data Lake Gen2  | Cloud storage for raw and processed data     |
| Jupyter Notebooks     | Used for development and ETL steps           |   |  

## 👨‍💻 About the Author

Yeasir Arafat Shohan
Data/ML Enthusiast