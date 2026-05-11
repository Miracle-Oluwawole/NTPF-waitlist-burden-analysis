## NTPF Regional Waitlist Burden Analysis
![Python](https://img.shields.io/badge/Python-3.10+-blue?style=flat-square&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?style=flat-square&logo=apachespark&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-NoSQL-green?style=flat-square&logo=mongodb&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Blob%20Storage-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-3.0.2-150458?style=flat-square&logo=pandas&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-ML-F7931E?style=flat-square&logo=scikitlearn&logoColor=white)

Report Links:  https://docs.google.com/presentation/d/1t83Qs1jtGi7neD8-j7xWeXCypqQpYutN/edit?usp=drive_link&ouid=118103977541279081890&rtpof=true&sd=true
https://docs.google.com/document/d/1pKsYYavglqk1Mvec84apBUPUA-Ok48tc/edit?usp=drive_link&ouid=118103977541279081890&rtpof=true&sd=true

### 1. PREREQUISITES
Before running the pipeline, ensure the following are installed on your Ubuntu system:

- Python: Version 3.11  
- Java: OpenJDK 17 (Required for PySpark)  
- Spark: Version 3.5.1  
- Storage and Database: Access to the Azure blob storage and MongoDB instance defined in the environment variables.

### 2. ENVIRONMENT SETUP

- Navigate to the root directory of project folder.  
- Create and activate the virtual environment.
- Install required python library in the requirement.txt
- Ensure the .env file is present in the root directory with valid credentials for Azure Storage and MongoDB

### 3. DATA REQUIREMENTS
The script expects the following CSV files in the root directory:

- OpenData_IPDCNational01_.csv (2023-2026)  
- OpenData_OPNational01_.csv (2023-2026)
- The script will automatically fetch the PEC26 regional projection data via the CSO PxStat API during execution.

### 4. EXECUTION INSTRUCTIONS
- The shell script 'run_analysis.sh' contains code that runs the 'application_pipeline' python script
- Ensure the application_pipeline.py is present in the root directory
- To execute the full pipeline, run command: ./run_analysis.sh
