1. PREREQUISITES
Before running the pipeline, ensure the following are installed on your Ubuntu system:

- Python: Version 3.11  
- Java: OpenJDK 17 (Required for PySpark)  
- Spark: Version 3.5.1  
- Storage and Database: Access to the Azure blob storage and MongoDB instance defined in the environment variables.

2. ENVIRONMENT SETUP

- Navigate to the root directory of project folder.  
- Create and activate the virtual environment.
- Install required python library in the requirement.txt
- Ensure the .env file is present in the root directory with valid credentials for Azure Storage and MongoDB

3. DATA REQUIREMENTS
The script expects the following CSV files in the root directory:

- OpenData_IPDCNational01_.csv (2023-2026)  
- OpenData_OPNational01_.csv (2023-2026)
- The script will automatically fetch the PEC26 regional projection data via the CSO PxStat API during execution.

4. EXECUTION INSTRUCTIONS
- The shell script 'run_analysis.sh' contains code that runs the 'application_pipeline' python script
- Ensure the application_pipeline.py is present in the root directory
- To execute the full pipeline, run command: ./run_analysis.sh