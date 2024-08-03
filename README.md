# Weather Data Pipeline

## Project Overview
This project implements a robust weather data pipeline using Apache Kafka, Apache Spark, and Apache Airflow. It collects weather data from multiple Australian cities, processes it in real-time, stores it in a PostgreSQL database, and generates visualizations.

## Technologies Used
- Apache Kafka: For real-time data streaming
- Apache Spark: For distributed data processing
- Apache Airflow: For workflow orchestration
- PostgreSQL: For data storage
- Python: Primary programming language
- Docker & Docker Compose: For containerization and easy deployment

## Project Structure
weather-data-pipeline/
├── dags/
│   └── weather_dag.py
├── scripts/
│   ├── weather_data_producer.py
│   ├── weather_data_processor.py
│   └── weather_visualizations.py
├── lib/
│   └── postgresql-42.7.3.jar
├── weather_visualizations/
│   ├──  weather_visualizations.pdf
│   └──  weather_visualizations.png
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
## Setup and Installation

### Prerequisites
- Docker and Docker Compose installed on your system
- Git for version control

### Steps to Run
1. Clone the repository:
git clone https://github.com/your-username/weather-data-pipeline.git
cd weather-data-pipeline

2. Build and start the services:
docker-compose up --build

3. Access Airflow web interface:
Open a browser and go to `http://localhost:8080`

4. Trigger the DAG in Airflow to start the data pipeline

## Component Details

### Weather Data Producer (`weather_data_producer.py`)
Fetches weather data from an API for Australian cities and publishes it to a Kafka topic.

### Weather Data Processor (`weather_data_processor.py`)
Consumes data from Kafka, processes it using Spark, and stores it in PostgreSQL.

### Weather Visualizations (`weather_visualizations.py`)
Generates visualizations based on the collected weather data.

### Airflow DAG (`weather_dag.py`)
Orchestrates the entire pipeline, scheduling and monitoring each step.

## Data Flow
1. Weather data is collected and sent to Kafka
2. Spark job processes the streaming data
3. Processed data is stored in PostgreSQL
4. Visualizations are generated from the stored data
5. Airflow manages the entire workflow

## Visualizations
The pipeline generates the following visualizations:
- Distribution of weather conditions (Pie Chart)
- Temperature over time for each city (Line Chart)
- Humidity vs Temperature by city (Scatter Plot)
- Wind Speed distribution by city (Box Plot)

## Future Improvements
- Implement real-time alerting for extreme weather conditions
- Add more cities and data sources
- Enhance visualizations with interactive dashboards

## Contributing
Contributions to this project are welcome. Please fork the repository and submit a pull request with your changes.

## License
Copyright (c) 2024, Ye Xiang Chen
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Contact
Adrian Ye Xiang Chen - yexiangchen0311@gmail.com

Project Link: https://github.com/yxc24/weather-data-pipeline#   w e a t h e r - d a t a - p i p e l i n e  
 