from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
import psycopg2
from urllib.parse import urlparse
import logging

# Configure logging
logger = logging.getLogger(__name__)


AIVEN_POSTGRES_URI = "postgres://avnadmin:AVNS_awyu_iZtPBe8AJq6pjF@pg-209e851f-velyvineayieta-0621.l.aivencloud.com:18498/defaultdb?sslmode=require"
OPENWEATHER_API_KEY = "6997cea907e57f9646887e4baf618c99"

def fetch_weather_data(**kwargs):
    try:
        city = kwargs['params'].get('city', 'London')
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
        
        logger.info(f"Fetching weather data for {city}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        weather_data = response.json()
        
        if not all(key in weather_data for key in ['name', 'main', 'weather']):
            raise ValueError("Invalid API response structure")
            
        return {
            'city': weather_data['name'],
            'temp': weather_data['main']['temp'],
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'conditions': weather_data['weather'][0]['main'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Weather API error: {str(e)}")
        raise

def store_weather_data(**kwargs):
    conn = None
    try:
        ti = kwargs['ti']
        weather_data = ti.xcom_pull(task_ids='fetch_weather_data')
        
        if not weather_data:
            raise ValueError("No weather data received")
        
        parsed_uri = urlparse(AIVEN_POSTGRES_URI)
        
        conn = psycopg2.connect(
            host=parsed_uri.hostname,
            port=parsed_uri.port,
            database=parsed_uri.path[1:],
            user=parsed_uri.username,
            password=parsed_uri.password,
            sslmode='require',
            connect_timeout=10
        )
        
        with conn:
            with conn.cursor() as cursor:
                # Create optimized table structure
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                        id SERIAL PRIMARY KEY,
                        city VARCHAR(100) NOT NULL,
                        temperature FLOAT NOT NULL,
                        humidity FLOAT NOT NULL,
                        pressure FLOAT NOT NULL,
                        conditions VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT unique_reading UNIQUE (city, timestamp)
                    )
                """)
                
                cursor.execute("""
                    INSERT INTO weather_data 
                    (city, temperature, humidity, pressure, conditions, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city, timestamp) DO NOTHING
                """, (
                    weather_data['city'],
                    weather_data['temp'],
                    weather_data['humidity'],
                    weather_data['pressure'],
                    weather_data['conditions'],
                    weather_data['timestamp']
                ))
                
        logger.info(f"Data stored for {weather_data['city']} at {weather_data['timestamp']}")
        
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

# DAG configuration
default_args = {
    'owner': 'vellie',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'weather_pipeline_production',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'production']
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
        params={'city': 'London'},
        provide_context=True
    )
    
    store_task = PythonOperator(
        task_id='store_weather_data',
        python_callable=store_weather_data,
        provide_context=True
    )
    
    fetch_task >> store_task