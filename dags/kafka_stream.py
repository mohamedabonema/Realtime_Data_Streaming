import json
import logging
from datetime import datetime
import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 19, 10, 00),
    'retries': 3,
    'retry_delay': 5
}


def get_data():
    """جلب البيانات من API"""
    try:
        res = requests.get("https://randomuser.me/api/", timeout=10)

        if res.status_code != 200:
            logging.error(f"API request failed with status {res.status_code}")
            return None

        data = res.json()
        results = data.get('results')

        if not results:
            logging.error("No results found in API response")
            return None

        return results[0]

    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        return None


def format_data(res):
    """تنسيق البيانات المستلمة من API"""
    data = {}
    location = res['location']
    data['id'] = res['login']['uuid']  # إضافة UUID فريد
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = str(location['postcode'])  # تحويل إلى string
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def create_kafka_producer():
    """إنشاء منتج Kafka مع إعادة محاولة الاتصال"""
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=['broker:29092'],
                max_block_ms=5000,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3
            )
            # اختبار الاتصال
            producer.send('test_topic', {'test': 'connection'})
            producer.flush(timeout=5)
            logging.info("Successfully connected to Kafka broker")
            return producer

        except NoBrokersAvailable as e:
            logging.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka broker not available yet. Error: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error("Could not connect to Kafka after all retries")
                raise
        except Exception as e:
            logging.error(f"Unexpected error creating Kafka producer: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

    return None


def stream_data(**context):
    """دفق البيانات إلى Kafka"""
    producer = None

    try:
        # إنشاء منتج Kafka
        producer = create_kafka_producer()

        if not producer:
            raise Exception("Failed to create Kafka producer")

        # دفق البيانات لمدة 60 ثانية
        start_time = time.time()
        messages_sent = 0
        errors_count = 0

        logging.info("Starting data streaming...")

        while time.time() < start_time + 60:
            try:
                # جلب البيانات
                raw_data = get_data()

                if raw_data is None:
                    logging.warning("Skipping empty API response")
                    time.sleep(2)
                    continue

                # تنسيق البيانات
                formatted_data = format_data(raw_data)

                # إرسال إلى Kafka
                future = producer.send('users_created', value=formatted_data)
                record_metadata = future.get(timeout=10)

                messages_sent += 1
                logging.info(f"Message {messages_sent} sent to Kafka - Offset: {record_metadata.offset}, "
                             f"Partition: {record_metadata.partition}, "
                             f"Data: {formatted_data['first_name']} {formatted_data['last_name']}")

                # انتظار ثانية بين كل رسالة
                time.sleep(1)

            except Exception as e:
                errors_count += 1
                logging.error(f"Error processing message {messages_sent + 1}: {e}", exc_info=True)
                time.sleep(2)

                # إذا زادت الأخطاء عن الحد، أوقف الدفق
                if errors_count > 10:
                    logging.error("Too many errors, stopping stream")
                    break

        # إرسال إحصائيات
        logging.info(f"Streaming completed - Total messages: {messages_sent}, Total errors: {errors_count}")

        # التأكد من إرسال جميع الرسائل
        producer.flush()

        # تحديث حالة المهمة في XCom (اختياري)
        context['task_instance'].xcom_push(key='messages_sent', value=messages_sent)
        context['task_instance'].xcom_push(key='errors_count', value=errors_count)

    except Exception as e:
        logging.error(f"Fatal error in stream_data: {e}", exc_info=True)
        raise

    finally:
        if producer:
            producer.close()
            logging.info("Kafka producer closed")


# تعريف DAG
with DAG(
        dag_id='realtime_stream_api_to_kafka',
        default_args=default_args,
        schedule="@daily",  # يمكن تغييرها إلى '@hourly' أو '*/15 * * * *'
        catchup=False,
        description='Stream realtime user data from RandomUser API to Kafka',
        tags=['kafka', 'streaming', 'api']
) as dag:
    stream_task = PythonOperator(
        task_id='stream_from_api_to_kafka',
        python_callable=stream_data,
        provide_context=True,
        retries=2,
        retry_delay=30
    )

    stream_task