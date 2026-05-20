#!/bin/bash
set -e

# تعيين متغيرات البيئة الافتراضية إذا لم تكن موجودة
export AIRFLOW_HOME=${AIRFLOW_HOME:-/opt/airflow}
export AIRFLOW__CORE__LOAD_EXAMPLES=${AIRFLOW__CORE__LOAD_EXAMPLES:-False}
export AIRFLOW__CORE__EXECUTOR=${EXECUTOR:-SequentialExecutor}

echo "=========================================="
echo "Airflow Entrypoint Script Starting..."
echo "=========================================="

# تثبيت المتطلبات إذا وجدت
if [ -e "/opt/airflow/requirements.txt" ]; then
    echo "Installing requirements from requirements.txt..."
    $(command python) pip install --upgrade pip
    $(command -v pip) install --user -r /opt/airflow/requirements.txt
    echo "Requirements installed successfully!"
fi

# تهيئة قاعدة البيانات وإنشاء المستخدم إذا لم تكن موجودة
if [ ! -f "/opt/airflow/airflow.db" ]; then
    echo "Initializing Airflow database..."
    airflow db init

    echo "Creating admin user..."
    airflow users create \
        --username ${AIRFLOW_ADMIN_USERNAME:-admin} \
        --firstname ${AIRFLOW_ADMIN_FIRSTNAME:-Admin} \
        --lastname ${AIRFLOW_ADMIN_LASTNAME:-User} \
        --role Admin \
        --email ${AIRFLOW_ADMIN_EMAIL:-admin@example.com} \
        --password ${AIRFLOW_ADMIN_PASSWORD:-admin}

    echo "Admin user created successfully!"
else
    echo "Database already exists, skipping initialization..."
fi

# ترقية قاعدة البيانات
echo "Upgrading Airflow database..."
airflow db upgrade

# انتظار توفر خدمات أخرى (اختياري)
wait_for_services() {
    echo "Waiting for Kafka broker to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if nc -z broker 29092 2>/dev/null; then
            echo "Kafka broker is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: Kafka broker not ready yet..."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "Warning: Kafka broker not available after $max_attempts attempts"
    return 1
}

# انتظار توفر الخدمات (يمكن تفعيلها حسب الحاجة)
# wait_for_services

# عرض معلومات الاتصال
echo "=========================================="
echo "Airflow Configuration:"
echo "- Database: ${AIRFLOW__DATABASE__SQL_ALCHEMY_CONN:-sqlite:////opt/airflow/airflow.db}"
echo "- Executor: ${AIRFLOW__CORE__EXECUTOR}"
echo "- Kafka Broker: broker:29092"
echo "=========================================="

# تشغيل Airflow webserver
echo "Starting Airflow webserver..."
exec airflow webserver