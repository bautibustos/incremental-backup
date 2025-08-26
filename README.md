# Incremental and Full Backup System

This is a Python-based system for performing scheduled incremental and full backups. It uses Celery to manage backup tasks asynchronously.

## Features

-   Performs both full and incremental backups.
-   Schedules backups to run at a specific time.
-   Backup type (full or incremental) is determined by the day of the week (weekdays for incremental, weekends for full).
-   Configuration is managed via a `config.json` file.
-   Uses Celery for asynchronous task execution, making the system robust and scalable.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install the dependencies:**
    Make sure you have Python and pip installed. Then, run the following command to install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Install and run a message broker:**
    This system uses Celery, which requires a message broker. We recommend Redis. If you don't have Redis, you can install it using your system's package manager (e.g., `sudo apt-get install redis-server` on Debian/Ubuntu) or by following the instructions on the [official Redis website](https://redis.io/topics/installation).

    Once installed, make sure the Redis server is running.

## Configuration

The behavior of the backup system is controlled by the `config.json` file:

-   `programacion`:
    -   `hora_backup`: The time (in HH:MM format) to run the backup.
    -   `intervalo_verificacion_segundos`: How often (in seconds) the scheduler checks if it's time to run the backup.
    -   `modo_prueba`: If `true`, the system will run both an incremental and a full backup immediately and then exit. This is useful for testing your configuration.

-   `origenes`: A list of sources to back up. Each source is an object with:
    -   `origen_ruta`: The path to the directory to be backed up.
    -   `destino_ruta`: The directory where the backup zip file will be saved.
    -   `nombre_base_zip`: A base name for the backup file.
    -   `tipo_backup` (optional): An override to force a specific backup type. For example:
        `"tipo_backup": {"completo": true, "incremental": false}`

## Usage

To run the backup system, you need to start two processes: the Celery worker and the scheduler.

1.  **Start the Celery worker:**
    Open a terminal and run the following command from the project's root directory. This worker will wait for and execute backup tasks.
    ```bash
    celery -A tasks worker --loglevel=info
    ```

2.  **Start the scheduler:**
    In a separate terminal, run the main script. This will start the scheduler, which will dispatch tasks at the configured time.
    ```bash
    python main.py
    ```

The scheduler will then run in the background, and you will see log messages in the console and in the `logs/` directory. The Celery worker will execute the backup tasks as they are dispatched.
