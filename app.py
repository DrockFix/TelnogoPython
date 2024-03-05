import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from re import sub
from zoneinfo import ZoneInfo

import configparser as configparser
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import telebot
from telebot import types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

utc = ZoneInfo('UTC')
localtz = ZoneInfo('Asia/Krasnoyarsk')

not_work_sensors = set()
do_work_sensors = set()
work_sensors = set()

config = configparser.ConfigParser()
with open('config.ini', 'r', encoding='utf-8-sig') as f:
    config.read_file(f)

MAX_LINES_PER_MESSAGE = 100
token = config['TG']['token']
title = config['TG']['title']
CHAT_ID = config['TG']['chat_id']
databases = {}

i = 1
while True:
    db_section = f'DB_{i}'

    if db_section not in config:
        break

    databases[i] = {
        'dbname': config[db_section]['dbname'],
        'user': config[db_section]['user'],
        'password': config[db_section]['password'],
        'host': config[db_section]['host'],
        'port': config[db_section]['port']
    }

    i += 1

db_path = "db.sqlite"

# Создаем логгер
logger = logging.getLogger('StatTG')
logger.setLevel(logging.DEBUG)

# Создаем файловый обработчик логов
fh = logging.FileHandler('log_file.log', encoding='utf-8')
fh.setLevel(logging.DEBUG)

# Создаем консольный обработчик логов
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# Создаем форматировщик
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Добавляем обработчики логов в логгер
logger.addHandler(fh)
logger.addHandler(ch)

logger.info("Программа запущена")

bot = telebot.TeleBot(token)
bot.delete_webhook()


def get_all_sensors():
    sensors = []
    for db_num, db in databases.items():
        try:
            conn = psycopg2.connect(
                dbname=db['dbname'],
                user=db['user'],
                password=db['password'],
                host=db['host'],
                port=db['port']
            )
            cursor = conn.cursor()
        except Exception as e:
            raise Exception(f"Ошибка подключения к базе {db_num}")
        try:
            query_params = {
                "name": "SENSOR_INFO",
                "protocol_version": "1.0",
                "mode": "ONE_ROW"
            }

            query = f"SELECT * FROM data_exchange.get_data('{json.dumps(query_params)}')"
            cursor.execute(query)
            sensor_rows = cursor.fetchall()

            for row in sensor_rows:
                rows_data = row[0].get('rows_data')
                if rows_data:
                    for data in rows_data:
                        sensor_dict = {
                            'status': data.get('status'),
                            'name': data.get('name').encode('utf-8'),
                            'projects_name': data.get('projects_name'),
                            'projects_id': data.get('projects_id'),
                            'pvr_last_time': data.get('pvr_last_time'),
                            'adapter_id': data.get('adapter_id')
                            # Add other desired fields here
                        }
                        sensors.append(sensor_dict)

            conn.commit()
            conn.close()
        except Exception:
            raise Exception("Ошибка получения данных.")
    return sensors


def stat_sensors(sensors, project, message=None):
    prev_work_sensors = 0
    prev_not_work_sensors = 0
    prev_do_work_sensors = 0
    prev_time = ""

    not_work_sensors.clear()
    do_work_sensors.clear()
    work_sensors.clear()
    count_work_sensors = 0
    count_not_work_sensors = 0
    count_do_work_sensors = 0

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Read previous values from the database
    try:
        cursor.execute(
            f"SELECT work, nwork, dowork, create_at AS current_datetime FROM data WHERE project_name = ? ORDER BY create_at DESC LIMIT 1",
            (project,)
        )
        result = cursor.fetchone()
        if result:
            prev_work_sensors = result[0]
            prev_not_work_sensors = result[1]
            prev_do_work_sensors = result[2]
            prev_time = result[3]
    except sqlite3.Error as error:
        print("An error occurred:", error)

    if project == title:
        for sens in sensors:
            status = int(sens['status'])
            if status == 0:
                count_not_work_sensors += 1
                not_work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))
            elif status == 1:
                count_work_sensors += 1
                work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))
            elif status == 2:
                count_do_work_sensors += 1
                do_work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))
    else:
        for sens in sensors:
            if str(sens['projects_id']) == project:
                status = int(sens['status'])
                if status == 0:
                    count_not_work_sensors += 1
                    not_work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))
                elif status == 1:
                    count_work_sensors += 1
                    work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))
                elif status == 2:
                    count_do_work_sensors += 1
                    do_work_sensors.add((sens['name'], sens['pvr_last_time'], sens['adapter_id']))

    try:
        cursor.execute("INSERT INTO data (project_name, work, nwork, dowork, create_at) VALUES (?, ?, ?, ?, ?)",
                       (project, count_work_sensors, count_not_work_sensors, count_do_work_sensors, datetime.now()))
        connection.commit()
    except sqlite3.Error as error:
        print("An error occurred:", error)

    if message:
        list_work_button = InlineKeyboardButton("Список ✅", callback_data="list_work_sensors")
        list_not_work_button = InlineKeyboardButton("Список ❌", callback_data="list_not_work_sensors")
        list_do_work_button = InlineKeyboardButton("Список ⚠️", callback_data="list_do_work_sensors")

        # Отправка сообщения с кнопками
        bot.send_message(
            message.chat.id,
            f"{project}\nПоследнее обращение: {prev_time}\n"
            f"✅ Рабочих датчиков: {count_work_sensors} ({'+' + str(count_work_sensors - prev_work_sensors) if count_work_sensors - prev_work_sensors > 0 else count_work_sensors - prev_work_sensors})\n"
            f"❌ Нерабочих датчиков: {count_not_work_sensors} ({'+' + str(count_not_work_sensors - prev_not_work_sensors) if count_not_work_sensors - prev_not_work_sensors > 0 else count_not_work_sensors - prev_not_work_sensors})\n"
            f"⚠️ С предупреждением: {count_do_work_sensors} ({'+' + str(count_do_work_sensors - prev_do_work_sensors) if count_do_work_sensors - prev_do_work_sensors > 0 else count_do_work_sensors - prev_do_work_sensors})\n",
            reply_to_message_id=message.message_id,
            disable_notification=True,
            reply_markup=InlineKeyboardMarkup([[list_work_button, list_not_work_button, list_do_work_button]])
        )

    cursor.close()
    connection.close()


def display_sensor_list(call, sensors):
    if not sensors:
        bot.send_message(call.message.chat.id, "Нет датчиков.")
    else:
        sensors_sorted = sorted(list(sensors))
        match call.data:
            case "list_work_sensors":
                text = "Список рабочих детекторов:\n"
            case "list_not_work_sensors":
                text = "Список нерабочих детекторов:\n"
            case "list_do_work_sensors":
                text = "Список детекторов с предупреждениями:\n"
        current_lines = 0

        for idx, (sensor, pvr_last_time, adapter_id) in enumerate(sensors_sorted, 1):
            sensor_str = sensor.decode('UTF-8')
            utctime = datetime.strptime(pvr_last_time, '%Y-%m-%dT%H:%M:%S.%f%z')
            localtime = utctime.astimezone(localtz)
            lines_needed = (len(sensor_str) + len(pvr_last_time) + 6) // 50 + 1  # Additional lines for formatting

            if current_lines + lines_needed <= MAX_LINES_PER_MESSAGE:
                text += f"{idx}. {sensor_str}\nPVR: {localtime.strftime('%Y-%m-%d %H:%M')}\n"
                current_lines += lines_needed
            else:
                bot.send_message(call.message.chat.id, text)
                text = f"{idx}. {sensor_str}\nPVR: {localtime.strftime('%Y-%m-%d %H:%M')}\n"
                current_lines = lines_needed

        if text.strip():  # If there's remaining content in 'text', send the last message.
            bot.send_message(call.message.chat.id, text)
            # bot.send_message(call.message.chat.id, "Для выбора датчика введите его номер из списка выше.")
            # bot.register_next_step_handler(call.message, lambda message: ssh_detect(message, sensors_sorted))
@bot.message_handler(commands=['start'])
def start(message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(types.KeyboardButton('🔢 Данные'), types.KeyboardButton('📊 График'))
    bot.send_message(message.chat.id,
             'Данные - получить информацию о датчиках на данный момент времени\n'
             'График - график изменения статусов за сутки датчиков', reply_markup=keyboard)

def clean_text(text):
    cleaned_text = sub(r'[^\w\s]', '', text)  # Удаляем все символы кроме букв, цифр, пробелов и подчеркивания
    cleaned_text = sub(r'\s+', '_', cleaned_text)  # Заменяем пробелы на подчеркивание
    return cleaned_text.lower()[:24]

@bot.message_handler(func=lambda message: True)
def handle_message(message):
    if message.text == '🔢 Данные':
        try:
            sensors = get_all_sensors()
            stat_sensors(sensors, title, message)
        except Exception as e:
            bot.send_message(message.chat.id, f"Не удалось подключиться к базе данных: {title}")
            logger.error(f"Не удалось получить данные {title}: {e}")
    if message.text == '📊 График':
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()
        try:
            now = datetime.now()
            start_time = now - timedelta(days=1)
            cursor.execute(
                f"SELECT work, nwork, dowork, create_at AS current_datetime FROM data WHERE project_name =? AND create_at >=?",
                (title, start_time))
            data = cursor.fetchall()

            # Создание списка со значениями work, nwork и dowork
            work_values = [row[0] for row in data]
            nwork_values = [row[1] for row in data]
            dowork_values = [row[2] for row in data]

            # Convert the 'create_at' field from string to datetime objects
            timestamps = [datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S.%f') for row in data]

            # Создание графика
            fig, ax = plt.subplots(figsize=(10, 5))

            # Добавление линии для work
            ax.plot(timestamps, work_values, color='green', label='Рабочие датчики')

            # Добавление линии для nwork
            ax.plot(timestamps, nwork_values, color='red', label='Нерабочие датчики')

            # Добавление линии для dowork
            ax.plot(timestamps, dowork_values, color='yellow', label='С предупреждением')

            # Добавление легенды
            ax.legend()

            # Настройка осей
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax.set_xticks(pd.date_range(start=start_time, end=now, freq='H'))
            ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
            ax.grid(True)
            ax.set_xlabel('Время')
            ax.set_ylabel('Датчики')
            ax.set_title('График работы датчиков от времени')

            ax.yaxis.set_major_locator(plt.MaxNLocator(20))

            # Сохранение графика в виде изображения
            plt.savefig(f'graph{message.id}.png', dpi=300, bbox_inches='tight')
        except sqlite3.Error as error:
            print("An error occurred:", error)
        with open(f'graph{message.id}.png', 'rb') as photo:
            bot.send_photo(message.chat.id, photo, timeout=100)

        # Удаление временного файла с изображением
        cursor.close()
        connection.close()
        os.remove(f'graph{message.id}.png')

@bot.callback_query_handler(func=lambda call: call.data == "list_work_sensors")
def list_work_sensors_callback(call):
    display_sensor_list(call, work_sensors)

@bot.callback_query_handler(func=lambda call: call.data == "list_not_work_sensors")
def list_not_work_sensors_callback(call):
    display_sensor_list(call, not_work_sensors)

@bot.callback_query_handler(func=lambda call: call.data == "list_do_work_sensors")
def list_do_work_sensors_callback(call):
    display_sensor_list(call, do_work_sensors)

def execute_commands():
    while True:
        try:
            # Ваши команды для выполнения каждый час
            sensors = get_all_sensors()
            stat_sensors(sensors, title)
        except Exception as e:
            logger.error(f"Не удалось получить данные {title}: {e}")

        # Пауза в течение одного часа
        time.sleep(3600)  # 3600 секунд = 1 час

try:
    # Создание и запуск отдельного потока для выполнения команд каждый час
    thread = threading.Thread(target=execute_commands)
    thread.start()

    bot.polling(none_stop=True)
except Exception as e:
    logger.error(f"Ошибка: {e}")
    time.sleep(5)
