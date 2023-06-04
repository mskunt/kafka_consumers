from kafka import KafkaConsumer
import json
import decimal
import base64
import pandas as pd
import keyboard
import numpy as np
import datetime
import time
from sqlalchemy import text
from sqlalchemy import create_engine
import mysql.connector
from datetime import timedelta
from pymysql.converters import escape_string
import os
import getopt
import sys
import uuid


# bu kısım takip edilen tabloya göre duzenlenecek.
topic_name = ""
target_table_name = ""
group_id = ""
sasl_username = "" #'sasl_username',
sasl_password = "" #'sasl_password'
consumerUID = str(uuid.uuid4())

id_fields = ["id"]

db_con = create_engine("mysql+pymysql://username:password@ip_adress/db_name".format(user="user_name",
                                                                                         pw="password",
                                                                                         db="db_name")
                       )

df_target = pd.DataFrame()
df_data_struct = pd.DataFrame()
boolean_to_int = True #mysql de bool yok

date_symantic_types = ["io.debezium.time.MicroTimestamp",
                       "io.debezium.time.MicroTimestamp",
                       "io.debezium.time.Timestamp",
                       "io.debezium.time.MicroTime",
                       "io.debezium.time.Time",
                       "io.debezium.time.Date"]

#####################################################################################



#####################################################################################

def operation_type(op):
    return {
        'c': 'create',
        'r': 'create',
        'd': 'delete',
        'u': 'update'
    }.get(op, 'undefined')

#####################################################################################

def big_decimal_to_decimal(big_decimal, precision, scale):
    ctx = decimal.Context()
    ctx.prec = precision
    result = ctx.create_decimal(
        int.from_bytes(base64.b64decode(big_decimal), byteorder='big', signed=True)) / 10 ** scale
    return float(result)

#####################################################################################

conn = mysql.connector.connect(user='user_name', password='password', host='ip_adress', database='db_name')
cursor = conn.cursor()
def execute_script(scripts):

    global conn
    global cursor
    if conn is None:
        conn = mysql.connector.connect(user='user_name', password='password', host='ip_adress', database='db_name')
        cursor = conn.cursor()

    for script in scripts:
        try:
            cursor.execute(script)
            conn.commit()
        except Exception as e:
            print("Execute Script Hatası : ", script, "Error: ", e)
            log_info("Error: Execute Script Hatası : ", script, "Error: ", e)
            conn.rollback()
            raise e


def create_data_struct(value):
    struct_json = json.loads(value)
    fields = struct_json.get("schema")["fields"][0].get("fields")
    data = [(f.get("field"),
             f.get("type"),
             f.get("name"),
             f.get('parameters', eval("{'scale': 'None', 'connect.decimal.precision': 'None'}")).get("scale"),
             f.get('parameters', eval("{'scale': 'None', 'connect.decimal.precision': 'None'}")).get(
                 "connect.decimal.precision")
             ) for f in fields]

    df = pd.DataFrame(data, columns=['field', 'type', 'name', 'scale', 'precision'])
    df.index = list(df["field"])
    return df
#####################################################################################


def format_datetime(metric, semantic_type):
    try:
        divider = divider_switch = {
            'io.debezium.time.MicroTimestamp': 1000000,
            'io.debezium.time.Timestamp': 1000,
            'io.debezium.time.MicroTime': 1000000,
            'io.debezium.time.Time': 1000,
            'io.debezium.time.Date': 1/(24*60*60)
        }.get(semantic_type)

        sec = metric / divider

        delta1 = timedelta(seconds=sec)
        posix_time = datetime.datetime.utcfromtimestamp(0)
        time_value = posix_time + delta1

        if semantic_type == "io.debezium.time.Time":
            time_value = time.strftime("%H:%M:%S", time_value)

        return cover_with_quotes(time_value)
    except Exception as e:
        log_info("Error on Date Value : %s", metric)
        return "Error"
#####################################################################################
def escape_strings(text_value):
    text_value = escape_string(text_value)
    return text_value

def cover_with_quotes(text_value):
    text_value = str(text_value)
    return "'" + text_value + "'"
#####################################################################################


def format_data_columns(dict_mes):
    for col in dict_mes:
        if col == "op":
            continue
        data_type = df_data_struct.loc[col, "type"]
        data_sub_type = df_data_struct.loc[col, "name"]
        scale = df_data_struct.loc[col, "scale"]
        precision = df_data_struct.loc[col, "precision"]

        if dict_mes[col] is not None:
            if data_type == "bytes" and data_sub_type == "org.apache.kafka.connect.data.Decimal":  # float bir sayı ve bigdecimal olarak gelmiş.
                dict_mes[col] = big_decimal_to_decimal(dict_mes[col],
                                                       int(precision),
                                                       int(scale))
            if data_type == "struct" and data_sub_type == "io.debezium.data.VariableScaleDecimal":  # numeric tipindekiler
                dict_mes[col] = big_decimal_to_decimal(dict_mes[col].get("value"),
                                                       20, # bu kısım sabit
                                                       int(dict_mes[col].get("scale")))
            if data_type == "string":
                dict_mes[col] = cover_with_quotes(escape_strings(str(dict_mes[col])))
            if data_type == "boolean" and boolean_to_int is True:
                dict_mes[col] = int(dict_mes[col])
            if data_type in ["int32", "int64"] and data_sub_type in date_symantic_types:
                dict_mes[col] = format_datetime(dict_mes[col], data_sub_type)
        else:
            dict_mes[col] = 'null'
    return dict_mes
#####################################################################################


def generate_delete_insert_script(dict_list, dwh_timestamp_column):
    delete_script = generate_delete_script(dict_list)

    batch_insert_value = ""
    batch_insert_col_names = "("

    for col in dict_list[0]:
        if col == "op":
            continue
        batch_insert_col_names = batch_insert_col_names + col + ", "
    batch_insert_col_names = batch_insert_col_names[0:-2] + ","+dwh_timestamp_column+") "

    for dict_mes in dict_list:
        val_insert = "("
        for col in dict_mes:
            if col == "op":
                continue
            val_insert = val_insert + str(dict_mes[col]) + ", "

        val_insert = val_insert[0:-2] + ",current_timestamp)"
        batch_insert_value = batch_insert_value + val_insert + ", "

    batch_insert_value = batch_insert_value[0:-2]

    insert_script = "INSERT INTO {tname} {colname} VALUES {vals};".format(tname=target_table_name,
                             colname=batch_insert_col_names,
                             vals=batch_insert_value)
    return delete_script, insert_script
#####################################################################################


def generate_delete_script(delete_list):
    in_clause = ''
    iteration = 1
    for dict_mes in delete_list:
        for col in dict_mes:
            iteration = iteration + 1
            if col == "op":
                continue
            if col in id_fields:
                in_clause = in_clause + "{value}, ".format(value=dict_mes[col])

    in_clause = id_fields[0] + " IN (" + in_clause[0:-2] + ")"

    delete_text = "DELETE FROM {tname} WHERE {inclause}".format(tname=target_table_name, inclause=in_clause)
    return delete_text
#####################################################################################


def generate_and_execute_script(op_type, data_list):
    for dict_mes in data_list:
        format_data_columns(dict_mes)

    dwh_timestamp_column = ""
    if op_type == "create":
        dwh_timestamp_column = "dwh_insert_time"
    if op_type == "update":
        dwh_timestamp_column = "dwh_update_time"

    if op_type in ["create", "update"]:
        delete_script, insert_script = generate_delete_insert_script(data_list, dwh_timestamp_column)
        execute_script([delete_script, insert_script])
        data_list.clear()
    elif op_type == "delete":
        delete_script = generate_delete_script(data_list)
        execute_script([delete_script])
        data_list.clear()
#####################################################################################


def parse_message(key, value):
    if value is None:
        return 'tombstone'
    #json_key = json.loads(key)
    json_value = json.loads(value)
    op_type = operation_type(json_value.get('payload').get('op'))

    json_table_data = ''
    if op_type in ['create', 'update']:
        json_table_data = json_value.get('payload').get('after')
    elif op_type == 'delete':
        json_table_data = json_value.get('payload').get('before')
    json_table_data["op"] = op_type
    return json_table_data
#####################################################################################


def consume_kafka_topic():
    consumer = KafkaConsumer(bootstrap_servers='kafka_bootstap_server_1_ip:9093,kafka_bootstap_server_2_ip:9093',
                             group_id=group_id,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             max_poll_records=2000,
                             max_partition_fetch_bytes=12428800,
                             security_protocol='SASL_PLAINTEXT',
                             sasl_mechanism='PLAIN',
                             sasl_plain_username=sasl_username,
                             sasl_plain_password=sasl_password

                             # max_poll_interval_ms=120000, #default 5 minutes iki pool arasında gecmesi gereken max süre. bu sureden önce pool yapmazsa consumer dead sayılır
                             # fetch.min_bytes #default 1
                             # max.pool.records #default 500
                             # max.partition.fetch.bytes #default 1MB
                             # fetch.max.bytes #deafult 50mb
                             # session.timeout.ms #default 10second consumer bu surede bir iş yapmaz ise disconnect olmuş sayılır
                             # heartbeat.interval.ms #default 3 seconds bu deger session.timeout.ms degerinin 3 de biri olmalı
                             )
    print("Subscribing the topic")
    consumer.subscribe(topic_name)
    total_mes_count = 0
    print("Subscribed")
    try:
        loop_count = 0
        while True:
            print("loop started")
            msg_pack = consumer.poll(timeout_ms=15, max_records=1)
            print("pooled")
            pool_mes_count = 0
            insert_count, delete_count, update_count = 0, 0, 0
            for tp, messages in msg_pack.items():
                data_list = []
                prev_op = ""
                for msg in messages:
                    pool_mes_count = pool_mes_count + 1
                    total_mes_count = total_mes_count + 1
                    if total_mes_count == 1:
                        global df_data_struct
                        df_data_struct = create_data_struct(msg.value)
                    dict_mes = parse_message(msg.key, msg.value)
                    if dict_mes == 'tombstone':
                        continue

                    #pool içindeki ilk mesaj ise op ataması yap
                    if prev_op == "":
                        prev_op = dict_mes["op"]

                    #farklı tip bir op geldi. önceki birikenleri işleyelim
                    if prev_op != dict_mes["op"]:
                        if prev_op == "create":
                            insert_count = insert_count + len(data_list)
                        elif prev_op == "update":
                            update_count = update_count + len(data_list)
                        else:
                            delete_count = delete_count + len(data_list)
                        generate_and_execute_script(prev_op, data_list)
                        prev_op = dict_mes["op"]

                    #eger peş peşe aynı kayıt update edilmiş ise, iki kez delete ve sonrası 2 kez insert oluşuyor. buda hataya sebep oluyır.
                    #bu yüzden aynı kayıtdan bu pool içinde daha önce geldi ise, son kaydı alacağız.
                    #print(dict_mes)
                    data_list = list(filter(lambda item: item['id'] != dict_mes["id"], data_list))
                    data_list.append(dict_mes)

                #i.erdeki döngü bitti. eğer işlenecek mesaj var ise işleyelim.
                if len(data_list) > 0:
                    if prev_op == "create":
                        insert_count = insert_count + len(data_list)
                    elif prev_op == "update":
                        update_count = update_count + len(data_list)
                    else:
                        delete_count = delete_count + len(data_list)
                    generate_and_execute_script(prev_op, data_list)

            consumer.commit()

            if pool_mes_count > 0:
                info_text = " - TMC:" \
                      + '{: <{}}'.format(str(total_mes_count), 5), " [InPool:" \
                      + '{: <{}}'.format(str(pool_mes_count), 5) \
                      + "] [C:", '{: <{}}'.format(str(insert_count), 4)  \
                      + "] [U:", '{: <{}}'.format(str(update_count), 4)\
                      + "] [D:", '{: <{}}'.format(str(delete_count), 4)
                print(info_text)
                log_info(info_text)
                if loop_count % 20 == 0:
                    print("--------------------------------------\n")
                    print("Topic Name : ", topic_name,
                          "\nGroup ID : ", group_id,
                          "\nTarget Table : ", target_table_name)
                    print("--------------------------------------\n")

            loop_count = loop_count + 1
    except KeyboardInterrupt:
        print("Exiting The Application...")

#####################################################################################


file = ""
def create_log_file():
    global topic_name, file, consumerUID

    path = f'logs/'
    isExist = os.path.exists(path)
    if not isExist:
        os.makedirs(path)

    file_full_path = "logs/" + topic_name + "_" + consumerUID + ".out"
    file = open(file_full_path, mode='a', encoding='utf-8')


def log_info(info_text):
    seconds = time.time()
    log_time = time.ctime(seconds)

    info_text = str(log_time) + " - " + str(info_text)
    file.write(info_text)
    file.write("\n")
    file.flush()



def main(argv):

    global topic_name, target_table_name, group_id, sasl_username, sasl_password
    topic_name, target_table_name, group_id, sasl_username, sasl_password = "", "", "", "", ""
    opts, args = getopt.getopt(argv, "", ["topic=", "table=", "group_id=", "user=", "pass="])

    for opt, arg in opts:
        if opt in "--topic":
            topic_name = arg
        elif opt == "--table":
            target_table_name = arg
        elif opt == "--group_id":
            group_id = arg
        elif opt == "--user":
            sasl_username = arg
        elif opt == "--pass":
            sasl_password = arg
        else:
            print("Missing Parameters. Use --topic , --table, --group_id ,  --user, --pass")
            # return

    create_log_file()

    if topic_name != "" and target_table_name != "" and group_id != "" and sasl_username != "" and sasl_password != "":
        create_log_file()
        info_text = "Consumer For Topic :  ", topic_name,"\nThe Data Insert Into Table : ", target_table_name, "\nThe Group is : ", group_id, \
                    "\n The SASL User is ", sasl_username

        print(info_text)
        log_info("Consumer For Topic :  " + topic_name)
        log_info("The Data Insert Into Table : " + target_table_name)
        log_info("The Group is : " + group_id)
        log_info("The SASL User is " + sasl_username)

    else:
        print("Missing Parameters. Use --topic , --table, --group_id ,  --user, --pass")
        topic_name = "default topic name like tb_replication_heartbeat"
        target_table_name = "default table name like tb_replication_heartbeat"
        group_id = "consumer_dev_test"
        sasl_username = "sasl_username"
        sasl_password = "sasl_password"
        # return

    print("Consumer is Starting")
    log_info("Consumer is Starting")
    while(True):
        try:
            consume_kafka_topic()
        except:
            print("An exception occurred. The loop will continue")

    file.close()

if __name__ == "__main__":
    main(sys.argv[1:])

