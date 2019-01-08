import json

import pymysql

from common import parse_config

config = parse_config()


class MysqlOutput():
    def __init__(self):
        mysql_host = config['outputs']['mysql_output']['mysql_host']
        mysql_port = config['outputs']['mysql_output']['mysql_port']
        mysql_user = config['outputs']['mysql_output']['mysql_user']
        mysql_pass = config['outputs']['mysql_output']['mysql_pass']
        mysql_db = config['outputs']['mysql_output']['mysql_db']
        self.conn = pymysql.Connect(host=mysql_host, db=mysql_db, passwd=mysql_pass, port=mysql_port, user=mysql_user)

    def create_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "create table IF NOT EXISTS leaks(id int primary key ,pasteid int,yaraRule varchar(255),scrape_url varchar(255),paste_site varchar(255), raw_data varchar(255), found_time timestamp default CURRENT_TIMESTAMP)")
            self.conn.commit()

    def store_paste(self, paste_data):
        with self.conn.cursor() as cursor:
            cursor.execute(
                "insert into leaks (pasteid,yaraRule,scrape_url,paste_site,raw_data,found_time ) values(%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                (paste_data['pasteid'], paste_data['YaraRule'], paste_data['scrape_url'], paste_data['pastesite'],
                 json.dumps(paste_data)))
            self.conn.commit()
