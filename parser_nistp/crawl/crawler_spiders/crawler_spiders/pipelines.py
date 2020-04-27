# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import JsonItemExporter
import mysql.connector
from mysql.connector import Error
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

class CrawlerParserItem(object):

    def process_item(self, item, spider):

        for f in item.fields:
            if item == 'lot':
                item.setdefault(f, list())
            else:
                item.setdefault(f, None)
        return item

class CrawlerDbConnect(object):
    
    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = mysql.connector.connect(
                    host =                   
                    user = '',
                    passwd = '',
                    database = '',
                
                )
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS lots_nistp(
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        data_origin text,
                        trading_id bigint(20),
                        trading_link text,
                        trading_number varchar(255),
                        trading_type text,
                        trading_form text,
                        trading_org text,
                        trading_org_inn text(12),
                        trading_org_contacts text,
                        msg_number bigint,
                        case_number varchar(255),
                        debtor_inn text(12),
                        arbit_manager text,
                        arbit_manager_inn text(12),
                        arbit_manager_org text,
                        status varchar(255),
                        lot_id bigint,
                        lot_link text,
                        lot_number smallint,
                        short_name mediumtext,
                        lot_info mediumtext,
                        property_information mediumtext,
                        start_date_requests timestamp,
                        end_date_requests timestamp,
                        start_date_trading timestamp,
                        end_date_trading timestamp,
                        start_price double,
                        step_price double,
                        periods mediumtext,
                        files mediumtext,
                        created_at timestamp,
                        CONSTRAINT UC_lots_nistp UNIQUE (lot_id)
                         
        
                                                )
                            
                """)
    
    def store_db(self, item):
        self.curr.execute("""insert into lots_nistp(data_origin,trading_id,trading_link,trading_number,trading_type,trading_form,trading_org,trading_org_inn,trading_org_contacts,msg_number,case_number,debtor_inn,arbit_manager,arbit_manager_inn,arbit_manager_org,status,lot_id,lot_link,lot_number,short_name,lot_info,property_information,start_date_requests,end_date_requests,start_date_trading,end_date_trading,start_price,step_price,periods,files,created_at) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE lot_id=VALUES(lot_id)""", 
                (
            item['data_origin'],
            item['trading_id'],
            item['trading_link'],
            item['trading_number'],
            item['trading_type'],
            item['trading_form'],
            item['trading_org'],
            item['trading_org_inn'],
            item['trading_org_contacts'],
            item['msg_number'],
            item['case_number'],
            item['debtor_inn'],
            item['arbit_manager'],
            item['arbit_manager_inn'],
            item['arbit_manager_org'],
            item['status'],
            item['lot_id'],
            item['lot_link'],
            item['lot_number'],
            item['short_name'],
            item['lot_info'],
            item['property_information'],
            item['start_date_requests'],
            item['end_date_requests'],
            item['start_date_trading'],
            item['end_date_trading'],
            item['start_price'],
            item['step_price'],
            item['periods'],
            item['files'],
            item['created_at']
            
       ))

        self.conn.commit()

    def process_item(self, item, spider):
        try:
            self.store_db(item)
            return item
        except Error as e:
            return e




#class JsonPipline(object):
#
#   def __init__(self):
#       self.file = open('lot.json', 'wb')
#       self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False, indent=4)
#       self.exporter.start_exporting()
#
#   def close_spider(self, spider):
#        self.exporter.finish_exporting()
#        self.file.close()
#
#   def process_item(self, item, spider):
#       self.exporter.export_item(item)
#       return item
