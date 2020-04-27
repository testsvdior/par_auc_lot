# -*- coding: utf-8 -*-

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Compose, Identity, ChainMap
import re
import os

def filter_value(value):
    if value:
        return value
    else:
        return '0'

def check_trading_type(code: str = None):
    """
    Check what type of trade
    :param code:str
    :return:
    """
    pattern = r'(\D{4}$)'
    match = re.findall(pattern, code)
    if 'ОА' in str(match[0:2]):
        return 'auction'
    elif 'ПП' in str(match[-1:2]):
        return 'offer'
    elif 'ОК' or 'ЗК' in str(match):
        return 'competition'
    else:
        return None

def check_trading_form(form: str = None):
    """
    Check what form
    :param form:str
    :return: trading form: open/closed
    """
    if 'Открытые торги' in str(form):
        return 'open'
    else:
        return 'closed'

def check_status(status: str = None):
    """
    Check status
    :param status: str
    :return: staus of trade
    """
    active = ('Прием заявок',)
    pending = ('Торги объявлены', 'Ожидает публикации')
    ended = ('Прием заявок завершен', 'Идут торги', 'Подведение результатов торгов',
             'Торги отменены', 'Торги завершены', 'Торги не состоялись', 'Торги приостановлены')
    try:
        if status in active:
            return 'active'
        elif status in pending:
            return 'pending'
        elif status in ended:
            return 'ended'
    except:
        return None

def remove_extension(value):
    #filename.extension
    return os.path.splitext(value)

def to_str(value: dict) -> dict:
    data = str(value)
    return data

def from_lst_dict_to_str(value):
    data = list(map(lambda x: str(x), value))
    return '\n'.join(data)





class CrawlerParserItem(scrapy.Item):
    data_origin = scrapy.Field()
    trading_id = scrapy.Field()
    trading_link = scrapy.Field()
    trading_number = scrapy.Field()
    trading_type = scrapy.Field()
    trading_form = scrapy.Field()
    trading_org = scrapy.Field()
    trading_org_inn = scrapy.Field()
    trading_org_contacts = scrapy.Field()
    msg_number = scrapy.Field()
    case_number = scrapy.Field()
    debtor_inn = scrapy.Field()
    arbit_manager = scrapy.Field()
    arbit_manager_inn = scrapy.Field()
    arbit_manager_org = scrapy.Field()
    status = scrapy.Field()
    lot_id = scrapy.Field()
    lot_link = scrapy.Field()
    lot_number = scrapy.Field()
    short_name = scrapy.Field()
    lot_info = scrapy.Field()
    property_information = scrapy.Field()
    start_date_requests = scrapy.Field()
    end_date_requests = scrapy.Field()
    start_date_trading = scrapy.Field()
    end_date_trading = scrapy.Field()
    start_price = scrapy.Field()
    step_price = scrapy.Field()
    periods = scrapy.Field()
    files = scrapy.Field()
    created_at = scrapy.Field()






class ParserItemLoader(ItemLoader):
    data_origin_out = TakeFirst()
    trading_id_out = TakeFirst()
    trading_link_out = TakeFirst()
    trading_number_out = TakeFirst()
    trading_type_out = Compose(TakeFirst(), check_trading_type)
    trading_form_out = Compose(TakeFirst(), check_trading_form)
    trading_org_out = Compose(TakeFirst(), lambda x: x.strip().replace('"', '\''), str)
    trading_org_inn_out = TakeFirst()
    trading_org_contacts_out =Compose(TakeFirst(), to_str)
    msg_number_out = TakeFirst()
    case_number_out = TakeFirst()
    debtor_inn_out = TakeFirst()
    arbit_manager_out = TakeFirst()
    arbit_manager_inn_out = TakeFirst()
    arbit_manager_org_out = Compose(TakeFirst(), lambda x: x.strip().replace('"', '\''), str)
    status_out = Compose(TakeFirst(), check_status)
    lot_id_out = TakeFirst()
    lot_link_out = TakeFirst()
    lot_number_out = TakeFirst()
    short_name_out = Compose(TakeFirst(), lambda x: x.strip().replace('"', '\''), str)
    lot_info_out = Compose(TakeFirst(), lambda x: x.strip().replace('"', '\''), str)
    property_information_out = Compose(TakeFirst(), lambda x: x.strip().replace('"', '\''), str)
    start_date_requests_out = TakeFirst()
    end_date_requests_out = TakeFirst()
    start_date_trading_out = TakeFirst()
    end_date_trading_out = TakeFirst()
    start_price_out = TakeFirst()
    step_price_out = TakeFirst()
    periods_out = Compose(from_lst_dict_to_str)
    files_out = Compose(TakeFirst(), to_str)
    created_at_out = TakeFirst()



class DownloadItemLoader(scrapy.Item):
    general = scrapy.Field(output_processor = MapCompose())
    lot = scrapy.Field(output_processor = MapCompose())



