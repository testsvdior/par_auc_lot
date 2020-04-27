from scrapy.spiders import Spider
from scrapy import Request
from scrapy.loader.processors import TakeFirst
from ..items import ParserItemLoader
from ..items import CrawlerParserItem
from ..items import DownloadItemLoader
import os
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup as BS
import re
import logging
import requests

logger = logging.getLogger('logger_nistp')



main_url = 
main_lot_url = '

proxies = {
       
}

session = requests.Session()
session.headers = {
    "User-Agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html) Safari/537.36"}
session.proxies.update(proxies)

# list for adding lot links for checking files to download
# adding new lot_link in function "parse_trading" and delete after and one full loop
lot_link_global = []

################___________FORMAT_____TIME_______##############
def format_time_strftime(func):
    def wrapper(*args):
        t = func(*args)
        return t.strftime('%Y-%m-%d %H:%M:%S')
    return wrapper

@format_time_strftime
def format_time(strtime):
    date = strtime
    return datetime.strptime(date, '%d.%m.%Y %H:%M')

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)
###############__________END____FORMAT_____TIME################


##############______DIR__FOR______DOWNLOADS####################
main_dir_download = '/home/admin/web/78.24.219.218/public_html/downloads/'
#############______END_____CREATE________DIR#####################

class NistpSpider(Spider):

    name = 'nistp'
    allowed_domains = [    start_urls = [
       
            ]

    def parse(self, response):
        # list of trading pages on pagination page
        lst_nodes = []
        # links fetch for xpath to "Код торгов"
        all_links = response.xpath('//td[@class="views-field views-field-phpcode-3"]').extract()
        regex_node = re.compile(r'node.?\d+')
        for link_node in all_links:
            match = regex_node.findall(str(link_node))
            lst_nodes.append(''.join(map(str, match)))
        # iterate trading links and begin fetch data
        for page_node in lst_nodes:
            url_node = main_url + '/' + str(page_node)
            yield Request(url=url_node, callback=self.parse_trading)
        
        next_page = response.css('li.pager-next a::attr(href)').get()
        if next_page is not None:
            next_page = main_url + next_page
            if next_page != 'http://nistp.ru/trades?page=3':
                yield Request(next_page, callback=self.parse)


    def parse_trading(self, response):
        """Parse trading page"""
        parse_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        info_trade = '//th[contains(text(),"ормация о торгах")]'
        check_date_requests = response.xpath(
                info_trade +
                '/following::td[contains(text(),"ата начала представления заявок")]/following-sibling::td[1]/span[@class]/text()').extract_first().strip('\n, -').replace('- ','')
        # check date for stoping parser
        dt = format_time(check_date_requests)
        if dt < '2017-01-01 00:00:00':
            yield
            
        else:
            lots_link = response.xpath(
            '//th/a[contains(text(),"Лот")][@href]').extract()
            # count - variable for count lots on page on put into xpath
            count = 0
            for lot in lots_link:
                count += 1
                if lot or not lot:
                    lot_id = ''.join(map(str, (re.findall(r'href=\W\d+\"', lot))))
                    lot_id = ''.join(map(str, (re.findall(r'\d+?', lot_id))))
                    lot_url = main_lot_url +'/' + lot_id
                    lot_number = response.xpath(
                        '//a[@href=$lot_id]/text()', lot_id=lot_id)
                    lot_number = ''.join(
                        map(str, (re.findall(r'\d{1,4}', str(lot_number)))))
                    l = ParserItemLoader(CrawlerParserItem(), response=response)
                    l.add_value('data_origin', main_url)
                    l.add_value(
                        'trading_id', ''.join(
                            re.findall(
                                r'\d+$', response.url)))
                    l.add_value('trading_link', response.url)
                    l.add_xpath(
                        'trading_number',
                        '//tr/th[text()="Информация о торгах"]/following::tr/td[text()="Код торгов"]/following-sibling::td[1]/text()')
                    l.add_xpath(
                        'trading_type',
                        '//tr/th[text()="Информация о торгах"]/following::tr/td[text()="Код торгов"]/following-sibling::td[1]/text()')
                    l.add_xpath(
                        'trading_form',
                        '//td[text()="Способ определения участников"]/following-sibling::td[1]/text()')

                    trade_org_info = '//th[text()="Информация об организаторе"]'
                    last_name_org = response.xpath(
                        trade_org_info +
                        '/following::td[text()="Фамилия"]/following-sibling::td[1]/text()').extract_first()
                    first_name_org = response.xpath(
                        trade_org_info +
                        '/following::td[text()="Имя"]/following-sibling::td[1]/text()').extract_first()
                    middle_name_org = response.xpath(
                        trade_org_info +
                        '/following::td[text()="Отчество"]/following-sibling::td[1]/text()').extract_first()
                    if_organization = response.xpath(
                        trade_org_info +
                        '/following::td[text()="Сокращенное наименование"]/following-sibling::td[1]/text()').extract_first()
                    inn = response.xpath(
                        '//div[@class="node profile"]/following::table[1]//td[contains(text(),"ИНН")]/following-sibling::td[1]/text()').get()
                    try:
                        email_xpath = ''.join(response.xpath(
                            trade_org_info +
                            '/following::td[text()="Адрес электронной почты"]/following-sibling::td[1]/text()').extract_first())
                        if '@' in email_xpath:
                            email_xpath = email_xpath
                        else:
                            email_xpath = ''
                    except:
                        return 'Email is not correct'

                
                    phone_xpath = ''.join(response.xpath(
                            trade_org_info +
                            '/following::td[text()="Телефон\Факс"]/following-sibling::td[1]/text()').extract_first())
                
                    ##############__________Multiraplace__________#############
                    def replaceMultiple(mainString, toBeReplaces, newString):
                        # Iterate over the sings to be replaced
                        for elem in toBeReplaces:
                            # Check if string is in the main string
                            if elem in mainString:
                                # Replace the string
                                mainString = mainString.replace(elem, newString)

                        return mainString

                    pattern_replace = ['(', ')', '-', '+', ' ']
                    ##############__________Multiraplace____END______##############
                    full_name = ''
                    if (last_name_org is not None) and (
                            first_name_org is not None) and (middle_name_org is not None):
                        full_name = last_name_org + ' ' + first_name_org + ' ' + middle_name_org
                    phone_xpath = replaceMultiple(phone_xpath, pattern_replace, '')
                    try:
                        for num in phone_xpath.strip():
                            if num.isdigit:
                                continue
                            else:
                                phone_xpath == ''
                                break

                    except:
                        return 'Phone is not correct'
                

                    contacts = {
                        'email':email_xpath,
                        'phone':phone_xpath}
                    ##################################################

                    if len(full_name) >0:
                        l.add_value('trading_org', full_name)
                
                    else:
                        l.add_value('trading_org', ''.join(if_organization).replace('\"', '\''))
                    l.add_xpath('trading_org_inn', '//th[contains(text(),"Информация об организатор")]/following::td[contains(text(), "ИНН")]/following-sibling::td[1]/text()') 
                            
                    l.add_value('trading_org_contacts', contacts)

                    info_trade = '//th[contains(text(),"ормация о торгах")]'
                    l.add_xpath(
                        'msg_number',
                        info_trade + '/following::td[contains(text(),"Номер объявления о проведении торгов")]/following-sibling::td[1]/text()')

                    l.add_xpath(
                        'case_number',
                        '//th[text()="Сведения о банкротстве"]/following::td[contains(text(),"Номер дела о банкротстве")]/following-sibling::td[1]/text()')


                    l.add_xpath(
                        'debtor_inn',
                        '//th[text()="Сведения о должнике"]/following::td[contains(text(),"ИНН")]/following-sibling::td[1]/text()',
                        re=r'^\d{10,12}')

                    l.add_xpath(
                        'arbit_manager',
                        '//th[text()="Арбитражный управляющий"]/following::td[contains(text(),"Фамилия, Имя, Отчество")]/following-sibling::td[1]/text()')
                    l.add_xpath(
                        'arbit_manager_inn',
                        '//th[text()="Арбитражный управляющий"]/following::td[contains(text(),"ИНН")]/following-sibling::td[1]/text()',
                        re=r'^\d{10,12}')
                    l.add_xpath(
                        'arbit_manager_org',
                        '//th[text()="Арбитражный управляющий"]/following::td[contains(text(),"Организация арбитражных управляющих")]/following-sibling::td[1]/text()')
                    l.add_xpath(
                        'status',
                        '//th[text()="Информация о торгах"]/following::tr/td[text()="Текущий статус"]/following-sibling::td[1]/text()')

                    l.add_value('lot_id', lot_id)
                    l.add_value('lot_link', lot_url)
                    lot_link_global.append(lot_url)
                    l.add_value('lot_number', lot_number)
                    l.add_xpath(
                        'short_name',
                        '//a[@href={}]/following::td[contains(text(), "едмет торгов")]/following-sibling::td[1]/text()'.format(str(lot_id)))

                    l.add_xpath(
                        'lot_info',
                        '//a[@href={}]/following::td[contains(text(), "ведения об имуществе")]/following-sibling::td[1]/text()'.format(str(lot_id)))
                    l.add_xpath(
                        'property_information',
                        '//a[@href={}]/following::td[contains(text(), "орядок ознакомления с имущес")]/following-sibling::td[1]/text()'.format(str(lot_id)))

                    #########________START___PERIODS_____________##################

                
                    try:
                        start_date_requests_out_periods = response.xpath(
                            info_trade +
                            '/following::td[contains(text(),"ата начала представления заявок")]/following-sibling::td[1]/span[@class]/text()').extract_first().strip('\n, -').replace(
                            '- ',
                            '')

                        l.add_value(
                            'start_date_requests',
                            format_time(start_date_requests_out_periods))
                    except BaseException:
                        return

                    try:
                        end_date_requests_out_periods = response.xpath(
                            info_trade +
                            '/following::td[contains(text(),"ата окончания представления заявок")]/following-sibling::td[1]/span[@class]/text()').extract_first().strip(
                            '\n, -').replace(
                            '- ',
                            '')

                        l.add_value(
                            'end_date_requests',
                            format_time(end_date_requests_out_periods))
                    except BaseException:
                        return
                    ##################____________IF_____START____DATE____PRESENT________########

                    try:
                        start_trading = response.xpath(
                            info_trade +
                            '/following::td[contains(text(),"ата проведения")]/following-sibling::td[1]/span[@class]/text()').get(default=None)
                    except:
                        continue


                    if start_trading is None:
                        try:
                            start_date_trading_out_periods = response.xpath(
                                info_trade +
                                '/following::td[contains(text(),"ата начала представления заявок")]/following-sibling::td[1]/span[@class]/text()').extract_first().strip('\n, -').replace(
                                '- ',
                                '')

                            l.add_value(
                                'start_date_trading',
                                format_time(start_date_trading_out_periods))
                            # # test value nedd delete
                            # l.add_value('end_date_trading', "null")
                        except BaseException:
                            return

                        try:
                            end_date_trading_out_periods = response.xpath(
                                info_trade +
                                '/following::td[contains(text(),"ата окончания представления заявок")]/following-sibling::td[1]/span[@class]/text()').extract_first().strip(
                                '\n, -').replace(
                                '- ',
                                '')

                            l.add_value(
                                'end_date_trading',
                                format_time(end_date_trading_out_periods))
                        except BaseException:
                            return
                    else:
                        try:
                            start_trading = start_trading.strip(
                            '\n, -').replace(
                            '- ',
                            '')
                            l.add_value(
                                'start_date_trading',
                                format_time(start_trading))
                        except BaseException:
                            return


                    l.add_xpath(
                        'start_price',
                        '//a[@href={}]/following::td[contains(text(), "тартовая цена")]/following-sibling::td[1]/text()'.format(
                            str(lot_id)),
                        TakeFirst(),
                        float,
                        re=r'^\d+?\.\d{2}')

                    l.add_xpath(
                        'step_price',
                        '//a[@href={}]/following::td[contains(text(), "Шаг аукциона")]/following-sibling::td[1]/text()'.format(
                        str(lot_id)),
                        TakeFirst(),
                        float,
                        re=r'^\d+?\.\d{2}')


                    table = response.xpath(
                        '//a[@href=$lot_id]/following::div[@class="view view-intervals-for-lot-pp view-id-intervals_for_lot_pp view-display-id-default view-dom-id-{}"]//table//tbody//tr'.format(count),
                        lot_id=lot_id)
                    period_dict = []
                    for tr in table:

                        start_date_requests = tr.xpath(
                            'td[1]//span//text()').extract_first().strip('\n, -, ').replace('- ', '')

                        end_date_requests = tr.xpath(
                            'td[2]//span//text()').extract_first().strip('\n, -, " ",').replace('- ', '')


                        period = {
                            'start_date_requests': format_time(start_date_requests),
                            'end_date_requests': format_time(end_date_requests),
                            "end_date_trading": format_time(end_date_requests),
                            "current_price": float(
                                tr.xpath('td[3]//text()').extract_first().strip('\n, -').replace(
                                    '- ',
                                    '').replace(
                                    ',',
                                    '.').replace(
                                    ' ',
                                    '')),
                        }
                        period_dict.append(period)
                    l.add_value('periods', period_dict)
                    #####################____END__PERIODS_____#####################

                    ##################_____DOWNLOAD____FILES_____##################
                    l.add_value('files', self.download_files(response))
                    l.add_value('created_at', parse_date)
                    yield l.load_item()
    
    def create_dir(self):
        return Path(f'{main_dir_download}{year}/{month}').mkdir(parents=True, exist_ok=True)


    def download_files(self, response):
        d = DownloadItemLoader()
        current_url = response.url
        idLot = ''.join(re.findall('\d+$', str(current_url)))
        soup = BS(response.text, 'lxml')
        find_table = soup.find(id=f'node-{str(idLot)}')
        link_files = []
        for file_l in find_table.find_all(href=re.compile(
            r'http://nistp.ru/sites.*/files/*?\d+/\d+\.\w+')):
            link_files.append(file_l)

        general = []
        lst_link_trade_files = []
        for l in link_files:
            link_download = l.get('href')
            original_name = str(l.get_text())
            file_name = ''.join(
                        re.findall(
                            r'\d+$', response.url)) +'~~' + original_name
            
            # reletive_file_name variable for representation path to file in file general
            reletive_file_name = '/downloads/' + file_name.replace(' ', '_').replace('(', '_').replace(')', '_')
            
            save_path = self.create_dir()
            save_path = f'{main_dir_download}{year}/{month}'
            full_path = save_path + '/' +  file_name.replace(' ', '_').replace('(', '_').replace(')', '_')
            
            with open(full_path, 'wb') as file:
                response_trade = session.get(link_download)
                file.write(response_trade.content)
                lst_link_trade_files.append(link_download)
                general.append({'original_name': original_name, 'link': reletive_file_name, 'link_etp': link_download})
        d['general'] = general
        

        ###############_____Lot____Page_____######################
        # lot_liks = soup.find_all(href=re.compile(r'^\d+$'))
        for lot in lot_link_global:
            if len(lot) > 0:
                lot_id = ''.join(map(str, (re.findall(r'\d+$', str(lot)))))
                lot_url = main_lot_url + '/' + lot_id
                
                # made requests to lot page
                response = session.get(lot_url)
                soup = BS(response.text, 'lxml')
                link_files = soup.find_all(
                    href=re.compile(r'http://nistp.ru/sites.*/files/*?\d+/\d+\.\w+'))
                # lot_1 -> list for store data about download files from lot page
                lot_1 = []
                for l in link_files:
                    link_download_lot = l.get('href')
                    original_name = str(l.get_text())
                    file_name = lot_id + '_lot~' + original_name
                    # reletive_file_name variable for representation path to file in file lots
                    reletive_file_name = '/downloads/' + file_name.replace(' ', '_').replace('(', '_').replace(')', '_')
                    
                    save_path = f'{main_dir_download}{year}/{month}'
                    full_path = save_path + '/' + file_name.replace(' ', '_').replace('(', '_').replace(')', '_')
                    
                    response_lot = session.get(link_download_lot)
                    if link_download_lot not in lst_link_trade_files:
                        with open(full_path, 'wb') as file:
                            file.write(response_lot.content)
                            lot_1.append({'original_name': original_name, 'link': reletive_file_name, 'link_etp': link_download_lot})
                d['lot'] = lot_1
                lot_link_global.clear()

        return d
