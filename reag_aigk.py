from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

import requests
from stem import Signal
from stem.control import Controller

from time import sleep
import sys
import socket
import socks

import ssl
import re

class CaptchaError(Exception):
    pass
class GetLatLongError(Exception):
    pass

lst_house_lat=['address', 'year', 'base_type', 'build_year', 'cesspools_volume',
                                       'cold_water_type', 'common_hold_square', 'electo_type', 'electro_inputs',
                                       'elevators', 'energo_class', 'entrance', 'ext_info', 'face_type',
                                       'fire_exting_type', 'floors', 'floors_min', 'garbage_chute_count', 'garbage_chute_type',
                                       'gas_supply_type', 'gutter_type', 'hot_water_type', 'id', 'kap_rem_fund',
                                       'land_plot_area', 'latitude', 'living_rooms', 'living_square', 'longtitude',
                                       'manager', 'not_living_rooms', 'not_living_square', 'other', 'overlap_type',
                                       'parking_area', 'playground', 'roof_type', 'roofing_type', 'rooms', 'seria', 'sportground',
                                       'square', 'type', 'underground_square', 'vent_type', 'wall_type', 'warming_type',
                                       'water_disposal_type', 'working_year', 'wreck']

dct_house_translate={'address':'адрес', 'build_year':'Год постройки',
'working_year':'Год ввода дома в эксплуатацию',
'seria':'Серия, тип постройки здания',
'type':'Тип дома',
'kap_rem_fund':'Способ формирования фонда капитального ремонта',
'wreck':'Дом признан аварийным',
'floors':'Количество этажей',
'floors_min': 'наименьшее, ед.',
'entrance':'Количество подъездов, ед.',
'elevators':'Количество лифтов, ед.',
'rooms':'Количество помещений, ед.',
'living_rooms': 'Количество помещений жилых, ед.',
'not_living_rooms':'Количество помещений нежилых, ед.',
'square':'Общая площадь дома, кв.м',
'living_square':'общая площадь жилых помещений, кв.м',
'not_living_square':'общая площадь нежилых помещений, кв.м',
'common_hold_square':'общая площадь помещений, входящих в состав общего имущества, кв.м',
'land_plot_area':'площадь земельного участка, входящего в состав общего имущества в многоквартирном доме, кв.м',
'parking_area':'площадь парковки в границах земельного участка, кв.м',
'energo_class':'Класс энергетической эффективности',
'playground':'детская площадка',
'sportground':'спортивная площадка',
'other':'другое',
'ext_info':'Дополнительная информация',
'base_type':'Тип фундамента',
'overlap_type':'Тип перекрытий',
'wall_type':'Материал несущих стен',
'underground_square':'Площадь подвала по полу, кв.м',
'garbage_chute_type':'Тип мусоропровода',
'garbage_chute_count':'Количество мусоропроводов, ед.',
'face_type':'Тип фасада',
'roof_type':'Тип крыши',
'roofing_type':'Тип кровли',
'electo_type':'Тип системы электроснабжения',
'electro_inputs':'Количество вводов в дом, ед.',
'warming_type':'Тип системы теплоснабжения',
'hot_water_type':'Тип системы горячего водоснабжения',
'cold_water_type':'Тип системы холодного водоснабжения	',
'water_disposal_type':'Тип системы водоотведения',
'cesspools_volume':'Объем выгребных ям, куб. м.	',
'gas_supply_type':'Тип системы газоснабжения',
'fire_exting_type':'Тип системы пожаротушения',
'gutter_type':'Тип системы водостоков'}

class kuce:
    _strBaseURL = r'https://www.reformagkh.ru/myhouse?tid={tid}'
    _strHomeURL = r'https://www.reformagkh.ru/myhouse/profile/view/{tid}/'
    _strListURL1 = r'https://www.reformagkh.ru/myhouse/list?tid={tid}&page=1&limit={pl}'
    _strListURL2 = r'https://www.reformagkh.ru/myhouse/list?page={pn}&limit={pl}'

    _gcontext = None
    _df_regs = None

    _controller=Controller.from_port(port=9151, address='127.0.0.1')
    #_df_houses = pd.DataFrame(columns=['id', 'address', 'year', 'sqr', 'manager'])
    _df_houses = pd.DataFrame(columns=lst_house_lat)

    urlTimeOut = 15 # big, because  www.reformagkh.ru very slow site

    def _change_tor_proxy(self):
        # set new TOR IP
        self._controller.authenticate(password='cmasf')
        self._controller.signal(Signal.NEWNYM)
        sleep(5)
        r = requests.get('http://icanhazip.com/') #check new IP
        print('new IP is ', r.text.strip())

    def _read_url(self, strURL, cooc=None):
        def _check_capcha(strInput):
            cstrCapcha = 'Превышено количество показов Каптча'
            if strInput.find(cstrCapcha) != -1: raise CaptchaError
            sp = BeautifulSoup(strInput, 'html.parser')
            f = sp.find('form', attrs={'action': r'/captcha-form', 'id': 'request_limiter_captcha'})
            if f is not None: raise CaptchaError

        socket.setdefaulttimeout(self.urlTimeOut)
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
        hdrs = {'User-Agent': user_agent}

        while True:
            try:
                req = requests.get(strURL, headers=hdrs, cookies=cooc, timeout=self.urlTimeOut)
                _check_capcha (req.text)
            except:
                #print(sys.exc_info()[0])
                sleep(3)
                self._change_tor_proxy()
            else:
                return req
        #strResp = resp.read().decode(resp.headers.get_content_charset())

    def __init__(self, code='', name='', level=0, is_ssl=True, fileName=''):
        self._df_regs = pd.DataFrame({'reg_name': [name], 'level': [level]}, index=[code])
        if is_ssl:
            #self._gcontext = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            self._gcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv2)

        # TOR MUST be started
        #connect to TOR - TOR must be configured by editing torrc file (in ../Tor Bouser/Bouser/TorBouser/Data/Tor)
        # ControlPort 9151
        # HashedControlPassword 16: A1C3F40FBB9F7C7560C3E4BBEC4BADC7A70EC0C1E06F6D6880F60EBD9A # hashed 'cmasf'
        socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9150, True)
        socket.socket = socks.socksocket
        self._change_tor_proxy() # set new IP for TOP

    def read_regions(self, reg_id=None, level=0):
        ''' fill regions sub-codes from big region'''
        if reg_id is None:
            reg_id = self._df_regs.index.values[0]
            level = self._df_regs['level'].iloc[0]
        s = self._read_url(self._strBaseURL.format(tid=reg_id))
        soup = BeautifulSoup(s.text, 'html.parser')
        regs = pd.Series(
            {re.search(r'tid=(?P<tid>[0-9]+)', str(a)).group('tid'): a.text for a in
             soup.find_all('a', class_='georefs')})

        if not regs.empty:
            d = pd.DataFrame({'reg_name': regs, 'level': level})
            self._df_regs = self._df_regs.append(d)
            for index, name in d.iterrows():
                print('work for', name['reg_name'])
                self.read_regions(reg_id=index, level=level + 1)
        else:
            #print('home level')
            return

    def read_houses_list(self, reg_id=0, page_num=1, item_limit=10000, cookies=None):
        '''read houses list and passwords'''
        def _get_list_page_count(soup):
            try:
                loc = soup.find('form', attrs={'id': 'paginatorForm'}).find('input',
                                                                            attrs={'type': 'hidden', 'name': 'page'})
                return (int(loc['value']), int(loc['data-count-of-elements']))
            except AttributeError:
                return (-1, 0)

        def _get_list_limit(soup):
            try:
                loc = soup.find('form', attrs={'id': 'paginatorForm'}).find('input',
                                                                            attrs={'type': 'hidden', 'name': 'limit'})
                return int(loc['value'])
            except AttributeError:
                # not right url
                return -1

        def _fill_list(soup):
            table = soup.find_all('table')
            tr = table[1].findAll('tr')
            for i, r in enumerate(tr[1:]):
                td = r.findAll('td')
                id=re.search(r'(\d+)', str(td[0].find('a')['href'])).group(1)
                print('step {1} from {2}: read pass for {0}'.format(id, i+1, len(tr[1:])), end='...')
                try:
                    house_pass=self.read_house_passport(id)
                    house_pass.update({'address': td[0].text})
                    self._df_houses = self._df_houses.append([house_pass])
                except:
                    print('Error {0}, skip num {}'.format(sys.exc_info()[0], id))
                    continue
                print(' done')
            return len(tr[1:])

        def _print_info(item_read, item_count, page_number=1, page_limit=item_limit):
            strInfo='done reading for page num {page}, items = {items}, limit = {limit}, all items = {count}'
            print(strInfo.format(page=page_number, limit=page_limit, items=item_read, count=item_count))
            self.save_houses('temporary_page_{}.csv'.format(page_number))

        if page_num==1:
            '''with reg_id work only first page, other pages  get reg_id from cookies'''
            req = self._read_url(self._strListURL1.format(tid=reg_id, pl=item_limit))
            cook = req.cookies # cookies with reg_id

            soup = BeautifulSoup(req.text, 'html.parser')

            spn=soup.findAll('input',  attrs={'type':'hidden', 'name':'page', 'value':1})
            num_page, all_houses = _get_list_page_count(soup)
            pages=np.ceil(all_houses/item_limit)
            item_read =_fill_list(soup) # run for page number 1
            _print_info(item_read, all_houses)
            i=1
            while item_read==item_limit: # run for other pages
                i += 1
                item_read=self.read_houses_list(reg_id=reg_id, page_num=i, cookies=cook)
                _print_info(item_read, all_houses, page_number=i)

            return item_read
        else:
            if cookies is None:
                req = self._read_url(self._strListURL1.format(tid=reg_id, pl=item_limit))
                cookies = req.cookies  # cookies with reg_id
                print(cookies)


            req = self._read_url(self._strListURL2.format(pn=page_num, pl=item_limit), cooc=cookies)
            soup = BeautifulSoup(req.text, 'html.parser')
            n_p, _ = _get_list_page_count(soup)
            if n_p != page_num:
                #check page fail
                return -1
            else:
                return _fill_list(soup)

    @property
    def regions(self):
        return self._df_regs

    def save_regions(self, strFileName):
        self._df_regs.to_csv(strFileName, sep=';', encoding='cp1251')

    def save_houses(self, strFileName):
        try:
            self._df_houses.set_index('id').to_csv(strFileName, sep=';', encoding='cp1251')
        except:
            try:
                self._df_houses.set_index('id').to_csv(strFileName, sep=';')
            except:
                self._df_houses.set_index('id').to_csv(strFileName)


    @property
    def houses(self):
        return self._df_houses

    @property
    def rootname(self):
        return self._df_regs['reg_name'].iloc[0]

    @property
    def rootlevel(self):
        return self._df_regs['level'].iloc[0]

    @property
    def rootID(self):
        return self._df_regs.index.values[0]

    @property
    def last_reg_level(self):
        return self._df_regs['level'].max()

    def _house_get_manager(self, html_soup):
        try:
            table1=html_soup.find('section', attrs={'class':'house_info clearfix'}).find('table', attrs={'class':'upper_text'})
        except:
            print(html_soup)
            return {'id':'', 'manager':''}
        tr = table1.findAll('tr')
        for r in tr:
            td = r.findAll('td')
            manager=td[1].text.strip()
            manID=None
            a=td[1].find('a')
            try:
                manID=re.search(r'(?P<man_id>\d+)', a['href']).group('man_id')
            except TypeError:
                pass
            return {'id':manID, 'manager':manager}

    def _house_get_lat_long(self, html_soup):
        ts=html_soup.find_all('script', attrs={'type':r'text/javascript'})
        for t in ts:
            try:
                f = re.search('window.onload = function', t.string)
            except TypeError:
                continue

            if f:
                sttrr = list(map(str.strip, t.string.split('\n')))
                try:
                    str_long_lat = sttrr[sttrr.index('var myPlacemark = new ymaps.Placemark(') + 1]
                except ValueError:
                    raise GetLatLongError
                    return {'latitude': np.nan, 'longtitude': np.nan}

                ll=re.search(r'(?P<lat>\d+\.\d+),(?P<long>\d+\.\d+)', str_long_lat)
                return {'latitude':ll.group('lat'), 'longtitude':ll.group('long')}

    def _house_get_main_info(self, html_soup):
        table=html_soup.find('section', attrs={'class':'house_info clearfix'}).find('table', attrs={'class':'col_list_group'})
        tbls = table.findAll('table', attrs={'class':'col_list'})
        tr=tbls[0].findAll('tr')
        try:
            square=float(tr[1].find('span').text.replace(' ', ''))
        except ValueError:
            square=None
        try:
            floors=int(tr[3].find('span').text.replace(' ', ''))
        except ValueError:
            floors=None
        tr = tbls[1].findAll('tr')
        anket_change=' '.join(list(map(str.strip, tr[1].find('span').text.split('\n'))))
        man_start=tr[3].find('span').text.strip()
        return {'square':square, 'floors':floors, 'anket_change':anket_change, 'man_start':man_start}

    def _house_get_detail_info(self, html_soup):
        def _convert(func, val):
            try:
                return func(val.replace(' ', ''))
            except ValueError:
                return val
        def _process_first_tab(s):
            spns=s.find('table', attrs={'class':'col_list'}).findAll('span')

            details.setdefault('build_year', _convert(int, spns[1].text)) #
            details.setdefault('working_year', _convert(int, spns[3].text)) #
            details.setdefault('seria', spns[5].text)
            details.setdefault('type', spns[7].text)
            details.setdefault('kap_rem_fund', spns[9].text)
            details.setdefault('wreck', spns[11].text)
            details.setdefault('floors', _convert(int, spns[14].text)) #
            details.setdefault('floors_min', _convert(int, spns[16].text)) #
            details.setdefault('entrance', _convert(int, spns[18].text)) #
            details.setdefault('elevators', _convert(int, spns[20].text)) #
            details.setdefault('rooms', _convert(int, spns[22].text)) #
            details.setdefault('living_rooms', _convert(int, spns[24].text)) #
            details.setdefault('not_living_rooms', _convert(int, spns[26].text)) #
            details.setdefault('square', _convert(float, spns[28].text)) #
            details.setdefault('living_square', _convert(float, spns[30].text)) #
            details.setdefault('not_living_square', _convert(float, spns[32].text)) #
            details.setdefault('common_hold_square', _convert(float, spns[34].text)) #
            details.setdefault('land_plot_area', _convert(float, spns[37].text)) #
            details.setdefault('parking_area', spns[39].text) #
            details.setdefault('energo_class', spns[41].text)
            details.setdefault('playground', spns[44].text)
            details.setdefault('sportground', spns[46].text)
            details.setdefault('other', spns[48].text)
            details.setdefault('ext_info', spns[50].text)

        def _process_sec_tab(s):
            tabls=s.findAll('table', attrs={'class':'col_list'})
            details.setdefault('base_type', tabls[0].findAll('span')[1].text.strip())
            details.setdefault('overlap_type', tabls[1].findAll('span')[1].text.strip())
            details.setdefault('wall_type', tabls[1].findAll('span')[3].text.strip())
            details.setdefault('underground_square', _convert(float, tabls[2].findAll('span')[1].text.strip()))
            details.setdefault('garbage_chute_type', tabls[3].findAll('span')[1].text.strip())
            details.setdefault('garbage_chute_count', _convert(int, tabls[3].findAll('span')[3].text.strip()))

            tabs_serv=s.findAll('table', attrs={'class':'orders overhaul-services-table'})
            details.setdefault('face_type', tabs_serv[0].findAll('td', attrs={'class':''})[0].text.strip())
            details.setdefault('roof_type', tabs_serv[1].findAll('td', attrs={'class': ''})[0].text.strip())
            try:
                details.setdefault('roofing_type', tabs_serv[1].findAll('td', attrs={'class': ''})[1].text.strip())
            except IndexError:
                details.setdefault('roofing_type', tabs_serv[1].findAll('td', attrs={'class': ''})[0].text.strip())
        #for i, spn in enumerate(spns):
        #    print(i, spn.text)

        def _process_third_tab(s):
            tabls = s.findAll('table', attrs={'class': 'col_list'})

            details.setdefault('electo_type',  tabls[0].findAll('span')[1].text.strip())
            details.setdefault('electro_inputs', _convert(int,  tabls[0].findAll('span')[3].text.strip()))

            details.setdefault('warming_type', tabls[1].findAll('span')[1].text.strip())

            details.setdefault('hot_water_type', tabls[2].findAll('span')[1].text.strip())
            details.setdefault('cold_water_type', tabls[3].findAll('span')[1].text.strip())
            details.setdefault('water_disposal_type', tabls[4].findAll('span')[1].text.strip())
            details.setdefault('cesspools_volume', _convert(float, tabls[4].findAll('span')[3].text.strip()))
            details.setdefault('gas_supply_type', tabls[5].findAll('span')[1].text.strip())
            details.setdefault('vent_type', tabls[6].findAll('span')[1].text.strip())
            details.setdefault('fire_exting_type', tabls[7].findAll('span')[1].text.strip())
            details.setdefault('gutter_type', tabls[8].findAll('span')[1].text.strip())

        tabs = html_soup.findAll('div', attrs={'class': 'subtab'})

        details = {}
        _process_first_tab(tabs[0])
        _process_sec_tab(tabs[1])
        _process_third_tab(tabs[2])

        return details

    def read_house_passport(self, house_id):
        r = self._read_url(self._strHomeURL.format(tid=house_id))
        if r.status_code == requests.codes.ok:
            s = BeautifulSoup(r.text, 'html.parser')
            ret_dict=self._house_get_manager(s)
            try:
                ret_dict.update(self._house_get_lat_long(s))
            except GetLatLongError:
                print(' error in lat-long for house_id {}...'.format(house_id), end= ' ')
                ret_dict.update({'latitude': np.nan, 'longtitude': np.nan})
            ret_dict.update(self._house_get_detail_info(s))
            ret_dict.update({'id':house_id})
            return ret_dict
        else:
            print('status request is ', r.status_code)
            return self.read_house_passport(house_id) ## !!!!!!!!!!!!!


if __name__ == '__main__':
    #2236864 - voronegskaja
    #code = 2340399, name = 'bashkirija'
    #code = 2280999 - Moscow
    # Владимирская область			2224825

    # Московская область			2281126
    # Тульская 2326046

    d=kuce(code=2208163, name='altai_kr')
    #d.read_regions()
    #print(d.regions)
    #d.save_regions('bashkirija.csv')
    print(d.rootID)
    d.read_houses_list(d.rootID, item_limit=10000)
    print(d.houses)
    d.save_houses('altaj_kr_houses.csv')

