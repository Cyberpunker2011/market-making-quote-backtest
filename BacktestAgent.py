import sys
sys.path.append(r'G:\gaojy\Strategy\sqlite_con')
import pandas as pd
import DatabaseCon as dc
from sqlalchemy import create_engine
import datetime as dt
import numpy as np
from abc import ABCMeta,abstractmethod

class agent(object):
    '''
    策略的抽象类，所有策略模型需要继承该类
    '''
    __metaclass__ = ABCMeta

    def __init__(self,isMarketMaking,isArbitrage,timespan,hyper_para):
        # self.record = pd.DataFrame(columns=['timestamp','target','position','price','position_value','cash','total','buy_signal','buy_volume','buy_price','sell_signal',
        #                                     'sell_volume','sell_price'])

        self.record = []

        self.position = dict(timestamp='',target='',position=0,price=0,position_value=0,cash=0,total=0,buy_signal='',buy_volume=0,buy_price=0,sell_signal='',
                                            sell_volume=0,sell_price=0)
        self.isMarketMaking = isMarketMaking
        self.isArbitrage = isArbitrage
        self.timespan = timespan
        self.hyper_para = hyper_para
        self.model_para = dict()




    def rcv_data_window(self,data_window):
        self.data_window = data_window

    def rcv_impact_window(self,impact_window):
        self.impact_window = impact_window

    def rcv_present_data(self,timestamp,data_present):
        self.timestamp = timestamp
        self.data_present = data_present

    def rcv_market_response(self,market_response):
        self.market_response = market_response

    @abstractmethod
    def modify_data(self):
        '''
        准备模型的输入数据，其中包含特征工程等内容，所以需要继承时进行重载
        :return:
        '''
        raise NotImplementedError('should implement modify_data()')

    @abstractmethod
    def calculate_signal(self):
        '''
        核心模型计算方法， 根据准备好的数据计算信号
        :return:
        '''
        raise NotImplementedError('should implement calculate_signal()')


    @abstractmethod
    def quote(self):
        '''
        根据回测的策略类型，发送限价单
        :return:
        '''
        raise NotImplementedError('should implement quote()')



    def update(self,timestamp):
        '''
        根据最新的行情，更新头寸的value，同时根据environment的对报价成交的反应，更新头寸信息
        Position:
        (timestamp='',target='',position=0,price=0,position_value=0,cash=0,total=0,buy_signal='',buy_volume=0,buy_price=0,sell_signal='',
                                            sell_volume=0,sell_price=0)

        market_response:
        ('timestamp':timestamp,'buy':trade_result['buy'],'buy_price':buy_price,'buy_qty':trade_result['buy_qty'],'sell':trade_result['sell'],
        'sell_price':sell_price,'sell_qty':trade_result['sell_qty']}

        :return: self.position
        '''
        self.position['price'] = self.data_present['ref_price']

        self.position['timestamp'] = timestamp

        if self.market_response['buy'] == True:
            
            self.position['buy_signal'] = 'buy'
            self.position['buy_volume'] = self.market_response['buy_qty']
            self.position['buy_price'] = self.market_response['buy_price']

            self.position['position'] += self.market_response['buy_qty']
            self.position['cash'] -= self.market_response['buy_qty'] * self.market_response['buy_price'] * self.hyper_para['multi']


        if self.market_response['sell'] == True:
            
            self.position['sell_signal'] = 'sell'
            self.position['sell_volume'] = self.market_response['sell_qty']
            self.position['sell_price'] = self.market_response['sell_price']

            self.position['position'] -= self.market_response['sell_qty']
            self.position['cash'] += self.market_response['sell_qty'] * self.market_response['sell_price'] * self.hyper_para['multi']

        self.position['position_value'] = self.position['price']*self.position['position'] * self.hyper_para['multi']
        self.position['total'] = self.position['position_value'] + self.position['cash']

        self.record.append(self.position.copy())

    def get_record(self):
        return pd.DataFrame(self.record)




    def run_on_new(self,data_window):
        self.push_data_window(data_window)
        self.modify_data()

        trade_signal = self.calculate_signal()

        self.push_present_data(data_window.iloc[-1])
        self.quote(trade_signal)

