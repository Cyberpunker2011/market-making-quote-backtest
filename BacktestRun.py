import sys
import time

sys.path.append(r'G:\gaojy\Strategy\sqlite_con')
import pandas as pd
import DatabaseCon as dc
from sqlalchemy import create_engine
import datetime as dt
import numpy as np
import KalmanFilterPkg as KF
from BacktestAgent import agent
import scipy.stats as ss


class backtestrunner():
    def __init__(self,env,agent):
        self.env = env
        self.agent = agent

    def run_backtest(self,ratio = 0.5,slippage_ratio=0.5):

        timespan_counter = 0

        buy_times = 0

        buy_volumes = 0

        sell_times = 0

        sell_volumes = 0

        start_t = time.time()

        for timestamp, data_slice in self.env.stream_panel.iterrows():

            if data_slice['bid1_price'] == 0:
                continue

            self.agent.rcv_present_data(timestamp,data_slice)

            self.agent.modify_data()

            if timespan_counter < 50:
                timespan_counter+=1
                continue

            else:

                self.agent.calculate_signal()

                quote_detail = self.agent.quote()

                market_response = self.env.react(timestamp,data_slice,quote_detail,ratio=ratio,slippage_ratio=slippage_ratio)

                self.agent.rcv_market_response(market_response)

                self.agent.update(timestamp)

            if (timespan_counter%500 == 0) or (market_response['buy'] == True) or (market_response['sell'] == True):
                # 记录买成交次数
                if market_response['buy'] == True:
                    buy_times += 1
                    buy_volumes += market_response['buy_qty']
                # 记录卖成交次数
                if market_response['sell'] == True:
                    sell_times += 1
                    sell_volumes += market_response['sell_qty']

                print('~'*150)
                print('backtest running***  timestamp:',timestamp,'\nquote:',quote_detail,'\nmarket_response:',market_response,'\nposition:',self.agent.position['position'],'balance:',
                      self.agent.position['total'],'\nbuy_threshold:',self.agent.position['buy_threshold'],'sell_threshold:',self.agent.position['sell_threshold'])

                print('orignal_mu:',self.agent._mu,'  filltered mu:',self.agent.signal['mu'])

                print('LOB***','ref_price',data_slice['ref_price'],'kf_price',data_slice['kf_price'], '   bid2_price', data_slice['bid2_price'], 'bid1_price', data_slice['bid1_price'],
                      'ask1_price', data_slice['ask1_price'], 'ask2_price', data_slice['ask2_price'])

                print('trade_times_buy:',buy_times,' trade_times_sell:',sell_times,' buy_volumes:',buy_volumes,' sell_volumes:',sell_volumes)

            timespan_counter += 1

        print('Backtesting cost ',int(time.time() - start_t),'s')

