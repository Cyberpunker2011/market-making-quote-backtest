import sys
sys.path.append(r'G:\gaojy\Strategy\sqlite_con')
import pandas as pd
import DatabaseCon as dc
from sqlalchemy import create_engine
import datetime as dt
import numpy as np

class environment():
    def __init__(self,strategy_type,date,contract,side_contract = None,delay = 1):
        '''
        :param strategy_type:  'arb' 或者 'single' ，分别对应回测策略是套利策略还是单合约策略
        :param date:  回测日期，决定从数据库获取数据表的日期
        :param contract: 合约名称
        :param side_contract: 默认为None，如果回测策略是套利策略，需要填写
        '''
        self.strategy_type = strategy_type
        self.date = date
        self.contract = contract
        self.side_contract = side_contract
        self.delay = delay


        try:
            self.engine = create_engine("sqlite:///G:/sharedFiles/database/plintradeDB.db", echo=False)
            self.db_handler = dc.db_handler([], [], self.engine)
            print('database connected')
        except Exception as e:
            print('failed to connect database' + str(e))

        # 取行情
        self.main_data = self.db_handler.query_table(self.contract,'d',self.date)
        self.main_kbar = self.db_handler.query_table(self.contract,'kbar',self.date)

        # 取对冲合约行情
        if self.strategy_type == 'arb':
            self.side_data = self.db_handler.query_table(self.side_contract,'d',self.date)
            self.side_kbar = self.db_handler.query_table(self.side_contract, 'kbar', self.date)
        
        try:
            with self.engine.begin() as conn:
                _trade_calender = pd.read_sql('trade_date', conn)
                self.pre_date = _trade_calender[_trade_calender['trade_date'] < self.date].max().values[0]
        except Exception as e:
            print('failed to get pre date'+str(e))

        # 后续处理步骤集中执行
        self.data_handle()
        self.impact_handle()
        self.add_random()



    def data_handle(self):
        '''
        本函数对回测期间的行情进行处理：
        1 首先将行情按照时间间隔进行划分
        2 如果回测策略是套利策略，则对套利行情进行拼接
        3 为后续回测时模拟延迟，将套利中的对冲合约的行情按照三挡进行shift，分别模拟滑点的情况

        函数返回的核心是stream_panel（DataFrame），该数据集包含回测需要的全部数据，同时策略回测时取的窗口行情从该数据集中截取
        * 为方便不同策略类型回测时都可以顺利处理，将stream_panel中的关键数据赋值并同名处理，关键数据有：
            target_price 策略模型建模的关键数据，也是时间窗口获取的主要对象（或者可以先不进行过多操作，具体策略的数据组装，在策略调取时间窗口数据后，自己组装）

        :return: self.stream_panel
        '''
        self.main_data = self.main_data.sort_values(by='timestamp', ascending=True)
        self.main_data = self.main_data[(
                    self.main_data['timestamp'] > self.pre_date[0:4] + '-' + self.pre_date[4:6] + '-' + self.pre_date[
                                                                                                     6:8] + ' 21:00:00.000000')]
        self.main_data['timestamp'] = self.main_data['timestamp'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))



        if self.contract[0:4] == 'SHFE':
            self.evenspace_d = self.main_data.resample(rule='500ms', on='timestamp').last()
        else:
            self.evenspace_d = self.main_data.resample(rule='250ms', on='timestamp').last()
        # _evenspace_d.index = _evenspace_d['timestamp']
        self.evenspace_d = self.evenspace_d.fillna(method='pad', axis=0)
        # 去掉闭市的时间
        self.evenspace_d = self.evenspace_d[~((
                                                      self.evenspace_d.index > self.pre_date[0:4] + '-' + self.pre_date[
                                                                                                          4:6] + '-' + self.pre_date[
                                                                                                                       6:8] + ' 23:00:00.000')
                                              & (
                                                      self.evenspace_d.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 9:00:00.000'))]
        self.evenspace_d = self.evenspace_d[~((
                                                      self.evenspace_d.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 10:15:00.000')
                                              & (
                                                      self.evenspace_d.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 10:30:00.000'))]
        self.evenspace_d = self.evenspace_d[~((
                                                      self.evenspace_d.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 11:30:00.000')
                                              & (
                                                      self.evenspace_d.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 13:30:00.000'))]
        self.evenspace_d = self.evenspace_d[~((
                                                      self.evenspace_d.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 15:00:00.000')
                                              & (
                                                      self.evenspace_d.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 21:00:00.000'))]

        print('main contract data done!')
        if self.strategy_type == 'arb':

            self.side_data = self.side_data.sort_values(by='timestamp', ascending=True)
            self.side_data = self.side_data[(
                    self.side_data['timestamp'] > self.pre_date[0:4] + '-' + self.pre_date[4:6] + '-' + self.pre_date[
                                                                                                        6:8] + ' 21:00:00.000000')]
            self.side_data['timestamp'] = self.side_data['timestamp'].apply(
                lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))

            if self.side_contract[0:4] == 'SHFE':
                self.evenspace_sd = self.side_data.resample(rule='500ms', on='timestamp').last()
            else:
                self.evenspace_sd = self.side_data.resample(rule='250ms', on='timestamp').last()
            # _evenspace_d.index = _evenspace_sd['timestamp']
            self.evenspace_sd = self.evenspace_sd.fillna(method='pad', axis=0)
            # 去掉闭市的时间
            self.evenspace_sd = self.evenspace_sd[~((
                                                          self.evenspace_sd.index > self.pre_date[
                                                                                   0:4] + '-' + self.pre_date[
                                                                                                4:6] + '-' + self.pre_date[
                                                                                                             6:8] + ' 23:00:00.000')
                                                  & (
                                                          self.evenspace_sd.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 9:00:00.000'))]
            self.evenspace_sd = self.evenspace_sd[~((
                                                          self.evenspace_sd.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 10:15:00.000')
                                                  & (
                                                          self.evenspace_sd.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 10:30:00.000'))]
            self.evenspace_sd = self.evenspace_sd[~((
                                                          self.evenspace_sd.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 11:30:00.000')
                                                  & (
                                                          self.evenspace_sd.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 13:30:00.000'))]

            self.evenspace_sd = self.evenspace_sd[~((
                                                          self.evenspace_sd.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 15:00:00.000')
                                                  & (
                                                          self.evenspace_sd.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 21:00:00.000'))]

            print('side contract data done!')

        if self.strategy_type == 'single':
            self.stream_panel = self.evenspace_d
        elif self.strategy_type == 'arb':
            '''
            如果回测类型为套利策略，对stream_panel进行调整，调整为主合约和对冲合约的拼接行情，其中根据预设的延迟水平调整对冲合约的可获得价格
            '''
            self.stream_panel = pd.merge(self.evenspace_d, self.evenspace_sd, how='left', left_index=True, right_index=True,
                                    suffixes=('', '_side'))

            self.stream_panel['hedge_bid_price_lag'] = self.stream_panel['BestBidPrice_side'].shift(-self.delay)
            # self.stream_panel['hedge_bid_price_lag1'] = self.stream_panel['BestBidPrice_side'].shift(-1)
            # self.stream_panel['hedge_bid_price_lag2'] = self.stream_panel['BestBidPrice_side'].shift(-2)
            # self.stream_panel['hedge_bid_price_lag3'] = self.stream_panel['BestBidPrice_side'].shift(-3)
            #
            self.stream_panel['hedge_ask_price_lag'] = self.stream_panel['BestAskPrice_side'].shift(-self.delay)
            # self.stream_panel['hedge_ask_price_lag1'] = self.stream_panel['BestAskPrice_side'].shift(-1)
            # self.stream_panel['hedge_ask_price_lag2'] = self.stream_panel['BestAskPrice_side'].shift(-2)
            # self.stream_panel['hedge_ask_price_lag3'] = self.stream_panel['BestAskPrice_side'].shift(-3)

            #  增加滑点模拟
            for i in range(5):
                self.stream_panel['slippage_bid'+str(i+1)+'_price_side'] = self.stream_panel['bid'+str(i+1)+'_price_side'].shift(-self.delay)
                self.stream_panel['slippage_bid'+str(i+1)+'_volume_side'] = self.stream_panel['bid'+str(i+1)+'_volume_side'].shift(-self.delay)

                self.stream_panel['slippage_ask'+str(i+1)+'_price_side'] = self.stream_panel['ask'+str(i+1)+'_price_side'].shift(-self.delay)
                self.stream_panel['slippage_ask'+str(i+1)+'_volume_side'] = self.stream_panel['ask'+str(i+1)+'_volume_side'].shift(-self.delay)





        return self.stream_panel


    def impact_handle(self):
        '''
        本函数利用数据库中的tick级别的kbar数据中的内外盘数据信息，对合约的市场冲击水平进行建模，为后续回测中模拟市场冲击提供依据。
        1 这里需要对数据进行时间上的分割
        2 是否有必要根据市场冲击模型对市场冲击建模？？？？
        3 需要将市场冲击的数量向前shift，
        :return:
        '''
        self.main_kbar = self.main_kbar.sort_values(by='timestamp', ascending=True)
        self.main_kbar = self.main_kbar[(
                    self.main_kbar['timestamp'] > self.pre_date[0:4] + '-' + self.pre_date[4:6] + '-' + self.pre_date[
                                                                                                     6:8] + ' 21:00:00.000000')]
        self.main_kbar['timestamp'] = self.main_kbar['timestamp'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))



        if self.contract[0:4] == 'SHFE':
            self.evenspace_main_kbar = self.main_kbar.resample(rule='500ms', on='timestamp').last()
        else:
            self.evenspace_main_kbar = self.main_kbar.resample(rule='250ms', on='timestamp').last()
        # _evenspace_main_kbar.index = _evenspace_main_kbar['timestamp']
        self.evenspace_main_kbar = self.evenspace_main_kbar.fillna(method='pad', axis=0)
        # 去掉闭市的时间
        self.evenspace_main_kbar = self.evenspace_main_kbar[~((
                                                      self.evenspace_main_kbar.index > self.pre_date[0:4] + '-' + self.pre_date[
                                                                                                          4:6] + '-' + self.pre_date[
                                                                                                                       6:8] + ' 23:00:00.000')
                                              & (
                                                      self.evenspace_main_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 9:00:00.000'))]
        self.evenspace_main_kbar = self.evenspace_main_kbar[~((
                                                      self.evenspace_main_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 10:15:00.000')
                                              & (
                                                      self.evenspace_main_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 10:30:00.000'))]
        self.evenspace_main_kbar = self.evenspace_main_kbar[~((
                                                      self.evenspace_main_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 11:30:00.000')
                                              & (
                                                      self.evenspace_main_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 13:30:00.000'))]
        self.evenspace_main_kbar = self.evenspace_main_kbar[~((
                                                      self.evenspace_main_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 15:00:00.000')
                                              & (
                                                      self.evenspace_main_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                      4:6] + '-' + self.date[
                                                                                                                   6:8] + ' 21:00:00.000'))]

        print('main contract impact data done!')
        if self.strategy_type == 'arb':

            self.side_kbar = self.side_kbar.sort_values(by='timestamp', ascending=True)
            self.side_kbar = self.side_kbar[(
                    self.side_kbar['timestamp'] > self.pre_date[0:4] + '-' + self.pre_date[4:6] + '-' + self.pre_date[
                                                                                                        6:8] + ' 21:00:00.000000')]
            self.side_kbar['timestamp'] = self.side_kbar['timestamp'].apply(
                lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))

            if self.side_contract[0:4] == 'SHFE':
                self.evenspace_side_kbar = self.side_kbar.resample(rule='500ms', on='timestamp').last()
            else:
                self.evenspace_side_kbar = self.side_kbar.resample(rule='250ms', on='timestamp').last()
            # _evenspace_d.index = _evenspace_side_kbar['timestamp']
            self.evenspace_side_kbar = self.evenspace_side_kbar.fillna(method='pad', axis=0)
            # 去掉闭市的时间
            self.evenspace_side_kbar = self.evenspace_side_kbar[~((
                                                          self.evenspace_side_kbar.index > self.pre_date[
                                                                                   0:4] + '-' + self.pre_date[
                                                                                                4:6] + '-' + self.pre_date[
                                                                                                             6:8] + ' 23:00:00.000')
                                                  & (
                                                          self.evenspace_side_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 9:00:00.000'))]
            self.evenspace_side_kbar = self.evenspace_side_kbar[~((
                                                          self.evenspace_side_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 10:15:00.000')
                                                  & (
                                                          self.evenspace_side_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 10:30:00.000'))]
            self.evenspace_side_kbar = self.evenspace_side_kbar[~((
                                                          self.evenspace_side_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 11:30:00.000')
                                                  & (
                                                          self.evenspace_side_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 13:30:00.000'))]

            self.evenspace_side_kbar = self.evenspace_side_kbar[~((
                                                          self.evenspace_side_kbar.index > self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 15:00:00.000')
                                                  & (
                                                          self.evenspace_side_kbar.index < self.date[0:4] + '-' + self.date[
                                                                                                          4:6] + '-' + self.date[
                                                                                                                       6:8] + ' 21:00:00.000'))]

            print('side contract impact data done!')

        if self.strategy_type == 'single':
            self.impact_panel = self.evenspace_main_kbar
        elif self.strategy_type == 'arb':
            '''
            如果回测类型为套利策略，对stream_panel进行调整，调整为主合约和对冲合约的拼接行情，其中根据预设的延迟水平调整对冲合约的可获得价格
            '''
            self.impact_panel = pd.merge(self.evenspace_main_kbar, self.evenspace_side_kbar, how='left', left_index=True, right_index=True,
                                    suffixes=('', '_side'))

            self.impact_panel['act_buy_side'] = self.impact_panel['OutSideQty_side'].diff(1).shift(-1)
            self.impact_panel['act_sell_side'] = self.impact_panel['InnerSideQty_side'].diff(1).shift(-1)

            self.impact_panel['act_buy_side'] = self.impact_panel['act_buy_side'].fillna(0)
            self.impact_panel['act_sell_side'] = self.impact_panel['act_sell_side'].fillna(0)

        self.impact_panel['act_buy'] = self.impact_panel['OutSideQty'].diff(1).shift(-1)
        self.impact_panel['act_sell'] = self.impact_panel['InnerSideQty'].diff(1).shift(-1)

        self.impact_panel['act_buy'] = self.impact_panel['act_buy'].fillna(0)
        self.impact_panel['act_sell'] = self.impact_panel['act_sell'].fillna(0)

        return self.impact_panel

    def add_random(self):
        self.stream_panel['random_bid'] = np.random.rand(len(self.stream_panel))
        self.stream_panel['random_ask'] = np.random.rand(len(self.stream_panel))
        self.stream_panel['slippage_bid'] = np.random.rand(len(self.stream_panel))
        self.stream_panel['slippage_ask'] = np.random.rand(len(self.stream_panel))


    def lob_simulator(self,timestamp,lob_data,quote,ratio = 1):
        '''
        :param lob_data:
        :param quote:dict , key :['bid','ask','bid_qty','ask_qty']
        :param ratio: 预先设定的，计算成交时的放缩系数，越大成交越激烈，越小成交越少
        :return: {'buy'买是否成交,'buy_volume'买的数量,'sell'卖是否成交,'sell_volume'卖的成交数量}
        '''

        # 被动限价单 确定报价的所在队列的长度
        buy_seq_len = 0
        sell_seq_len = 0
        for i in range(5):
            if lob_data['bid'+str(i+1)+'_price'] > quote['bid']:
                buy_seq_len += lob_data['bid'+str(i+1)+'_volume']
            if lob_data['bid'+str(i+1)+'_price'] == quote['bid']:
                buy_seq_len += round(lob_data['bid'+str(i+1)+'_volume']*lob_data['random_bid'],0)

            if lob_data['ask'+str(i+1)+'_price'] < quote['ask']:
                sell_seq_len += lob_data['ask'+str(i+1)+'_volume']
            if lob_data['ask'+str(i+1)+'_price'] == quote['ask']:
                sell_seq_len += round(lob_data['ask'+str(i+1)+'_volume']*lob_data['random_ask'],0)

        # print('buy_seq_len', buy_seq_len)
        # print('sell_seq_len', sell_seq_len)

        # 获取市场冲击的成交量，并乘以放缩系数
        act_buy = round(self.impact_panel.loc[timestamp]['act_buy'] * ratio,0)
        act_sell = round(self.impact_panel.loc[timestamp]['act_sell'] * ratio,0)

        if act_buy > buy_seq_len:
            buy = True
            buy_qty = min(quote['bid_qty'],(act_buy-buy_seq_len))
        else:
            buy = False
            buy_qty = 0

        if act_sell > sell_seq_len:
            sell = True
            sell_qty = min(quote['ask_qty'],(act_sell-sell_seq_len))
        else:
            sell = False
            sell_qty = 0

        #  利用限价单实现市价单效果，实际成交为市价成交,这里假设对价限价单成交时都以限价单价格成交，这个后续再仔细调整
        if quote['ask'] < lob_data['bid1_price'] :
            sell = True
            sell_qty = quote['ask_qty']
        elif quote['bid'] > lob_data['ask1_price'] :
            buy = True
            buy_qty = quote['bid_qty']

        return {'buy':buy,'buy_price':quote['bid'],'buy_qty':buy_qty,'sell':sell,'sell_price':quote['ask'],'sell_qty':sell_qty,'buy_seq_len':buy_seq_len,'act_buy':act_buy,'sell_seq_len':sell_seq_len,'act_sell':act_sell}

    def slippage_simulatator(self,timestamp,lob_data,slippage_ratio = 0.5):


        # 获取市场冲击的成交量，并乘以放缩系数
        act_buy = round(self.impact_panel.loc[timestamp]['act_buy_side'] * slippage_ratio, 0)
        act_sell = round(self.impact_panel.loc[timestamp]['act_sell_side'] * slippage_ratio, 0)

        slippage_result = {}

        bid_seq_len = 0
        ask_seq_len = 0

        if lob_data['slippage_ask1_price_side']==0:
            slippage_result['hedge_ask'] = lob_data['hedge_ask_price_lag']
        else:
            for i in range(5):
                ask_seq_len += lob_data['slippage_ask'+str(i+1)+'_volume_side']
                if  act_buy < ask_seq_len:
                    slippage_result['hedge_ask'] = lob_data['slippage_ask' + str(i + 1) + '_price_side']

                    break

            if act_buy >= ask_seq_len:
                slippage_result['hedge_ask'] = lob_data['slippage_ask5_price_side']

        if lob_data['slippage_bid1_price_side']==0:
            slippage_result['hedge_bid'] = lob_data['hedge_bid_price_lag']
        else:
            for i in range(5):
                bid_seq_len += lob_data['slippage_bid'+str(i+1)+'_volume_side']

                if  act_sell < bid_seq_len:
                    slippage_result['hedge_bid'] = lob_data['slippage_bid' + str(i + 1) + '_price_side']

                    break

            if act_sell >= bid_seq_len:
                slippage_result['hedge_bid'] = lob_data['slippage_bid5_price_side']

        slippage_result['bid_slippage'] = lob_data['BestBidPrice_side'] - slippage_result['hedge_bid']
        slippage_result['ask_slippage'] = slippage_result['hedge_ask'] - lob_data['BestAskPrice_side']

        return slippage_result

    def react(self,timestamp,lob_data,quote,ratio = 1,slippage_ratio=0.5):
        '''
        根据设定的延迟，调用成交模拟器LOB_simulator，向agent返回成交结果
        :param timestamp:
        :param lob_data:
        :param quote:
        :param ratio:
        :param delay:
        :return:
        '''
        trade_result = self.lob_simulator(timestamp,lob_data,quote,ratio)


        if self.strategy_type == 'arb':
            hedge_result = self.slippage_simulatator(timestamp,lob_data,slippage_ratio)
            if trade_result['buy'] == True:
                buy_price = trade_result['buy_price'] - hedge_result['hedge_bid']  + 2
            else:
                buy_price = 0

            if trade_result['sell'] == True:
                sell_price = trade_result['sell_price'] - hedge_result['hedge_ask'] -2
            else:
                sell_price = 0

        else:
            if trade_result['buy'] == True:
                buy_price = trade_result['buy_price']
            else:
                buy_price = 0

            if trade_result['sell'] == True:
                sell_price = trade_result['sell_price']
            else:
                sell_price = 0

        msg = {'timestamp':timestamp,'buy':trade_result['buy'],'buy_price':buy_price,'buy_qty':trade_result['buy_qty'],'sell':trade_result['sell'],'sell_price':sell_price,'sell_qty':trade_result['sell_qty'],
                'buy_seq_len':trade_result['buy_seq_len'],'act_buy':trade_result['act_buy'],'sell_seq_len':trade_result['sell_seq_len'],'act_sell':trade_result['act_sell']}

        if self.strategy_type == 'arb':
            msg.update(hedge_result)
        return msg


    def get_stream_panel(self):
        return self.stream_panel

    def get_impact_panel(self):
        return self.impact_panel



