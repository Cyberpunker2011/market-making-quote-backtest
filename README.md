These backtesting codes are prepared for data format of eploestar 9.5 or esunny 9.0 which have outersideqty and innersideqty in their market data. I am a new .... I have been in China commodlity market making business for one and half year. Because of the limitation of my working environment, I mainly use esunny api to aquire data. Those codes is a simple framework of backtest market making strategies with random sampling and simulation of market impact. The solution I have is to use outerside and innerside quantities. There are two classes. One is agent which requires to be derived to rewrite the abstract method, "calculate_signal". And the environment class reads data and use them to simulate the queue and calculate whether the quotes would be filled. The codes are lacking detail and the full version is on my working laptop...... I put it here for the case that I would use them in my own study off work.
