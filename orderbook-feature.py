import pandas as pd
import timeit
import math

def get_sim_df (fn):
    print('loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric,errors='ignore')
    
    # print (df.to_string());print ('------')
    print(df[:20])

    group = df.groupby(['timestamp'])
    return group

def get_sim_df_trade (fn):

    print('loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric,errors='ignore')
    
    group = df.groupby(['timestamp'])
    return group


def faster_calc_indicators(raw_fn):
    
    start_time = timeit.default_timer()

    exchange = 'upbit'
    currency = 'BTC'
    
    group_o = get_sim_df(raw_fn + '-book.csv')
    group_t = get_sim_df_trade(raw_fn + '-trade.csv') #fix for book-1 regression

    delay = timeit.default_timer() - start_time
    print('df loading delay: %.2fs' % delay)
     
    level_1 = 2 
    level_2 = 15 #5

    print('param levels', exchange, currency, level_1, level_2)

    #(ratio, level, interval seconds )
    book_imbalance_params = [(0.2,level_2,1)] 
    book_delta_params = [(0.2,level_2,1)]

    variables = {}
    _dict = {}
    _dict_indicators = {}
    _l_indicator_fn = {}
    _l_indicator_fn['BI'] = live_cal_book_i_v1
    _l_indicator_fn['BDv1'] = live_cal_book_d_v1

    def init_indicator_var(indicator, param):
        var = {}
        var['_flag'] = True

        var['bidSideCount'] = 0
        var['askSideCount'] = 0
        var['bidSideAdd'] = 0
        var['bidSideDelete'] = 0
        var['askSideAdd'] = 0
        var['askSideDelete'] = 0
        var['bidSideTrade'] = 0
        var['askSideTrade'] = 0
        var['bidSideFlip'] = 0
        var['askSideFlip'] = 0

        if (indicator == 'BDv1'):
            var['prevBidQty'] = 0
            var['prevAskQty'] = 0
            var['prevBidTop'] = 0
            var['prevAskTop'] = 0

        return var

    
    for p in book_imbalance_params:
        indicator = 'BI'
        _dict.update( {(indicator, p): []} )
        _dict_var = init_indicator_var(indicator, p)
        variables.update({(indicator, p): _dict_var})
        
    for p in book_delta_params:
        indicator = 'BDv1'
        _dict.update( {(indicator, p): []} )
        _dict_var = init_indicator_var(indicator, p)
        variables.update({(indicator, p): _dict_var})
    
    _timestamp = []
    _mid_price = []

    seq = 0
    print('total groups:', len(group_o.size().index), len(group_t.size().index))
    
    #main part
    for (gr_o, gr_t) in zip (group_o, group_t):
        
        if gr_o is None or gr_t is None:
            print('Warning: group is empty')
            continue

        timestamp = (gr_o[1].iloc[0])['timestamp']
        gr_o = gr_o[1] 
        gr_t = gr_t[1]

        gr_bid_level = gr_o[(gr_o.type == 0)]
        gr_ask_level = gr_o[(gr_o.type == 1)]
        diff = get_diff_count_units(gr_t)

        mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(gr_bid_level, gr_ask_level, gr_t)
        # mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(gr_bid_level, gr_ask_level, gr_t, 'wt')
        # print(mid_price, bid, ask, bid_qty, ask_qty)

        if bid >= ask:
            seq += 1
            continue

        _timestamp.append(timestamp)
        _mid_price.append(mid_price)
        
        _dict_group = {}
        for (indicator, p) in _dict.keys(): #indicator_fn, param
            level = p[1]
            if level not in _dict_group:   
                orig_level = level
                level = min (level, len(gr_bid_level), len(gr_ask_level))   
                _dict_group[level] = (gr_bid_level.head(level), gr_ask_level.head(level))
                
            p1 = () 
            if len(p) == 3:
                p1 = (p[0], level, p[2]) 
            if len(p) == 4:
                p1 = (p[0], level, p[2], p[3]) 
            
            _i = _l_indicator_fn[indicator](p1, _dict_group[level][0], _dict_group[level][1], diff, variables[(indicator,p)], mid_price)
            _dict[(indicator,p)].append(_i)
        
        for (indicator, p) in _dict.keys(): #indicator_fn, param
            col_name = '%s-%s-%s-%s' % (indicator,p[0],p[1],p[2])
            if len(p) == 4 and (indicator == 'TIv1' or indicator == 'TIv2'):
                col_name = '%s-%s-%s-%s-%s' % (indicator,p[0],p[1],p[2],p[3])
            _dict_indicators[col_name] = _dict[(indicator,p)]

        _dict_indicators['timestamp'] = _timestamp
        _dict_indicators['mid_price'] = _mid_price

        seq += 1

    df = pd.DataFrame.from_dict(_dict_indicators)
    print(df)
    outputFilename = _dict_indicators['timestamp'][0][:10] + '-' + exchange + '-' + currency + '-feature.csv'
    df.to_csv(outputFilename, sep='|', index=False)


# Feature calc helper methods

# Feature: calculating midprice using orderbook
# @params
# gr_bid_level: all bid level
# gr_ask_level: all ask level
# group_t: trade data

def cal_mid_price (gr_bid_level, gr_ask_level, group_t, mid_type = ''):
    
    level = 5 
    #gr_rB = gr_bid_level.head(level)
    #gr_rT = gr_ask_level.head(level)
    
    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5 #what is mid price?
    
        if mid_type == 'wt':
            mid_price = ((gr_bid_level.head(level))['price'].mean() + (gr_ask_level.head(level))['price'].mean()) * 0.5
        elif mid_type == 'mkt':
            mid_price = ((bid_top_price*ask_top_level_qty) + (ask_top_price*bid_top_level_qty))/(bid_top_level_qty+ask_top_level_qty)
            mid_price = truncate(mid_price, 1)
        elif mid_type == 'vwap':
            mid_price = (group_t['total'].sum())/(group_t['units_traded'].sum())
            mid_price = truncate(mid_price, 1)
        
        #print mid_type, mid_price

        return (mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty)

    else:
        print('Error: serious cal_mid_price')
        return (-1, -1, -2, -1, -1)

def get_diff_count_units (diff):
    
    _count_1 = _count_0 = _units_traded_1 = _units_traded_0 = 0
    _price_1 = _price_0 = 0

    diff_len = len (diff)
    if diff_len == 1:
        row = diff.iloc[0]
        if row['type'] == 1:
            _count_1 = row['count']
            _units_traded_1 = row['units_traded']
            _price_1 = row['price']
        else:
            _count_0 = row['count']
            _units_traded_0 = row['units_traded']
            _price_0 = row['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

    elif diff_len == 2:
        row_1 = diff.iloc[1]
        row_0 = diff.iloc[0]
        _count_1 = row_1['count']
        _count_0 = row_0['count']

        _units_traded_1 = row_1['units_traded']
        _units_traded_0 = row_0['units_traded']
        
        _price_1 = row_1['price']
        _price_0 = row_0['price']

        return (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0)

def truncate(f, n):
    #Truncates/pads a float f to n decimal places without rounding
    #https://stackoverflow.com/questions/783897/how-to-truncate-float-values
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])

# Feature: calculating 'bookI' using orderbook 
# book imbalance

# @params

# gr_bid_level: all bid level
# gr_ask_level: all ask level
# diff: summary of trade, refer to get_diff_count_units()
# var: can be empty
# mid: midprice

def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):
    
    mid_price = mid

    ratio = param[0]; level = param[1]; interval = param[2]
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 
    
        
    _flag = var['_flag']
        
    if _flag: #skipping first line
        var['_flag'] = False
        return 0.0

    quant_v_bid = gr_bid_level.quantity**ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity**ratio
    price_v_ask = gr_ask_level.price * quant_v_ask
 
    #quant_v_bid = gr_r[(gr_r['type']==0)].quantity**ratio
    #price_v_bid = gr_r[(gr_r['type']==0)].price * quant_v_bid

    #quant_v_ask = gr_r[(gr_r['type']==1)].quantity**ratio
    #price_v_ask = gr_r[(gr_r['type']==1)].price * quant_v_ask
        
    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()
    bid_ask_spread = interval
        
    book_price = 0 #because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty*bidPx)/bidQty) + ((bidQty*askPx)/askQty)) / (bidQty+askQty)

        
    indicator_value = (book_price - mid_price) / bid_ask_spread
    #indicator_value = (book_price - mid_price)
    
    return indicator_value


# Feature: calculating 'bookD' using orderbook and trade

# @params

# gr_bid_level: all bid level
# gr_ask_level: all ask level
# diff: summary of trade, refer to get_diff_count_units()
# var: can be empty
# mid: midprice

def live_cal_book_d_v1(param, gr_bid_level, gr_ask_level, diff, var, mid):

    #print gr_bid_level
    #print gr_ask_level

    ratio = param[0]; level = param[1]; interval = param[2]
    #print ('processing... %s %s,level:%s,interval:%s' % (sys._getframe().f_code.co_name,ratio,level,interval)), 

    decay = math.exp(-1.0/interval)
    
    _flag = var['_flag']
    prevBidQty = var['prevBidQty']
    prevAskQty = var['prevAskQty']
    prevBidTop = var['prevBidTop']
    prevAskTop = var['prevAskTop']
    bidSideAdd = var['bidSideAdd']
    bidSideDelete = var['bidSideDelete']
    askSideAdd = var['askSideAdd']
    askSideDelete = var['askSideDelete']
    bidSideTrade = var['bidSideTrade']
    askSideTrade = var['askSideTrade']
    bidSideFlip = var['bidSideFlip']
    askSideFlip = var['askSideFlip']
    bidSideCount = var['bidSideCount']
    askSideCount = var['askSideCount'] 
  
    curBidQty = gr_bid_level['quantity'].sum()
    curAskQty = gr_ask_level['quantity'].sum()
    curBidTop = gr_bid_level.iloc[0].price #what is current bid top?
    curAskTop = gr_ask_level.iloc[0].price

    #curBidQty = gr_r[(gr_r['type']==0)].quantity.sum()
    #curAskQty = gr_r[(gr_r['type']==1)].quantity.sum()
    #curBidTop = gr_r.iloc[0].price #what is current bid top?
    #curAskTop = gr_r.iloc[level].price


    if _flag:
        var['prevBidQty'] = curBidQty
        var['prevAskQty'] = curAskQty
        var['prevBidTop'] = curBidTop
        var['prevAskTop'] = curAskTop
        var['_flag'] = False
        return 0.0
        
    if curBidQty > prevBidQty:
        bidSideAdd += 1
        bidSideCount += 1
    if curBidQty < prevBidQty:
        bidSideDelete += 1
        bidSideCount += 1
    if curAskQty > prevAskQty:
        askSideAdd += 1
        askSideCount += 1
    if curAskQty < prevAskQty:
        askSideDelete += 1
        askSideCount += 1
        
    if curBidTop < prevBidTop:
        bidSideFlip += 1
        bidSideCount += 1
    if curAskTop > prevAskTop:
        askSideFlip += 1
        askSideCount += 1

    
    (_count_1, _count_0, _units_traded_1, _units_traded_0, _price_1, _price_0) = diff

    #_count_1 = (diff[(diff['type']==1)])['count'].reset_index(drop=True).get(0,0)
    #_count_0 = (diff[(diff['type']==0)])['count'].reset_index(drop=True).get(0,0)
    
    bidSideTrade += _count_1
    bidSideCount += _count_1
    
    askSideTrade += _count_0
    askSideCount += _count_0
    

    if bidSideCount == 0:
        bidSideCount = 1
    if askSideCount == 0:
        askSideCount = 1

    bidBookV = (-bidSideDelete + bidSideAdd - bidSideFlip) / (bidSideCount**ratio)
    askBookV = (askSideDelete - askSideAdd + askSideFlip ) / (askSideCount**ratio)
    tradeV = (askSideTrade/askSideCount**ratio) - (bidSideTrade / bidSideCount**ratio)
    bookDIndicator = askBookV + bidBookV + tradeV
        
       
    var['bidSideCount'] = bidSideCount * decay #exponential decay
    var['askSideCount'] = askSideCount * decay
    var['bidSideAdd'] = bidSideAdd * decay
    var['bidSideDelete'] = bidSideDelete * decay
    var['askSideAdd'] = askSideAdd * decay
    var['askSideDelete'] = askSideDelete * decay
    var['bidSideTrade'] = bidSideTrade * decay
    var['askSideTrade'] = askSideTrade * decay
    var['bidSideFlip'] = bidSideFlip * decay
    var['askSideFlip'] = askSideFlip * decay

    var['prevBidQty'] = curBidQty
    var['prevAskQty'] = curAskQty
    var['prevBidTop'] = curBidTop
    var['prevAskTop'] = curAskTop
    #var['df1'] = df1
 
    return bookDIndicator

def main() :
    rawfilename = "2024-05-01-upbit-BTC"
    faster_calc_indicators(rawfilename)

main()