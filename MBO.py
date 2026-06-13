import asyncio
from datetime import datetime, timedelta

import json

import zlib

import websockets
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_timestamp():
    now = datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"


def partial(res):
    data_obj = res['data'][0]
    bids = data_obj['bids']
    asks = data_obj['asks']
    # print('bids: ', type(bids[0]), bids)
    instrument_id = res['arg']['instId']
    # print('全量数据bids为：' + str(bids))
    # print('档数为：' + str(len(bids)))
    # print('全量数据asks为：' + str(asks))
    # print('档数为：' + str(len(asks)))
    return bids, asks, instrument_id


def update_bids(res, bids_p):
    # 获取增量bids数据
    bids_u = res['data'][0]['bids']
    # print('增量数据bids为：' + str(bids_u))
    # print('档数为：' + str(len(bids_u)))
    # bids合并
    for i in bids_u:
        bid_price = i[0]
        for j in bids_p:
            if bid_price == j[0]:
                if i[1] == '0':
                    bids_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                bids_p.append(i)
    else:
        bids_p.sort(key=lambda price: sort_num(price[0]), reverse=True)
        # print('合并后的bids为：' + str(bids_p) + '，档数为：' + str(len(bids_p)))
    return bids_p


def update_asks(res, asks_p):
    # 获取增量asks数据
    asks_u = res['data'][0]['asks']
    # print('增量数据asks为：' + str(asks_u))
    # print('档数为：' + str(len(asks_u)))
    # asks合并
    for i in asks_u:
        ask_price = i[0]
        for j in asks_p:
            if ask_price == j[0]:
                if i[1] == '0':
                    asks_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                asks_p.append(i)
    else:
        asks_p.sort(key=lambda price: sort_num(price[0]))
        # print('合并后的asks为：' + str(asks_p) + '，档数为：' + str(len(asks_p)))
    return asks_p


def sort_num(n):
    if n.isdigit():
        return int(n)
    else:
        return float(n)


def check(bids, asks):
    # 获取bid档str
    bids_l = []
    bid_l = []
    count_bid = 1
    while count_bid <= 25:
        if count_bid > len(bids):
            break
        bids_l.append(bids[count_bid - 1])
        count_bid += 1
    for j in bids_l:
        str_bid = ':'.join(j[0: 2])
        bid_l.append(str_bid)
    # 获取ask档str
    asks_l = []
    ask_l = []
    count_ask = 1
    while count_ask <= 25:
        if count_ask > len(asks):
            break
        asks_l.append(asks[count_ask - 1])
        count_ask += 1
    for k in asks_l:
        str_ask = ':'.join(k[0: 2])
        ask_l.append(str_ask)
    # 拼接str
    num = ''
    if len(bid_l) == len(ask_l):
        for m in range(len(bid_l)):
            num += bid_l[m] + ':' + ask_l[m] + ':'
    elif len(bid_l) > len(ask_l):
        # bid档比ask档多
        for n in range(len(ask_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(ask_l), len(bid_l)):
            num += bid_l[l] + ':'
    elif len(bid_l) < len(ask_l):
        # ask档比bid档多
        for n in range(len(bid_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(bid_l), len(ask_l)):
            num += ask_l[l] + ':'

    new_num = num[:-1]
    int_checksum = zlib.crc32(new_num.encode())
    fina = change(int_checksum)
    return fina


def change(num_old):
    num = pow(2, 31) - 1
    if num_old > num:
        out = num_old - num * 2 - 2
    else:
        out = num_old
    return out


# subscribe channels un_need login
async def subscribe_without_login(url, channels):
    def def_str():
        return defaultdict(str)

    def def_list():
        return defaultdict(list)

    lob = defaultdict(def_list)
    tickers = defaultdict(def_str)
    writer = {}
    write_time = defaultdict(datetime)
    counters = defaultdict(int)
    file_time = defaultdict(datetime)
    file_counter = defaultdict(int)

    while True:
        try:
            print(0)
            async with websockets.connect(url) as ws:
                print(1)
                sub_param = {"op": "subscribe", "args": channels}
                print(2)
                sub_str = json.dumps(sub_param)
                print(3)
                await ws.send(sub_str)
                print(4)
                print(f"send: {sub_str}")

                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            print(res)
                            continue
                        except Exception as e:
                            print(e)
                            break

                    time_ = get_timestamp()
                    # print(time_ + res)

                    res = eval(res)

                    if 'event' in res:
                        continue

                    instrument_id = res['arg']['instId']
                    if res['arg']['channel'] == 'books':
                        if res['action'] == 'snapshot':
                            bids_p, asks_p, instrument_id = partial(res)
                            lob[instrument_id]['asks_p'] = asks_p
                            lob[instrument_id]['bids_p'] = bids_p

                            checksum = res['data'][0]['checksum']
                            check_num = check(bids_p, asks_p)
                            if check_num == checksum:
                                pass

                            else:
                                await unsubscribe_without_login(url, channels)

                                async with websockets.connect(url) as ws:
                                    sub_param = {"op": "subscribe", "args": channels}
                                    sub_str = json.dumps(sub_param)
                                    await ws.send(sub_str)

                        elif res['action'] == 'update':
                            bids_p = lob[instrument_id]['bids_p']
                            asks_p = lob[instrument_id]['asks_p']

                            bids_p = update_bids(res, bids_p)
                            asks_p = update_asks(res, asks_p)

                            checksum = res['data'][0]['checksum']

                            check_num = check(bids_p, asks_p)

                            if check_num == checksum:
                                lob[instrument_id]['bids_p'] = bids_p
                                lob[instrument_id]['asks_p'] = asks_p

                            else:
                                await unsubscribe_without_login(url, channels)

                                async with websockets.connect(url) as ws:
                                    sub_param = {"op": "subscribe", "args": channels}
                                    sub_str = json.dumps(sub_param)
                                    await ws.send(sub_str)
                                    print(f"send: {sub_str}")

                    elif res['arg']['channel'] == 'tickers':
                        data = res['data'][0]

                        cond = instrument_id in write_time

                        now_time = datetime.now()

                        if (tickers[instrument_id] and tickers[instrument_id]['last'] != data['last'])\
                                or cond and (now_time - write_time[instrument_id]) >= timedelta(seconds=0.5):
                            tickers[instrument_id]['last'] = data['last']
                            tickers[instrument_id]['askSz'] = data['askSz']
                            tickers[instrument_id]['bidSz'] = data['bidSz']

                            asks_li = [','.join([l[0], l[1], l[3]]) for l in lob[instrument_id]['asks_p'].copy()]
                            bids_li = [','.join([l[0], l[1], l[3]]) for l in lob[instrument_id]['bids_p'].copy()]
                            last = tickers[instrument_id]['last']
                            ask_sz = tickers[instrument_id]['askSz']
                            bid_sz = tickers[instrument_id]['bidSz']

                            df = pd.DataFrame({'time': [time_], 'last': [last], 'askSz': [ask_sz], 'bidSz': [bid_sz],
                                               'asks_p': ['|'.join(asks_li)], 'bids_p': ['|'.join(bids_li)]})

                            table = pa.Table.from_pandas(df)

                            cond = instrument_id in file_time and datetime.now() - file_time[instrument_id] >= timedelta(hours=12)
                            if instrument_id not in writer or cond:
                                if instrument_id in writer:
                                    writer[instrument_id].close()

                                file_counter[instrument_id] += 1
                                name = instrument_id.split('-')[0] + f'LOB{file_counter[instrument_id]}.parquet'
                                path = "E:/data/" + name

                                writer[instrument_id] = pq.ParquetWriter(path, table.schema, compression='gzip')
                                print(name, 'created!')
                                file_time[instrument_id] = datetime.now()
                                counters[instrument_id] = 0

                            writer[instrument_id].write_table(table=table)
                            counters[instrument_id] += 1
                            write_time[instrument_id] = now_time
                            print(instrument_id, 'record:', file_counter[instrument_id], counters[instrument_id])

                        else:
                            tickers[instrument_id]['last'] = data['last']
                            tickers[instrument_id]['askSz'] = data['askSz']
                            tickers[instrument_id]['bidSz'] = data['bidSz']

        except Exception as e:
            print(e)
            print("error")
            continue


# unsubscribe channels
async def unsubscribe_without_login(url, channels):
    async with websockets.connect(url) as ws:
        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(f"send: {sub_str}")

        res = await ws.recv()
        print(f"recv: {res}")

if __name__ == "__main__":
    config = json.load(open('config.json'))
    url = config['url']
    tickers_channel = config["tickers_channel"]
    books_channel = config['books_channel']
    coins_list = config['coins_list']
    
    channels = [{"channel": books_channel, "instId": f'{coin}-USDT-SWAP'} for coin in coins_list] +
               [{"channel": tickers_channel, "instId": f'{coin}-USDT-SWAP'} for coin in coins_list]
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(subscribe_without_login(url, channels))
    loop.close()
