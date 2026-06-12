import asyncio

from datetime import datetime, timedelta

import json
import websockets
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_timestamp():
    now = datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"


def sort_num(n):
    if n.isdigit():
        return int(n)
    else:
        return float(n)


def update_trades(past_li, curr_data):
    price, sz, side = curr_data['px'], curr_data['sz'], curr_data['side']
    for vap in past_li:  # vap = [px, ask_v, bid_v]
        if vap[0] == price:
            if side == 'buy':
                vap[1] = str(sort_num(vap[1]) + sort_num(sz))
            else:
                vap[2] = str(sort_num(vap[2]) + sort_num(sz))
            return past_li
    new_rec = [price, '0', '0']
    if side == 'buy':
        new_rec[1] = sz
    else:
        new_rec[2] = sz
    past_li.append(new_rec)
    past_li.sort(key=lambda x: sort_num(x[0]))
    return past_li


def write_data(inst_id, ohlc, trades):
    global writer, file_counter, file_time, counters
    time_ = datetime.fromtimestamp(int(ohlc[0]) // 1000)
    if not ohlc:
        ohlc = ['', '', '', '']
    if not trades:
        trades = [[]]
    vap = [','.join(l) for l in trades.copy()]

    df = pd.DataFrame({'time': [time_], 'open': [ohlc[1]], 'high': [ohlc[2]], 'low': [ohlc[3]], 'close': [ohlc[4]],
                       'vap': ['|'.join(vap)]})
    print(time_, inst_id, 'saved!', get_timestamp())
    table = pa.Table.from_pandas(df)

    cond = inst_id in file_time and datetime.now() - file_time[inst_id] >= timedelta(hours=24)
    if inst_id not in writer or cond:
        if inst_id in writer:
            writer[inst_id].close()
        file_counter[inst_id] += 1
        name = inst_id.split('-')[0] + f'VAP{file_counter[inst_id]}.parquet'
        path = "E:/data/" + name
        writer[inst_id] = pq.ParquetWriter(path, table.schema, compression='gzip')
        print(f'{name} created!')
        file_time[inst_id] = datetime.now()
        counters[inst_id] = 0

    writer[inst_id].write_table(table=table)
    counters[inst_id] += 1
    print(inst_id, 'record:', file_counter[inst_id], counters[inst_id])


# subscribe channels un_need login
async def subscribe_without_login(url, channels):
    trades = defaultdict(list)
    ohlc_data = defaultdict(list)

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

                    # time_ = get_timestamp()
                    # print(time_ + res)

                    res = eval(res)

                    if 'event' in res:
                        continue
                    # print(res['data'][0]['asks'][0])
                    # print(len(res['data'][0]['asks']))
                    # print('args: ', res['arg'])

                    instrument_id = res['arg']['instId']
                    if res['arg']['channel'] == 'trades':
                        past_li = trades[instrument_id]
                        curr_data = res['data'][0]
                        trades[instrument_id] = update_trades(past_li, curr_data)

                    elif res['arg']['channel'].startswith('candle'):
                        ohlc = res['data'][0]
                        if ohlc_data[instrument_id] and ohlc_data[instrument_id][0] != ohlc[0]:
                            write_data(instrument_id, ohlc_data[instrument_id], trades[instrument_id])
                            trades[instrument_id] = []
                            ohlc_data[instrument_id] = []

                        else:
                            ohlc_data[instrument_id] = ohlc

        except Exception as e:
            print(e)
            print("error")
            continue


if __name__ == '__main__':
    writer = {}
    counters = defaultdict(int)
    file_time = defaultdict(datetime)
    file_counter = defaultdict(int)

    url = "wss://wsaws.okx.com:8443/ws/v5/public"
    channels = [{"channel": "candle1m", "instId": "BTC-USDT-SWAP"}, {"channel": "trades", "instId": "BTC-USDT-SWAP"},
                {"channel": "candle1m", "instId": "ETH-USDT-SWAP"}, {"channel": "trades", "instId": "ETH-USDT-SWAP"},
                {"channel": "candle1m", "instId": "XRP-USDT-SWAP"}, {"channel": "trades", "instId": "XRP-USDT-SWAP"},
                {"channel": "candle1m", "instId": "ETC-USDT-SWAP"}, {"channel": "trades", "instId": "ETC-USDT-SWAP"},
                ]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(subscribe_without_login(url, channels))
    loop.close()
