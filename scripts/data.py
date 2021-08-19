import yfinance
from threading import Thread
import os

def get_symbols():
    with open('nasdaqlisted.txt') as handle:
        lines = handle.read().split('\n')
        return [name.split('|')[0] for name in lines[1:]]

def process(symbols):
    for symbol in symbols:
        output_path = f'.,/data/{symbol}.csv'
        
        if os.path.exists(output_path):
            continue

        ticker = yfinance.Ticker(symbol)
        history = ticker.history(period="max", interval="1d")
        history.to_csv(output_path)

        print(symbol)

def main():
    symbols = get_symbols()

    workers = []
    count = len(symbols) // 15

    for i in range(count):
        start_index = i * 15
        end_index = start_index + 15
        worker_symbols = symbols[start_index:end_index]

        new_worker = Thread(target=process, args=(worker_symbols,))

        new_worker.start()
        workers.append(new_worker)

    for worker in workers:
        worker.join()

if __name__ == '__main__':
    main()
