import yaml
import pandas as pd
from pypxlib import Table


class Config:
    def __init__(self, file_dir):
        self.file_dir = file_dir
        self._data = self._load_config()

    def _load_config(self):
        with open(self.file_dir, 'r') as f:
            return yaml.safe_load(f)

    def get_data(self):
        return self._data

class Processor:
    def __init__(self, config_data):
        self.config_data = config_data
        self.dataset = pd.DataFrame(
            columns = [
                'settlement',
                'market_name',
                'item_name',
                'item_code',
                'item_kzp_category_code',
                'item_retail_price',
                'item_promotional_price'
            ]
        )

    def load_data(self):
        for market_name, market_info in self.config_data.items():
            print(market_info)
            table = Table(market_info['path_to_db'], encoding='windows-1251')
            for row in table:
                self.dataset.loc[len(self.dataset)] = [
                    market_info['settlement'],
                    market_info['name'],
                    row.Item,
                    row.id,
                    "Not Yet Calculated!!!",
                    row.ClientPrice,
                    0
                ]
            print(self.dataset.head())

def main():
    config = Config('./config.yaml')
    processor = Processor(config.get_data())
    processor.load_data()

if __name__ == '__main__':
    main()
