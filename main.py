import yaml


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

    def load_data(self):
        for market_name, market_info in self.config_data.items():
            print(market_info)

def main():
    config = Config("./config.yaml")
    processor = Processor(config.get_data())
    processor.load_data()

if __name__ == "__main__":
    main()
