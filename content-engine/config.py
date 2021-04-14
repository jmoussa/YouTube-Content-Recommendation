from dotmap import DotMap
import json
import os

CONFIG_LOCATION = os.getenv("CONFIG_LOCATION")

if CONFIG_LOCATION is None:
    raise Exception("Please set environment variable `CONFIG_LOCATION`")

directory = os.fsencode(CONFIG_LOCATION)
master_config = {}
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith(".json"):
        # print(os.path.join(directory, filename))
        f = open(str(CONFIG_LOCATION) + f"/{filename}", "r")
        data = json.load(f)
        _config = DotMap(**data)
        master_config.update(_config)
        continue
    else:
        continue

config = DotMap(**master_config)

if __name__ == "__main__":
    print(json.dumps(config, indent=2))
