import yaml, pathlib
cfg = yaml.safe_load(open('config.yml'))
paths = [q['file'] for group in cfg.values() for q in group['queries']]
for p in paths:
    assert pathlib.Path(p).exists(), f"Missing file: {p}"
