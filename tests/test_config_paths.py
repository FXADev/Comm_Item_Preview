import yaml
import pathlib

def test_config_paths():
    """Test that all file paths in config.yml exist."""
    # Check for config in new location first, then fallback to old location
    config_path = 'config/config.yml' if pathlib.Path('config/config.yml').exists() else 'config.yml'
    
    # Load the configuration
    cfg = yaml.safe_load(open(config_path))
    
    # Get all query file paths from the config
    paths = [q['file'] for group in cfg.values() if 'queries' in group for q in group['queries']]
    
    # Verify each path exists
    for p in paths:
        assert pathlib.Path(p).exists(), f"Missing file: {p}"
