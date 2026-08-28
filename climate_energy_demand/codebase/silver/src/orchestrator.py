import importlib
import yaml
import os

def run_silver_pipeline(config_name):
    # 1. Load the YAML
    with open(f"configs/{config_name}.yml", "r") as f:
        config = yaml.safe_load(f)
    
    # 2. Import the correct domain module (weather, nature, etc.)
    module = importlib.import_module(f"transforms.{config['module']}")
    
    # 3. Get the specific function
    func = getattr(module, config['function'])
    
    # 4. Load the required Bronze sources
    sources = {key: spark.table(val) for key, val in config['sources'].items()}
    
    # 5. Execute transformation
    df_silver = func(sources, config.get('params', {}))
    
    # 6. Save to Silver
    df_silver.write.format("delta").mode("overwrite").saveAsTable(config['target_table'])