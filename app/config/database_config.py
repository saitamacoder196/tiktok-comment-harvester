# app/config/database_config.py
import json
from pathlib import Path
import os
from app.config.env_config import load_env_config

def get_database_config():
    """
    Lấy cấu hình database từ file cấu hình
    
    Returns:
        dict: Cấu hình database
    """
    config_dir = Path("app/config")
    config_file = config_dir / "settings.json"
    
    # Đảm bảo thư mục cấu hình tồn tại
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Tải cấu hình từ file .env
    env_config = load_env_config()
    
    # Mặc định config
    default_config = {
        "db_enabled": False,
        "db_host": env_config.get("db_host", "localhost"),
        "db_port": env_config.get("db_port", 5432),
        "db_user": env_config.get("db_user", "postgres"),
        "db_password": env_config.get("db_password", ""),
        "db_name": env_config.get("db_name", "tiktok_data"),
        "auto_save_to_db": False
    }
    
    # Kiểm tra xem file cấu hình đã tồn tại chưa
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
                # Lấy cấu hình database nếu có
                if not any(key.startswith('db_') for key in config.keys()):
                    # Thêm cấu hình database mặc định nếu chưa có
                    config.update(default_config)
                    
                    # Lưu cấu hình mới
                    with open(config_file, 'w', encoding='utf-8') as fw:
                        json.dump(config, fw, indent=4, ensure_ascii=False)
                
                # Ưu tiên sử dụng giá trị từ .env nếu có
                for key in ["db_host", "db_port", "db_user", "db_password", "db_name"]:
                    if env_config.get(key.replace("db_", "")) is not None:
                        config[key] = env_config.get(key.replace("db_", ""))
                
                return {
                    "db_enabled": config.get("db_enabled", default_config["db_enabled"]),
                    "db_host": config.get("db_host", default_config["db_host"]),
                    "db_port": config.get("db_port", default_config["db_port"]),
                    "db_user": config.get("db_user", default_config["db_user"]),
                    "db_password": config.get("db_password", default_config["db_password"]),
                    "db_name": config.get("db_name", default_config["db_name"]),
                    "auto_save_to_db": config.get("auto_save_to_db", default_config["auto_save_to_db"])
                }
        except Exception as e:
            print(f"Lỗi khi đọc file cấu hình database: {e}")
    
    # Trả về cấu hình mặc định
    return default_config

def save_database_config(config):
    """
    Lưu cấu hình database vào file cấu hình
    
    Args:
        config (dict): Cấu hình database cần lưu
    """
    config_dir = Path("app/config")
    config_file = config_dir / "settings.json"
    
    # Đảm bảo thư mục cấu hình tồn tại
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # Kiểm tra xem file cấu hình đã tồn tại chưa
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                full_config = json.load(f)
                
            # Cập nhật cấu hình database
            for key, value in config.items():
                if key.startswith('db_'):
                    full_config[key] = value
            
            # Lưu cấu hình mới
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(full_config, f, indent=4, ensure_ascii=False)
                
        except Exception as e:
            print(f"Lỗi khi lưu file cấu hình database: {e}")
    else:
        # Tạo file cấu hình mới
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)