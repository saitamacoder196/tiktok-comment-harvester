# app/config/env_config.py
import os
from pathlib import Path
from dotenv import load_dotenv

def load_env_config():
    """
    Tải cấu hình từ file .env
    
    Returns:
        dict: Cấu hình từ file .env
    """
    # Tìm file .env từ thư mục hiện tại hoặc thư mục gốc dự án
    env_path = Path('.env')
    root_env_path = Path(__file__).parent.parent.parent / '.env'
    
    if env_path.exists():
        load_dotenv(env_path)
    elif root_env_path.exists():
        load_dotenv(root_env_path)
    else:
        # Nếu không tìm thấy file .env, vẫn load env từ biến môi trường
        load_dotenv()
    
    # Đọc các biến môi trường cho database
    db_config = {
        "db_name": os.getenv("DB_NAME", "tiktok_data"),
        "db_user": os.getenv("DB_USER", "postgres"),
        "db_password": os.getenv("DB_PASSWORD", ""),
        "db_host": os.getenv("DB_HOST", "localhost"),
        "db_port": int(os.getenv("DB_PORT", "5432")),
    }
    
    return db_config