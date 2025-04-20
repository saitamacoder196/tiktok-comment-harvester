# app/config/db_init.py
import os
from pathlib import Path
import logging
from app.data.database import get_db_connector
from app.config.database_config import get_database_config

logger = logging.getLogger(__name__)

def init_database_schema():
    """
    Khởi tạo schema database từ script SQL
    
    Returns:
        bool: True nếu thành công, False nếu thất bại
    """
    # Lấy đường dẫn đến script SQL
    script_path = Path(__file__).parent.parent / "scripts" / "db_schema.sql"
    
    if not script_path.exists():
        logger.error(f"Không tìm thấy file script SQL: {script_path}")
        return False
    
    # Lấy cấu hình database
    db_config = get_database_config()
    
    # Kết nối đến database
    db = get_db_connector(db_config)
    
    try:
        # Kết nối đến database
        if not db.connect_to_database():
            logger.error("Không thể kết nối đến database.")
            return False
        
        # Đọc nội dung file SQL
        with open(script_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        # Thực thi script
        db.cursor.execute(sql_script)
        db.conn.commit()
        
        logger.info(f"Đã khởi tạo schema database thành công từ script: {script_path}")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo schema database: {e}")
        return False
    finally:
        # Đóng kết nối
        if db:
            db.close()