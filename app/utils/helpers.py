import re
import logging
from pathlib import Path
import os
from typing import Optional, Union
import urllib.parse

def validate_tiktok_url(url: str) -> bool:
    """
    Kiểm tra xem URL có phải là URL video TikTok hợp lệ không
    
    Args:
        url (str): URL cần kiểm tra
        
    Returns:
        bool: True nếu URL hợp lệ, False nếu không
    """
    # Regex cơ bản để kiểm tra URL TikTok
    tiktok_regex = r'^https?:\/\/(www\.|vm\.|vt\.)?tiktok\.com\/([@a-zA-Z0-9_.]+\/video\/|v\/|@[^\/]+\/|embed\/|)([0-9]+)'
    
    if not url:
        return False
    
    # Kiểm tra URL bằng regex
    match = re.match(tiktok_regex, url)
    
    if match:
        return True
    
    return False

def get_video_id_from_url(url: str) -> Optional[str]:
    """
    Trích xuất ID video từ URL TikTok
    
    Args:
        url (str): URL video TikTok
        
    Returns:
        str or None: ID video nếu tìm thấy, None nếu không
    """
    if not validate_tiktok_url(url):
        return None
    
    # Trích xuất ID video từ URL
    regex = r'tiktok\.com\/(?:@[^\/]+\/video\/|v\/|@[^\/]+\/|embed\/|)([0-9]+)'
    match = re.search(regex, url)
    
    if match:
        return match.group(1)
    
    return None

def get_username_from_url(url: str) -> Optional[str]:
    """
    Trích xuất tên người dùng từ URL TikTok
    
    Args:
        url (str): URL video TikTok
        
    Returns:
        str or None: Tên người dùng nếu tìm thấy, None nếu không
    """
    if not validate_tiktok_url(url):
        return None
    
    # Trích xuất tên người dùng từ URL
    regex = r'tiktok\.com\/(@[^\/]+)'
    match = re.search(regex, url)
    
    if match:
        return match.group(1)
    
    return None

def setup_logger(name: str, log_file: Optional[Union[str, Path]] = None, level=logging.INFO):
    """
    Thiết lập logger
    
    Args:
        name (str): Tên của logger
        log_file (str, optional): Đường dẫn file log
        level (int): Cấp độ log
        
    Returns:
        Logger: Đối tượng logger đã được cấu hình
    """
    # Tạo logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Tạo formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Tạo console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Tạo file handler nếu có chỉ định file log
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

def clean_filename(filename: str) -> str:
    """
    Làm sạch tên file để đảm bảo tính hợp lệ
    
    Args:
        filename (str): Tên file cần làm sạch
        
    Returns:
        str: Tên file đã được làm sạch
    """
    # Thay thế các ký tự không hợp lệ bằng dấu gạch dưới
    invalid_chars = r'[\\/*?:"<>|]'
    cleaned = re.sub(invalid_chars, '_', filename)
    
    # Giới hạn độ dài tên file
    max_length = 240  # Để an toàn cho hầu hết các hệ thống file
    
    if len(cleaned) > max_length:
        extension = os.path.splitext(cleaned)[1]
        cleaned = cleaned[:max_length - len(extension)] + extension
    
    return cleaned

def format_number(num_str: str) -> int:
    """
    Chuyển đổi chuỗi số có định dạng (vd: "1.2K", "4.5M") sang số nguyên
    
    Args:
        num_str (str): Chuỗi số cần chuyển đổi
        
    Returns:
        int: Số nguyên tương ứng
    """
    if not num_str or num_str == "Unknown":
        return 0
    
    # Loại bỏ các ký tự không phải số hoặc dấu chấm
    num_str = num_str.strip()
    
    # Xử lý các hậu tố như K, M, B
    multiplier = 1
    if num_str.endswith('K') or num_str.endswith('k'):
        multiplier = 1000
        num_str = num_str[:-1]
    elif num_str.endswith('M') or num_str.endswith('m'):
        multiplier = 1000000
        num_str = num_str[:-1]
    elif num_str.endswith('B') or num_str.endswith('b'):
        multiplier = 1000000000
        num_str = num_str[:-1]
    
    try:
        # Chuyển đổi phần còn lại thành số
        return int(float(num_str) * multiplier)
    except (ValueError, TypeError):
        return 0