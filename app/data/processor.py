import pandas as pd
import numpy as np
from typing import Dict, Any
import re
from app.utils.helpers import format_number

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Làm sạch dữ liệu bình luận
    
    Args:
        df (DataFrame): DataFrame chứa dữ liệu bình luận
        
    Returns:
        DataFrame: DataFrame đã được làm sạch
    """
    # Tạo bản sao để tránh thay đổi dữ liệu gốc
    df_clean = df.copy()
    
    # Chuyển đổi cột likes và replies_count từ chuỗi sang số
    if 'likes' in df_clean.columns:
        df_clean['likes_count'] = df_clean['likes'].apply(format_number)
    
    if 'replies_count' in df_clean.columns:
        df_clean['replies_number'] = df_clean['replies_count'].apply(format_number)
    
    # Tính độ dài comment
    if 'comment_text' in df_clean.columns:
        df_clean['comment_length'] = df_clean['comment_text'].apply(lambda x: len(str(x)))
    
    # Loại bỏ khoảng trắng thừa trong username
    if 'username' in df_clean.columns:
        df_clean['username'] = df_clean['username'].apply(lambda x: str(x).strip())
    
    # Chuyển đổi cột thời gian
    if 'comment_time' in df_clean.columns:
        df_clean['comment_time'] = df_clean['comment_time'].apply(lambda x: str(x).strip())
    
    return df_clean

def basic_analysis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Phân tích cơ bản dữ liệu bình luận
    
    Args:
        df (DataFrame): DataFrame chứa dữ liệu bình luận
        
    Returns:
        dict: Kết quả phân tích
    """
    # Làm sạch dữ liệu
    df_clean = clean_data(df)
    
    # Kết quả phân tích
    results = {}
    
    # Số người dùng duy nhất
    results['unique_users'] = df_clean['username'].nunique()
    
    # Độ dài bình luận trung bình
    results['avg_comment_length'] = df_clean['comment_length'].mean()
    
    # Lượt thích trung bình
    if 'likes_count' in df_clean.columns:
        results['avg_likes'] = df_clean['likes_count'].mean()
    else:
        results['avg_likes'] = 0
    
    # Phân phối độ dài bình luận
    comment_length_bins = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500]
    hist, bin_edges = np.histogram(df_clean['comment_length'], bins=comment_length_bins)
    
    # Tạo DataFrame cho phân phối độ dài
    bin_labels = [f"{bin_edges[i]}-{bin_edges[i+1]}" for i in range(len(bin_edges)-1)]
    results['comment_length_dist'] = pd.DataFrame({
        'Độ dài': bin_labels,
        'Số lượng': hist
    }).set_index('Độ dài')
    
    # Top người dùng tích cực nhất
    top_users = df_clean['username'].value_counts().reset_index()
    top_users.columns = ['Người dùng', 'Số bình luận']
    results['top_users'] = top_users.set_index('Người dùng').head(10)
    
    return results

def sentiment_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phân tích cảm xúc cơ bản dựa trên từ khóa
    
    Args:
        df (DataFrame): DataFrame chứa dữ liệu bình luận
        
    Returns:
        DataFrame: DataFrame đã thêm cột sentiment
    """
    # Làm sạch dữ liệu
    df_clean = clean_data(df)
    
    # Từ điển từ khóa tích cực và tiêu cực đơn giản (có thể mở rộng)
    positive_keywords = [
        'hay', 'tốt', 'đẹp', 'thích', 'yêu', 'tuyệt vời', 'xuất sắc', 'tuyệt', 
        'giỏi', 'thú vị', 'ủng hộ', 'tài năng', 'đỉnh', 'chất', 'vip', 'pro',
        'hahaha', 'hihi', 'xinh', 'dễ thương', 'đáng yêu', 'cool', 'thích thú',
        '❤️', '😍', '👍', '👏', '🔥', '💯', '👌', '😊'
    ]
    
    negative_keywords = [
        'tệ', 'kém', 'dở', 'ghét', 'chán', 'buồn', 'thất vọng', 'không thích',
        'tào lao', 'vô duyên', 'nhảm', 'xấu', 'dở tệ', 'phí', 'dỡ', 'lừa đảo',
        'scam', 'cùi', 'gà', 'dở hơi', 'phèn', 'cay', 'toxic',
        '👎', '😒', '😡', '🤮', '💩', '😤', '🤬'
    ]
    
    # Function để xác định cảm xúc
    def determine_sentiment(text):
        if not isinstance(text, str):
            return 'neutral'
            
        text = text.lower()
        pos_count = sum(1 for keyword in positive_keywords if keyword.lower() in text)
        neg_count = sum(1 for keyword in negative_keywords if keyword.lower() in text)
        
        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'
    
    # Thêm cột cảm xúc
    df_clean['sentiment'] = df_clean['comment_text'].apply(determine_sentiment)
    
    return df_clean

def extract_hashtags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trích xuất hashtags từ bình luận
    
    Args:
        df (DataFrame): DataFrame chứa dữ liệu bình luận
        
    Returns:
        DataFrame: DataFrame đã thêm cột hashtags
    """
    # Làm sạch dữ liệu
    df_clean = clean_data(df)
    
    # Function để trích xuất hashtags
    def extract_tags(text):
        if not isinstance(text, str):
            return []
            
        # Tìm các hashtag với regex
        hashtags = re.findall(r'#(\w+)', text)
        return hashtags
    
    # Thêm cột hashtags
    df_clean['hashtags'] = df_clean['comment_text'].apply(extract_tags)
    
    return df_clean

def get_popular_hashtags(df: pd.DataFrame, top_n: int = 10) -> pd.Series:
    """
    Lấy các hashtag phổ biến nhất
    
    Args:
        df (DataFrame): DataFrame đã có cột hashtags
        top_n (int): Số lượng hashtag cần trả về
        
    Returns:
        Series: Series chứa số lượng các hashtag phổ biến nhất
    """
    # Kiểm tra nếu cột hashtags chưa tồn tại
    if 'hashtags' not in df.columns:
        df = extract_hashtags(df)
    
    # Làm phẳng danh sách hashtags
    all_hashtags = [tag for tags in df['hashtags'] for tag in tags]
    
    # Đếm tần suất
    hashtag_counts = pd.Series(all_hashtags).value_counts().head(top_n)
    
    return hashtag_counts