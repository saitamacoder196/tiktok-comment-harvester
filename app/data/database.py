import os
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class PostgresConnector:
    """
    Kết nối và tương tác với PostgreSQL database
    """
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 user: str = "postgres", password: str = None, 
                 database: str = "tiktok_data"):
        """
        Khởi tạo kết nối PostgreSQL
        
        Args:
            host (str): Máy chủ PostgreSQL
            port (int): Cổng PostgreSQL
            user (str): Tên người dùng
            password (str): Mật khẩu
            database (str): Tên database
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """
        Kết nối đến PostgreSQL server
        
        Returns:
            bool: True nếu kết nối thành công, False nếu thất bại
        """
        try:
            # Kết nối tới PostgreSQL server
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            
            logger.info(f"Đã kết nối thành công đến PostgreSQL server: {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi kết nối đến PostgreSQL server: {e}")
            return False
    
    def close(self):
        """Đóng kết nối"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Đã đóng kết nối PostgreSQL")
    
    def create_database(self) -> bool:
        """
        Tạo database nếu chưa tồn tại
        
        Returns:
            bool: True nếu tạo thành công hoặc đã tồn tại, False nếu thất bại
        """
        try:
            # Kiểm tra xem database đã tồn tại chưa
            self.cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.database,))
            exists = self.cursor.fetchone()
            
            if not exists:
                # Tạo database mới
                self.cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.database)))
                logger.info(f"Đã tạo database: {self.database}")
            else:
                logger.info(f"Database {self.database} đã tồn tại")
                
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tạo database: {e}")
            return False
    
    def connect_to_database(self) -> bool:
        """
        Kết nối đến database cụ thể
        
        Returns:
            bool: True nếu kết nối thành công, False nếu thất bại
        """
        try:
            # Đóng kết nối hiện tại
            self.close()
            
            # Kết nối đến database cụ thể
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.conn.cursor()
            
            logger.info(f"Đã kết nối đến database: {self.database}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi kết nối đến database: {e}")
            return False
    
    def create_tables(self) -> bool:
        """
        Tạo các bảng cần thiết trong database
        
        Returns:
            bool: True nếu tạo thành công, False nếu thất bại
        """
        try:
            # Tạo bảng videos để lưu thông tin video
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                video_url TEXT NOT NULL,
                author VARCHAR(255),
                title TEXT,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # Tạo bảng comments để lưu thông tin bình luận
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                comment_id SERIAL PRIMARY KEY,
                video_id VARCHAR(255) REFERENCES videos(video_id),
                username VARCHAR(255) NOT NULL,
                comment_text TEXT,
                likes INTEGER DEFAULT 0,
                comment_time VARCHAR(255),
                replies_count INTEGER DEFAULT 0,
                is_reply BOOLEAN DEFAULT FALSE,
                parent_comment_id INTEGER,
                crawled_at TIMESTAMP
            )
            """)
            
            logger.info("Đã tạo các bảng cần thiết")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tạo bảng: {e}")
            return False
    
    def insert_video(self, video_id: str, video_url: str, author: str = None, title: str = None) -> bool:
        """
        Thêm hoặc cập nhật thông tin video
        
        Args:
            video_id (str): ID của video
            video_url (str): URL của video
            author (str): Tên tác giả
            title (str): Tiêu đề video
            
        Returns:
            bool: True nếu thêm/cập nhật thành công, False nếu thất bại
        """
        try:
            # Kiểm tra xem video đã tồn tại chưa
            self.cursor.execute("SELECT 1 FROM videos WHERE video_id = %s", (video_id,))
            exists = self.cursor.fetchone()
            
            if exists:
                # Cập nhật thông tin video
                self.cursor.execute("""
                UPDATE videos SET 
                    video_url = %s,
                    author = %s,
                    title = %s,
                    crawled_at = CURRENT_TIMESTAMP
                WHERE video_id = %s
                """, (video_url, author, title, video_id))
                logger.info(f"Đã cập nhật thông tin video: {video_id}")
            else:
                # Thêm video mới
                self.cursor.execute("""
                INSERT INTO videos (video_id, video_url, author, title)
                VALUES (%s, %s, %s, %s)
                """, (video_id, video_url, author, title))
                logger.info(f"Đã thêm video mới: {video_id}")
                
            return True
        except Exception as e:
            logger.error(f"Lỗi khi thêm/cập nhật video: {e}")
            return False
    
    def insert_comments(self, video_id: str, comments_data: List[Dict[str, Any]]) -> bool:
        """
        Thêm các bình luận vào database
        
        Args:
            video_id (str): ID của video
            comments_data (list): Danh sách các bình luận
            
        Returns:
            bool: True nếu thêm thành công, False nếu thất bại
        """
        try:
            # Ánh xạ username với comment_id để theo dõi parent_comment
            username_to_id = {}
            
            for comment in comments_data:
                is_reply = comment.get('is_reply', False)
                parent_comment_id = None
                
                # Nếu là reply, tìm parent_comment_id
                if is_reply and 'parent_comment_username' in comment:
                    parent_username = comment.get('parent_comment_username')
                    if parent_username in username_to_id:
                        parent_comment_id = username_to_id[parent_username]
                
                # Chuyển đổi chuỗi likes sang số nguyên
                likes_text = comment.get('likes', '0')
                try:
                    # Xử lý các chuỗi như "1.2K", "4.5M"
                    if 'K' in likes_text:
                        likes = int(float(likes_text.replace('K', '')) * 1000)
                    elif 'M' in likes_text:
                        likes = int(float(likes_text.replace('M', '')) * 1000000)
                    else:
                        likes = int(likes_text) if likes_text.isdigit() else 0
                except ValueError:
                    likes = 0
                
                # Chuyển đổi chuỗi replies_count sang số nguyên
                replies_count = int(comment.get('replies_count', '0')) if comment.get('replies_count', '0').isdigit() else 0
                
                # Thêm bình luận vào database
                self.cursor.execute("""
                INSERT INTO comments 
                (video_id, username, comment_text, likes, comment_time, replies_count, is_reply, parent_comment_id, crawled_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING comment_id
                """, (
                    video_id,
                    comment.get('username', ''),
                    comment.get('comment_text', ''),
                    likes,
                    comment.get('comment_time', ''),
                    replies_count,
                    is_reply,
                    parent_comment_id,
                    comment.get('crawled_at', None)
                ))
                
                # Lưu comment_id với username để theo dõi parent_comment
                comment_id = self.cursor.fetchone()[0]
                username_to_id[comment.get('username', '')] = comment_id
            
            logger.info(f"Đã thêm {len(comments_data)} bình luận cho video: {video_id}")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi thêm bình luận: {e}")
            return False
    
    def export_dataframe_to_postgres(self, df: pd.DataFrame, video_id: str, video_url: str) -> bool:
        """
        Xuất DataFrame vào PostgreSQL
        
        Args:
            df (DataFrame): DataFrame chứa dữ liệu bình luận
            video_id (str): ID của video
            video_url (str): URL của video
            
        Returns:
            bool: True nếu xuất thành công, False nếu thất bại
        """
        try:
            # Đảm bảo đã kết nối đến database
            if not self.conn or not self.cursor:
                logger.error("Chưa kết nối đến database")
                return False
            
            # Thêm thông tin video
            self.insert_video(video_id, video_url)
            
            # Chuyển đổi DataFrame thành list của dict
            comments_data = df.to_dict('records')
            
            # Thêm bình luận vào database
            return self.insert_comments(video_id, comments_data)
        except Exception as e:
            logger.error(f"Lỗi khi xuất DataFrame vào PostgreSQL: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Lấy thống kê từ database
        
        Returns:
            dict: Thông tin thống kê
        """
        stats = {}
        
        try:
            # Số lượng video
            self.cursor.execute("SELECT COUNT(*) FROM videos")
            stats['videos_count'] = self.cursor.fetchone()[0]
            
            # Số lượng bình luận
            self.cursor.execute("SELECT COUNT(*) FROM comments")
            stats['comments_count'] = self.cursor.fetchone()[0]
            
            # Số lượng người dùng duy nhất
            self.cursor.execute("SELECT COUNT(DISTINCT username) FROM comments")
            stats['unique_users'] = self.cursor.fetchone()[0]
            
            # Video có nhiều bình luận nhất
            self.cursor.execute("""
            SELECT v.video_id, v.video_url, COUNT(c.comment_id) as comment_count
            FROM videos v
            JOIN comments c ON v.video_id = c.video_id
            GROUP BY v.video_id, v.video_url
            ORDER BY comment_count DESC
            LIMIT 1
            """)
            result = self.cursor.fetchone()
            if result:
                stats['most_commented_video'] = {
                    'video_id': result[0],
                    'video_url': result[1],
                    'comment_count': result[2]
                }
            
            # Người dùng tích cực nhất
            self.cursor.execute("""
            SELECT username, COUNT(*) as comment_count
            FROM comments
            GROUP BY username
            ORDER BY comment_count DESC
            LIMIT 10
            """)
            stats['top_users'] = [{'username': row[0], 'comment_count': row[1]} for row in self.cursor.fetchall()]
            
            return stats
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin thống kê: {e}")
            return stats
    
    def query_comments(self, video_id: str = None, username: str = None, 
                       limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Truy vấn bình luận với các bộ lọc
        
        Args:
            video_id (str): ID video cần lọc
            username (str): Tên người dùng cần lọc
            limit (int): Số lượng kết quả tối đa
            offset (int): Vị trí bắt đầu
            
        Returns:
            list: Danh sách các bình luận thỏa mãn điều kiện
        """
        try:
            query = "SELECT * FROM comments WHERE 1=1"
            params = []
            
            if video_id:
                query += " AND video_id = %s"
                params.append(video_id)
            
            if username:
                query += " AND username LIKE %s"
                params.append(f"%{username}%")
            
            query += " ORDER BY crawled_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            self.cursor.execute(query, tuple(params))
            columns = [desc[0] for desc in self.cursor.description]
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
            return results
        except Exception as e:
            logger.error(f"Lỗi khi truy vấn bình luận: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Kiểm tra kết nối PostgreSQL
        
        Returns:
            bool: True nếu kết nối thành công, False nếu thất bại
        """
        try:
            if not self.conn or not self.cursor:
                return False
                
            self.cursor.execute("SELECT 1")
            return self.cursor.fetchone()[0] == 1
        except Exception:
            return False


def get_db_connector(config: Dict[str, Any] = None) -> PostgresConnector:
    """
    Lấy đối tượng kết nối database dựa trên cấu hình
    
    Args:
        config (dict): Cấu hình database
        
    Returns:
        PostgresConnector: Đối tượng kết nối database
    """
    if not config:
        config = {}
    
    host = config.get('db_host', 'localhost')
    port = config.get('db_port', 5432)
    user = config.get('db_user', 'postgres')
    password = config.get('db_password', '')
    database = config.get('db_name', 'tiktok_data')
    
    db = PostgresConnector(
        host=host, 
        port=port, 
        user=user, 
        password=password, 
        database=database
    )
    
    return db


def setup_database(config: Dict[str, Any] = None) -> bool:
    """
    Thiết lập database ban đầu
    
    Args:
        config (dict): Cấu hình database
        
    Returns:
        bool: True nếu thiết lập thành công, False nếu thất bại
    """
    db = get_db_connector(config)
    
    try:
        # Kết nối đến PostgreSQL server
        if not db.connect():
            return False
        
        # Tạo database nếu chưa tồn tại
        if not db.create_database():
            return False
        
        # Kết nối đến database cụ thể
        if not db.connect_to_database():
            return False
        
        # Tạo các bảng cần thiết
        if not db.create_tables():
            return False
        
        return True
    except Exception as e:
        logger.error(f"Lỗi khi thiết lập database: {e}")
        return False
    finally:
        db.close()