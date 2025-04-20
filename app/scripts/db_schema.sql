-- app/scripts/db_schema.sql
-- Script tạo bảng cho TikTok Comment Harvester Database

-- Tạo bảng videos để lưu thông tin video
CREATE TABLE IF NOT EXISTS videos (
    video_id VARCHAR(255) PRIMARY KEY,
    video_url TEXT NOT NULL,
    author VARCHAR(255),
    title TEXT,
    description TEXT,
    views_count BIGINT,
    likes_count BIGINT,
    shares_count BIGINT,
    comments_count BIGINT,
    post_time VARCHAR(255),
    music_name TEXT,
    tags TEXT[],
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo bảng comments để lưu thông tin bình luận
CREATE TABLE IF NOT EXISTS comments (
    comment_id SERIAL PRIMARY KEY,
    video_id VARCHAR(255) REFERENCES videos(video_id) ON DELETE CASCADE,
    username VARCHAR(255) NOT NULL,
    comment_text TEXT,
    likes INTEGER DEFAULT 0,
    comment_time VARCHAR(255),
    replies_count INTEGER DEFAULT 0,
    is_reply BOOLEAN DEFAULT FALSE,
    parent_comment_id INTEGER,
    sentiment VARCHAR(20),
    hashtags TEXT[],
    crawled_at TIMESTAMP
);

-- Tạo bảng search_queries để lưu lịch sử tìm kiếm
CREATE TABLE IF NOT EXISTS search_queries (
    query_id SERIAL PRIMARY KEY,
    keyword TEXT NOT NULL,
    results_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo bảng search_results để lưu kết quả tìm kiếm
CREATE TABLE IF NOT EXISTS search_results (
    result_id SERIAL PRIMARY KEY,
    query_id INTEGER REFERENCES search_queries(query_id) ON DELETE CASCADE,
    video_id VARCHAR(255) REFERENCES videos(video_id) ON DELETE CASCADE,
    rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tạo index để tối ưu truy vấn
CREATE INDEX IF NOT EXISTS idx_comments_video_id ON comments(video_id);
CREATE INDEX IF NOT EXISTS idx_comments_username ON comments(username);
CREATE INDEX IF NOT EXISTS idx_comments_is_reply ON comments(is_reply);
CREATE INDEX IF NOT EXISTS idx_search_results_query_id ON search_results(query_id);
CREATE INDEX IF NOT EXISTS idx_search_results_video_id ON search_results(video_id);

-- Tạo VIEW để dễ dàng truy vấn thống kê
CREATE OR REPLACE VIEW video_stats AS
SELECT 
    v.video_id,
    v.author,
    v.title,
    COUNT(c.comment_id) AS total_comments,
    COUNT(DISTINCT c.username) AS unique_users,
    AVG(c.likes) AS avg_likes_per_comment,
    SUM(CASE WHEN c.is_reply = false THEN 1 ELSE 0 END) AS main_comments,
    SUM(CASE WHEN c.is_reply = true THEN 1 ELSE 0 END) AS replies
FROM 
    videos v
LEFT JOIN 
    comments c ON v.video_id = c.video_id
GROUP BY 
    v.video_id, v.author, v.title;

-- Tạo VIEW cho phân tích cảm xúc
CREATE OR REPLACE VIEW sentiment_analysis AS
SELECT 
    v.video_id,
    v.author,
    v.title,
    COUNT(c.comment_id) AS total_comments,
    SUM(CASE WHEN c.sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_comments,
    SUM(CASE WHEN c.sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_comments,
    SUM(CASE WHEN c.sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_comments,
    ROUND(
        (SUM(CASE WHEN c.sentiment = 'positive' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(c.comment_id), 0)) * 100
    ) AS positive_percentage,
    ROUND(
        (SUM(CASE WHEN c.sentiment = 'negative' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(c.comment_id), 0)) * 100
    ) AS negative_percentage
FROM 
    videos v
LEFT JOIN 
    comments c ON v.video_id = c.video_id
GROUP BY 
    v.video_id, v.author, v.title;