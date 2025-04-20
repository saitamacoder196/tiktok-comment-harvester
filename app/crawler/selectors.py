"""
Tập hợp các XPATH selector để trích xuất dữ liệu từ TikTok.
Các selector này cập nhật cho cấu trúc HTML mới của TikTok (2025).
"""

COMMENT_SELECTORS = {
    # Container chứa tất cả comments
    "container": "//div[contains(@class, 'DivCommentListContainer')]",
    
    # Comment icon để mở tab bình luận
    "comment_icon": "//span[@data-e2e='comment-icon']",
    
    # Selector cho các tab
    "comments_tab": "//button[contains(@class, 'TUXTabBar-itemTitle') and @id='comments']",
    
    # Comment count (tổng số bình luận)
    "comment_count": "//div[contains(@class, 'DivCommentCountContainer')]/span",
    
    # Selector cho từng comment item
    "comment_item": "//div[contains(@class, 'DivCommentItemWrapper')]",
    
    # Username của người comment
    "username": ".//div[@data-e2e='comment-username-1']//p",
    
    # Username của người reply
    "reply_username": ".//div[@data-e2e='comment-username-2']//p",
    
    # Nội dung comment cấp 1
    "comment_text": ".//span[@data-e2e='comment-level-1']/p",
    
    # Nội dung comment cấp 2 (replies)
    "reply_text": ".//span[@data-e2e='comment-level-2']/p",
    
    # Số lượng likes của comment
    "likes": ".//div[contains(@class, 'DivLikeContainer')]/span",
    
    # Thời gian comment
    "time": ".//div[contains(@class, 'DivCommentSubContentWrapper')]/span[1]",
    
    # Nút trả lời
    "reply_button": ".//span[@data-e2e='comment-reply-1']",
    
    # Container replies
    "replies_container": ".//div[contains(@class, 'DivReplyContainer')]",
    
    # Nút xem thêm replies
    "view_replies": ".//div[contains(@class, 'DivViewRepliesContainer')]",
    
    # Reply items
    "reply_items": ".//div[contains(@class, 'DivReplyContainer')]//div[contains(@class, 'DivCommentItemWrapper')]",
}

VIDEO_SELECTORS = {
    # Tiêu đề video
    "title": "//h4[contains(@class, 'video-title') or contains(@data-e2e, 'video-title')] | //h1[contains(@class, 'video-title')]",
    
    # Tên tài khoản đăng video
    "author": "//a[contains(@class, 'author-uniqueId') or contains(@data-e2e, 'video-author')] | //h3[contains(@class, 'author-uniqueId')]",
    
    # Mô tả video
    "description": "//div[contains(@class, 'video-desc') or contains(@data-e2e, 'video-description')]",
    
    # Số lượng likes
    "likes": "//strong[@data-e2e='like-count'] | //span[@data-e2e='like-count']",
    
    # Số lượng comments
    "comments_count": "//strong[@data-e2e='comment-count'] | //span[@data-e2e='comment-count']",
    
    # Số lượng shares
    "shares": "//strong[@data-e2e='share-count'] | //span[@data-e2e='share-count']",
    
    # Thời gian đăng video
    "post_time": "//span[contains(@class, 'uploadTime') or contains(@data-e2e, 'video-post-time')]",
}