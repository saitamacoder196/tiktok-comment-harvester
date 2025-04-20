import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
import time
from app.data.database import get_db_connector
from app.config.database_config import get_database_config

def render_database_view_page():
    """
    Hiển thị trang xem và quản lý dữ liệu PostgreSQL
    """
    st.header("🐘 PostgreSQL Database")
    
    # Kiểm tra xem tính năng database có được bật không
    db_config = get_database_config()
    
    if not db_config.get("db_enabled", False):
        st.warning("Tính năng database chưa được kích hoạt. Vui lòng kích hoạt trong trang Cài đặt.")
        
        with st.expander("Hướng dẫn kích hoạt PostgreSQL", expanded=True):
            st.markdown("""
            ### Kích hoạt tính năng PostgreSQL:
            
            1. Cài đặt PostgreSQL nếu bạn chưa cài đặt
            2. Đi đến trang **Cài đặt** > tab **Database**
            3. Chọn "Kích hoạt tính năng database"
            4. Nhập thông tin kết nối PostgreSQL
            5. Bấm "Kiểm tra kết nối" để xác nhận kết nối hoạt động
            6. Bấm "Thiết lập database" để tạo database và các bảng cần thiết
            7. Lưu cài đặt
            """)
        
        if st.button("Đi đến trang Cài đặt"):
            st.session_state['page'] = 'Settings'
            st.rerun()
            
        return
    
    # Kết nối đến database
    db = get_db_connector(db_config)
    
    try:
        if not db.connect_to_database():
            st.error("Không thể kết nối đến PostgreSQL database. Vui lòng kiểm tra lại cài đặt.")
            
            if st.button("Đi đến trang Cài đặt"):
                st.session_state['page'] = 'Settings'
                st.rerun()
                
            return
        
        # Lấy thống kê từ database
        stats = db.get_database_stats()
        
        # Hiển thị thống kê
        st.subheader("📊 Thống kê Database")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Số lượng video", stats.get('videos_count', 0))
        
        with col2:
            st.metric("Số lượng bình luận", stats.get('comments_count', 0))
        
        with col3:
            st.metric("Số người dùng", stats.get('unique_users', 0))
        
        # Hiển thị thông tin video có nhiều bình luận nhất
        if 'most_commented_video' in stats and stats['most_commented_video']:
            st.subheader("Video có nhiều bình luận nhất")
            most_commented = stats['most_commented_video']
            st.markdown(f"""
            **Video ID:** {most_commented['video_id']}  
            **URL:** [{most_commented['video_url']}]({most_commented['video_url']})  
            **Số bình luận:** {most_commented['comment_count']}
            """)
        
        # Hiển thị top người dùng tích cực nhất
        if 'top_users' in stats and stats['top_users']:
            st.subheader("Top người dùng tích cực nhất")
            
            # Tạo DataFrame từ top_users
            top_users_df = pd.DataFrame(stats['top_users'])
            
            # Hiển thị biểu đồ
            fig = px.bar(
                top_users_df, 
                x='username', 
                y='comment_count',
                labels={'username': 'Người dùng', 'comment_count': 'Số bình luận'},
                title="Top người dùng tích cực nhất"
            )
            st.plotly_chart(fig)
        
        # Thêm tabs để xem dữ liệu cụ thể
        tab1, tab2 = st.tabs(["Bình luận", "Videos"])
        
        with tab1:
            st.subheader("Dữ liệu bình luận")
            
            # Bộ lọc
            col1, col2, col3 = st.columns(3)
            
            with col1:
                search_username = st.text_input("Tìm kiếm theo username")
            
            with col2:
                # Lấy danh sách video_id từ database
                db.cursor.execute("SELECT video_id FROM videos")
                video_ids = [row[0] for row in db.cursor.fetchall()]
                
                # Dropdown chọn video_id
                filter_video_id = st.selectbox(
                    "Lọc theo video ID",
                    options=["Tất cả"] + video_ids,
                    index=0
                )
                
                # Chuyển đổi selection thành giá trị filter
                selected_video_id = None if filter_video_id == "Tất cả" else filter_video_id
            
            with col3:
                # Số lượng kết quả hiển thị
                limit = st.number_input("Số lượng kết quả", min_value=10, max_value=1000, value=100, step=10)
            
            # Nút tìm kiếm
            search_button = st.button("🔍 Tìm kiếm", use_container_width=False)
            
            if search_button or 'last_comment_search' in st.session_state:
                # Lưu trạng thái tìm kiếm
                if search_button:
                    st.session_state['last_comment_search'] = {
                        'username': search_username,
                        'video_id': selected_video_id,
                        'limit': limit
                    }
                
                # Lấy tham số từ session state nếu cần
                search_params = st.session_state.get('last_comment_search', {})
                username = search_params.get('username', search_username)
                video_id = search_params.get('video_id', selected_video_id)
                query_limit = search_params.get('limit', limit)
                
                # Truy vấn bình luận
                with st.spinner("Đang tải dữ liệu..."):
                    comments = db.query_comments(
                        video_id=video_id,
                        username=username,
                        limit=query_limit
                    )
                
                # Hiển thị kết quả
                if comments:
                    # Chuyển đổi sang DataFrame
                    comments_df = pd.DataFrame(comments)
                    
                    # Định dạng thời gian
                    if 'crawled_at' in comments_df.columns:
                        comments_df['crawled_at'] = pd.to_datetime(comments_df['crawled_at'])
                    
                    # Hiển thị DataFrame
                    st.dataframe(comments_df)
                    
                    # Tùy chọn xuất dữ liệu
                    if st.button("📥 Xuất kết quả tìm kiếm"):
                        export_format = st.radio(
                            "Chọn định dạng xuất:",
                            options=["CSV", "Excel", "JSON"]
                        )
                        
                        # Tạo thư mục export nếu chưa tồn tại
                        export_dir = Path("data/exports")
                        export_dir.mkdir(parents=True, exist_ok=True)
                        
                        # Tên file xuất
                        timestamp = time.strftime("%Y%m%d_%H%M%S")
                        
                        if export_format == "CSV":
                            export_path = export_dir / f"comments_export_{timestamp}.csv"
                            if export_to_csv(comments_df, export_path):
                                st.success(f"Đã xuất dữ liệu sang: {export_path}")
                        elif export_format == "Excel":
                            from app.data.exporter import export_to_excel
                            export_path = export_dir / f"comments_export_{timestamp}.xlsx"
                            if export_to_excel(comments_df, export_path):
                                st.success(f"Đã xuất dữ liệu sang: {export_path}")
                        else:  # JSON
                            from app.data.exporter import export_to_json
                            export_path = export_dir / f"comments_export_{timestamp}.json"
                            if export_to_json(comments_df, export_path):
                                st.success(f"Đã xuất dữ liệu sang: {export_path}")
                else:
                    st.info("Không tìm thấy bình luận nào thỏa mãn điều kiện tìm kiếm.")
        
        with tab2:
            st.subheader("Dữ liệu video")
            
            # Truy vấn danh sách video
            db.cursor.execute("""
            SELECT v.video_id, v.video_url, v.author, COUNT(c.comment_id) as comment_count, v.crawled_at
            FROM videos v
            LEFT JOIN comments c ON v.video_id = c.video_id
            GROUP BY v.video_id, v.video_url, v.author, v.crawled_at
            ORDER BY v.crawled_at DESC
            """)
            
            # Lấy kết quả và tên cột
            columns = [desc[0] for desc in db.cursor.description]
            videos = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
            
            if videos:
                # Chuyển đổi sang DataFrame
                videos_df = pd.DataFrame(videos)
                
                # Định dạng thời gian
                if 'crawled_at' in videos_df.columns:
                    videos_df['crawled_at'] = pd.to_datetime(videos_df['crawled_at'])
                
                # Hiển thị DataFrame
                st.dataframe(videos_df)
                
                # Hiển thị thông tin chi tiết khi chọn video
                st.subheader("Thông tin chi tiết video")
                selected_video_id = st.selectbox(
                    "Chọn video để xem chi tiết",
                    options=[v['video_id'] for v in videos]
                )
                
                if selected_video_id:
                    # Lấy thông tin video
                    selected_video = next((v for v in videos if v['video_id'] == selected_video_id), None)
                    
                    if selected_video:
                        st.markdown(f"""
                        **Video ID:** {selected_video['video_id']}  
                        **URL:** [{selected_video['video_url']}]({selected_video['video_url']})  
                        **Tác giả:** {selected_video['author'] or 'Không xác định'}  
                        **Số bình luận:** {selected_video['comment_count']}  
                        **Thời gian thu thập:** {selected_video['crawled_at']}
                        """)
                        
                        # Nút xem bình luận
                        if st.button("Xem bình luận của video này"):
                            st.session_state['last_comment_search'] = {
                                'username': '',
                                'video_id': selected_video_id,
                                'limit': 100
                            }
                            st.rerun()
            else:
                st.info("Chưa có dữ liệu video nào trong database.")
        
        # Thêm tính năng quản lý database
        st.markdown("---")
        st.subheader("⚙️ Quản lý Database")
        
        with st.expander("Tùy chọn nâng cao"):
            st.warning("Các thao tác dưới đây có thể xóa dữ liệu vĩnh viễn. Hãy cẩn thận!")
            
            # Nút xóa tất cả bình luận
            if st.button("🗑️ Xóa tất cả bình luận", use_container_width=False):
                confirm = st.checkbox("Tôi hiểu rằng hành động này không thể hoàn tác và muốn xóa tất cả bình luận")
                
                if confirm:
                    try:
                        db.cursor.execute("DELETE FROM comments")
                        st.success("Đã xóa tất cả bình luận!")
                    except Exception as e:
                        st.error(f"Lỗi khi xóa bình luận: {str(e)}")
            
            # Nút xóa toàn bộ dữ liệu
            if st.button("⚠️ Xóa toàn bộ dữ liệu", use_container_width=False):
                confirm = st.checkbox("Tôi hiểu rằng hành động này không thể hoàn tác và muốn xóa tất cả dữ liệu", key="confirm_delete_all")
                
                if confirm:
                    try:
                        # Xóa comments trước vì có foreign key
                        db.cursor.execute("DELETE FROM comments")
                        db.cursor.execute("DELETE FROM videos")
                        st.success("Đã xóa toàn bộ dữ liệu!")
                    except Exception as e:
                        st.error(f"Lỗi khi xóa dữ liệu: {str(e)}")
            
            # Nút thiết lập lại database
            if st.button("🔄 Thiết lập lại database", use_container_width=False):
                confirm = st.checkbox("Tôi hiểu rằng hành động này không thể hoàn tác và muốn thiết lập lại database", key="confirm_reset_db")
                
                if confirm:
                    try:
                        # Xóa các bảng cũ
                        db.cursor.execute("DROP TABLE IF EXISTS comments")
                        db.cursor.execute("DROP TABLE IF EXISTS videos")
                        
                        # Tạo lại các bảng
                        if db.create_tables():
                            st.success("Đã thiết lập lại database thành công!")
                        else:
                            st.error("Lỗi khi thiết lập lại database.")
                    except Exception as e:
                        st.error(f"Lỗi khi thiết lập lại database: {str(e)}")
    
    except Exception as e:
        st.error(f"Lỗi khi tương tác với database: {str(e)}")
    
    finally:
        # Đóng kết nối database
        db.close()