import streamlit as st
import pandas as pd
import time
import os
from pathlib import Path
from datetime import datetime

from app.crawler.tiktok_crawler import TikTokCommentCrawler
from app.utils.helpers import validate_tiktok_url, get_video_id_from_url
from app.data.processor import basic_analysis
from app.data.exporter import export_to_excel, export_to_csv, export_to_json
from app.data.database import get_db_connector
from app.config.database_config import get_database_config

def show_captcha_message():
    st.warning("""
    ### ⚠️ Phát hiện CAPTCHA!
    
    TikTok yêu cầu xác minh CAPTCHA. Vui lòng:
    
    1. Giải CAPTCHA trong cửa sổ trình duyệt đã mở
    2. Kéo thanh trượt để hoàn thành CAPTCHA
    3. Sau khi hoàn thành, quá trình đăng nhập sẽ tự động tiếp tục
    
    Hệ thống sẽ đợi tối đa 60 giây để bạn giải CAPTCHA.
    """)


def render_crawler_page():
    st.header("🕸️ Thu thập bình luận TikTok")
    
    from app.config.database_config import get_database_config
    db_config = get_database_config()
    db_enabled = db_config.get("db_enabled", False)
    auto_save_to_db = db_config.get("auto_save_to_db", False)
    
    # Khởi tạo biến session state để lưu trữ crawler và trạng thái
    if 'tiktok_crawler' not in st.session_state:
        st.session_state['tiktok_crawler'] = None
        st.session_state['logged_in'] = False
        st.session_state['stage'] = 'login'  # Các giai đoạn: login, crawl, completed
    
    # Giai đoạn 1: Đăng nhập TikTok
    if st.session_state['stage'] == 'login':
        st.subheader("🔑 Bước 1: Đăng nhập TikTok")
        
        st.info("""
        Vui lòng đăng nhập vào TikTok để tiếp tục. Đăng nhập giúp truy cập nhiều nội dung hơn và tránh các hạn chế.
        
        **Lưu ý:** 
        - TikTok có thể yêu cầu xác minh CAPTCHA. Trong trường hợp này, bạn sẽ cần giải CAPTCHA thủ công.
        - Trình duyệt sẽ được mở và giữ mở cho đến khi bạn hoàn tất quá trình.
        """)
        
        # Form đăng nhập
        with st.form(key="login_form"):
            login_username = st.text_input("Username hoặc Email", value="", type="default")
            login_password = st.text_input("Password", value="", type="password")
            
            # Tùy chọn đăng nhập
            with st.expander("Tùy chọn đăng nhập"):
                login_timeout = st.slider(
                    "Thời gian chờ đăng nhập tối đa (giây)", 
                    min_value=10, 
                    max_value=120, 
                    value=30, 
                    step=5
                )
                
                headless = st.checkbox("Chế độ headless (chạy ngầm)", value=False)
                st.warning("Không nên sử dụng chế độ headless khi đăng nhập vì có thể cần giải CAPTCHA thủ công.")
            
            # Nút đăng nhập
            login_button = st.form_submit_button(label="Đăng nhập")
        
        # Xử lý đăng nhập
        if login_button:
            if not login_username or not login_password:
                st.error("Vui lòng nhập đầy đủ thông tin đăng nhập.")
            else:
                # Tạo một crawler instance để đăng nhập
                try:
                    login_progress = st.progress(0)
                    login_status = st.empty()
                    
                    login_progress.progress(10)
                    login_status.info("Đang khởi tạo trình duyệt...")
                    
                    # Tạo crawler mới
                    crawler = TikTokCommentCrawler(headless=headless)
                    crawler.captcha_callback = show_captcha_message

                    login_progress.progress(30)
                    login_status.info("Đang tiến hành đăng nhập...")
                    
                    # Thực hiện đăng nhập
                    if crawler.login_to_tiktok(login_username, login_password, max_wait=login_timeout):
                        login_progress.progress(100)
                        login_status.success("Đăng nhập thành công!")
                        
                        # Lưu crawler vào session state để sử dụng lại
                        st.session_state['tiktok_crawler'] = crawler
                        st.session_state['logged_in'] = True
                        st.session_state['stage'] = 'crawl'
                        
                        # Tự động reload trang để hiển thị giai đoạn tiếp theo
                        st.rerun()
                    else:
                        login_progress.progress(100)
                        login_status.error("Đăng nhập thất bại. Vui lòng thử lại.")
                        
                        # Đóng crawler nếu đăng nhập thất bại
                        crawler.close()
                        
                except Exception as e:
                    st.error(f"Lỗi trong quá trình đăng nhập: {str(e)}")
                    # Đảm bảo crawler được đóng nếu có lỗi
                    if 'crawler' in locals() and crawler:
                        crawler.close()
    
    # Giai đoạn 2: Thu thập bình luận
    if st.session_state['stage'] == 'crawl':
        st.subheader("🕸️ Bước 2: Thu thập bình luận")
        
        st.success("✅ Đã đăng nhập thành công! Bây giờ bạn có thể thu thập bình luận.")
        
        # Thêm tab để chọn giữa URL trực tiếp và tìm kiếm
        tab1, tab2 = st.tabs(["Nhập URL trực tiếp", "Tìm kiếm"])
        
        with tab1:
            # Form thu thập dữ liệu bằng URL trực tiếp (code hiện tại)
            with st.form(key="crawler_form_url"):
                # URL video TikTok
                tiktok_url = st.text_input(
                    "URL video TikTok", 
                    placeholder="https://www.tiktok.com/@username/video/1234567890123456789"
                )
                
                col1, col2 = st.columns(2)
                
                # Số lượng comments tối đa
                with col1:
                    max_comments = st.number_input(
                        "Số lượng bình luận tối đa", 
                        min_value=10, 
                        max_value=10000, 
                        value=100, 
                        step=10
                    )
                
                # Thời gian chờ giữa các lần cuộn
                with col2:
                    scroll_pause_time = st.slider(
                        "Thời gian chờ giữa các lần cuộn (giây)", 
                        min_value=0.5, 
                        max_value=5.0, 
                        value=1.5, 
                        step=0.1
                    )
                
                # Tùy chọn nâng cao
                with st.expander("Tùy chọn nâng cao"):
                    include_replies = st.checkbox("Thu thập cả trả lời (replies)", value=True)
                    
                    col3, col4 = st.columns(2)
                    with col3:
                        timeout = st.number_input(
                            "Thời gian chờ tối đa (giây)", 
                            min_value=5, 
                            max_value=60, 
                            value=10, 
                            step=1
                        )
                    
                    with col4:
                        output_format = st.selectbox(
                            "Định dạng đầu ra",
                            options=["CSV", "JSON", "Excel"],
                            index=0
                        )
                    
                    # Tùy chọn database (nếu được bật)
                    if db_enabled:
                        save_to_db = st.checkbox(
                            "Lưu vào database PostgreSQL", 
                            value=auto_save_to_db,
                            help="Lưu dữ liệu thu thập vào PostgreSQL database"
                        )
                    else:
                        save_to_db = False
                        
                # Nút submit
                submit_button = st.form_submit_button(label="Bắt đầu thu thập")
                
                # Nút kết thúc
                end_session_button = st.form_submit_button(label="Kết thúc phiên", type="secondary")
            if submit_button:
                if not validate_tiktok_url(tiktok_url):
                    st.error("URL không hợp lệ. Vui lòng nhập URL TikTok hợp lệ.")
                    return
                
                # Lấy crawler từ session state
                crawler = st.session_state['tiktok_crawler']
                
                if crawler is None:
                    st.error("Phiên đã hết hạn. Vui lòng đăng nhập lại.")
                    st.session_state['stage'] = 'login'
                    st.rerun()
                    return
                
                # Tạo thư mục data nếu chưa tồn tại
                data_dir = Path("data/raw")
                data_dir.mkdir(parents=True, exist_ok=True)
                
                # Tạo tên file đầu ra
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                video_id = get_video_id_from_url(tiktok_url)
                
                if output_format == "CSV":
                    output_file = data_dir / f"tiktok_comments_{video_id}_{timestamp}.csv"
                elif output_format == "JSON":
                    output_file = data_dir / f"tiktok_comments_{video_id}_{timestamp}.json"
                else:  # Excel
                    output_file = data_dir / f"tiktok_comments_{video_id}_{timestamp}.xlsx"
                
                # Hiển thị tiến trình
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(percent, message):
                    """Cập nhật thanh tiến trình và tin nhắn"""
                    progress_bar.progress(percent)
                    status_text.text(message)
                
                try:
                    # Điều hướng đến phần bình luận
                    update_progress(10, "Đang mở trang video và điều hướng đến bình luận...")
                    if not crawler.navigate_to_comments(tiktok_url):
                        st.error("Không thể mở trang bình luận. Vui lòng kiểm tra URL và thử lại.")
                        return
                    
                    # Đợi trang tải xong
                    time.sleep(2)
                    
                    # Tải comments
                    update_progress(20, "Đang tải bình luận...")
                    crawler.load_all_comments(
                        max_comments=max_comments,
                        scroll_pause_time=scroll_pause_time,
                        progress_callback=update_progress
                    )
                    
                    # Trích xuất comments
                    update_progress(80, "Đang trích xuất dữ liệu bình luận...")
                    comments_data = crawler.extract_comments(
                        max_comments=max_comments,
                        include_replies=include_replies
                    )
                    
                    if not comments_data:
                        st.warning("Không tìm thấy bình luận nào.")
                        return
                    
                    # Lưu dữ liệu
                    update_progress(90, f"Đang lưu {len(comments_data)} bình luận...")
                    
                    success = False
                    if output_format == "CSV":
                        success = crawler.save_to_csv(comments_data, output_file=output_file)
                    elif output_format == "JSON":
                        success = crawler.save_to_json(comments_data, output_file=output_file)
                    else:  # Excel
                        df = pd.DataFrame(comments_data)
                        success = export_to_excel(df, output_file)
                    
                    # Lưu vào database nếu được yêu cầu
                    if success and db_enabled and save_to_db:
                        update_progress(95, "Đang lưu dữ liệu vào PostgreSQL...")
                        
                        # Chuyển đổi thành DataFrame
                        df = pd.DataFrame(comments_data)
                        
                        # Lấy kết nối database
                        db = get_db_connector(db_config)
                        
                        try:
                            # Kết nối đến database
                            if db.connect_to_database():
                                # Xuất DataFrame vào PostgreSQL
                                db_success = db.export_dataframe_to_postgres(df, video_id, tiktok_url)
                                if db_success:
                                    st.success(f"Đã lưu {len(df)} bình luận vào PostgreSQL database!")
                                else:
                                    st.warning("Không thể lưu dữ liệu vào PostgreSQL database.")
                            else:
                                st.warning("Không thể kết nối đến PostgreSQL database.")
                        except Exception as e:
                            st.warning(f"Lỗi khi lưu dữ liệu vào database: {str(e)}")
                        finally:
                            db.close()
                    
                    if success:
                        update_progress(100, f"Đã hoàn thành! Thu thập được {len(comments_data)} bình luận.")
                        
                        # Hiển thị thông tin và xem trước dữ liệu
                        st.success(f"Đã lưu {len(comments_data)} bình luận vào: {output_file}")
                        
                        # Hiển thị dữ liệu
                        df = pd.DataFrame(comments_data)
                        st.subheader("Xem trước dữ liệu")
                        st.dataframe(df.head(10))
                        
                        # Phân tích cơ bản
                        if len(comments_data) > 0:
                            st.subheader("Phân tích cơ bản")
                            
                            # Phân tách comments chính và replies
                            main_comments = [c for c in comments_data if not c.get('is_reply', False)]
                            replies = [c for c in comments_data if c.get('is_reply', False)]
                            
                            col_a, col_b, col_c = st.columns(3)
                            
                            with col_a:
                                st.metric("Tổng số bình luận", len(comments_data))
                                
                            with col_b:
                                st.metric("Bình luận chính", len(main_comments))
                                
                            with col_c:
                                st.metric("Trả lời", len(replies))
                            
                            # Phân tích thêm nếu có đủ dữ liệu
                            if len(main_comments) > 5:
                                analysis_results = basic_analysis(pd.DataFrame(main_comments))
                                
                                col_d, col_e = st.columns(2)
                                
                                with col_d:
                                    st.metric("Số người dùng khác nhau", analysis_results.get("unique_users", 0))
                                    
                                with col_e:
                                    st.metric("Độ dài bình luận trung bình", f"{analysis_results.get('avg_comment_length', 0):.1f} ký tự")
                                
                                # Hiển thị biểu đồ người dùng tích cực nhất
                                if "top_users" in analysis_results:
                                    st.subheader("Top 10 người dùng tích cực nhất")
                                    st.bar_chart(analysis_results["top_users"])
                    else:
                        st.error("Không thể lưu dữ liệu bình luận.")
                        
                except Exception as e:
                    st.error(f"Đã xảy ra lỗi: {str(e)}")
        
            # Xử lý khi nút kết thúc được nhấn
            if end_session_button:
                # Lấy crawler từ session state
                crawler = st.session_state['tiktok_crawler']
                
                if crawler:
                    # Đóng trình duyệt
                    crawler.close()
                    
                    # Xóa session state
                    st.session_state['tiktok_crawler'] = None
                    st.session_state['logged_in'] = False
                    st.session_state['stage'] = 'completed'
                    
                    st.rerun()
                
        with tab2:
            # Form tìm kiếm
            with st.form(key="crawler_form_search"):
                # Từ khóa tìm kiếm
                search_keyword = st.text_input(
                    "Từ khóa tìm kiếm", 
                    placeholder="Nhập từ khóa để tìm video TikTok"
                )
                
                col1, col2 = st.columns(2)
                
                # Số lượng video tối đa
                with col1:
                    max_videos = st.number_input(
                        "Số lượng video tối đa", 
                        min_value=5, 
                        max_value=50, 
                        value=10, 
                        step=5
                    )
                
                # Số lượng comments tối đa cho mỗi video
                with col2:
                    max_comments_per_video = st.number_input(
                        "Số lượng bình luận tối đa/video", 
                        min_value=10, 
                        max_value=1000, 
                        value=50, 
                        step=10
                    )
                
                # Tùy chọn nâng cao
                with st.expander("Tùy chọn nâng cao"):
                    include_replies = st.checkbox("Thu thập cả trả lời (replies)", value=True)
                    
                    col3, col4 = st.columns(2)
                    with col3:
                        scroll_pause_time = st.slider(
                            "Thời gian chờ giữa các lần cuộn (giây)", 
                            min_value=0.5, 
                            max_value=5.0, 
                            value=1.5, 
                            step=0.1
                        )
                    
                    with col4:
                        output_format = st.selectbox(
                            "Định dạng đầu ra",
                            options=["CSV", "JSON", "Excel"],
                            index=0
                        )
                    
                    # Tùy chọn database (nếu được bật)
                    if db_enabled:
                        save_to_db = st.checkbox(
                            "Lưu vào database PostgreSQL", 
                            value=auto_save_to_db,
                            help="Lưu dữ liệu thu thập vào PostgreSQL database"
                        )
                    else:
                        save_to_db = False
                        
                # Nút tìm kiếm
                search_button = st.form_submit_button(label="Tìm kiếm")
            
            # Xử lý khi nút tìm kiếm được nhấn
            if search_button:
                if not search_keyword:
                    st.error("Vui lòng nhập từ khóa tìm kiếm.")
                    return
                
                # Lấy crawler từ session state
                crawler = st.session_state['tiktok_crawler']
                
                if crawler is None:
                    st.error("Phiên đã hết hạn. Vui lòng đăng nhập lại.")
                    st.session_state['stage'] = 'login'
                    st.rerun()
                    return
                
                # Thực hiện tìm kiếm
                with st.spinner(f"Đang tìm kiếm video với từ khóa '{search_keyword}'..."):
                    videos = crawler.search_tiktok(search_keyword, max_videos=max_videos)
                
                if not videos:
                    st.warning(f"Không tìm thấy video nào với từ khóa '{search_keyword}'.")
                    return
                
                # Hiển thị danh sách video tìm thấy
                st.success(f"Tìm thấy {len(videos)} video.")
                
                # Tạo danh sách video để hiển thị
                video_list = []
                for i, video in enumerate(videos):
                    video_list.append({
                        "STT": i+1,
                        "ID Video": video["video_id"],
                        "Tác giả": video["author"],
                        "Mô tả": video["description"][:50] + "..." if len(video["description"]) > 50 else video["description"],
                        "URL": video["video_url"]
                    })
                
                # Hiển thị bảng video
                st.dataframe(pd.DataFrame(video_list))
                
                # Chọn video để thu thập bình luận
                selected_videos = st.multiselect(
                    "Chọn video để thu thập bình luận",
                    options=[f"{v['STT']}. {v['Tác giả']} - {v['ID Video']}" for v in video_list],
                    default=[f"1. {video_list[0]['Tác giả']} - {video_list[0]['ID Video']}"] if video_list else []
                )
                
                if selected_videos:
                    # Nút bắt đầu thu thập bình luận
                    if st.button("Bắt đầu thu thập bình luận", key="start_crawl_selected"):
                        # Tạo thư mục data nếu chưa tồn tại
                        data_dir = Path("data/raw")
                        data_dir.mkdir(parents=True, exist_ok=True)
                        
                        # Lấy danh sách video đã chọn
                        selected_indices = [int(v.split(".")[0])-1 for v in selected_videos]
                        selected_video_data = [videos[i] for i in selected_indices]
                        
                        # Thu thập bình luận từ mỗi video đã chọn
                        all_comments_data = []
                        
                        # Progress bar tổng thể
                        overall_progress = st.progress(0)
                        overall_status = st.empty()
                        
                        for idx, video in enumerate(selected_video_data):
                            overall_status.text(f"Đang xử lý video {idx+1}/{len(selected_video_data)}: {video['video_id']}")
                            overall_progress.progress((idx) / len(selected_video_data))
                            
                            # Hiển thị tiến trình cho video hiện tại
                            st.write(f"Video {idx+1}: {video['author']} - {video['video_id']}")
                            video_progress = st.progress(0)
                            video_status = st.empty()
                            
                            def update_video_progress(percent, message):
                                """Cập nhật thanh tiến trình và tin nhắn cho video hiện tại"""
                                video_progress.progress(percent)
                                video_status.text(message)
                            
                            try:
                                # Điều hướng đến phần bình luận
                                update_video_progress(10, "Đang mở trang video và điều hướng đến bình luận...")
                                if not crawler.navigate_to_comments(video['video_url']):
                                    video_status.warning("Không thể mở trang bình luận. Chuyển sang video tiếp theo.")
                                    continue
                                
                                # Đợi trang tải xong
                                time.sleep(2)
                                
                                # Tải comments
                                update_video_progress(20, "Đang tải bình luận...")
                                crawler.load_all_comments(
                                    max_comments=max_comments_per_video,
                                    scroll_pause_time=scroll_pause_time,
                                    progress_callback=update_video_progress
                                )
                                
                                # Trích xuất comments
                                update_video_progress(80, "Đang trích xuất dữ liệu bình luận...")
                                comments_data = crawler.extract_comments(
                                    max_comments=max_comments_per_video,
                                    include_replies=include_replies
                                )
                                
                                if not comments_data:
                                    video_status.warning("Không tìm thấy bình luận nào cho video này.")
                                    continue
                                
                                # Thêm thông tin video vào comments
                                for comment in comments_data:
                                    comment['video_id'] = video['video_id']
                                    comment['video_url'] = video['video_url']
                                    comment['video_author'] = video['author']
                                
                                # Thêm vào tất cả comments
                                all_comments_data.extend(comments_data)
                                
                                update_video_progress(100, f"Đã hoàn thành! Thu thập được {len(comments_data)} bình luận.")
                                
                            except Exception as e:
                                video_status.error(f"Lỗi khi thu thập bình luận từ video {video['video_id']}: {str(e)}")
                        
                        # Hoàn thành tất cả video
                        overall_progress.progress(1.0)
                        overall_status.text(f"Đã hoàn thành! Thu thập được tổng cộng {len(all_comments_data)} bình luận từ {len(selected_video_data)} video.")
                        
                        if all_comments_data:
                            # Tạo tên file đầu ra
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            
                            if output_format == "CSV":
                                output_file = data_dir / f"tiktok_comments_search_{timestamp}.csv"
                                export_to_csv(pd.DataFrame(all_comments_data), output_file)
                            elif output_format == "JSON":
                                output_file = data_dir / f"tiktok_comments_search_{timestamp}.json"
                                export_to_json(pd.DataFrame(all_comments_data), output_file)
                            else:  # Excel
                                output_file = data_dir / f"tiktok_comments_search_{timestamp}.xlsx"
                                export_to_excel(pd.DataFrame(all_comments_data), output_file)
                            
                            st.success(f"Đã lưu {len(all_comments_data)} bình luận vào: {output_file}")
                            
                            # Lưu vào database nếu được yêu cầu
                            if db_enabled and save_to_db:
                                st.info("Đang lưu dữ liệu vào PostgreSQL...")
                                
                                # Chuyển đổi thành DataFrame
                                df = pd.DataFrame(all_comments_data)
                                
                                # Lấy kết nối database
                                db = get_db_connector(db_config)
                                
                                try:
                                    # Kết nối đến database
                                    if db.connect_to_database():
                                        # Lưu từng video vào database
                                        success_count = 0
                                        for video in selected_video_data:
                                            video_id = video['video_id']
                                            video_comments = df[df['video_id'] == video_id]
                                            
                                            if not video_comments.empty:
                                                if db.export_dataframe_to_postgres(video_comments, video_id, video['video_url']):
                                                    success_count += 1
                                        
                                        if success_count > 0:
                                            st.success(f"Đã lưu dữ liệu vào PostgreSQL cho {success_count}/{len(selected_video_data)} video!")
                                        else:
                                            st.warning("Không thể lưu dữ liệu vào PostgreSQL database.")
                                    else:
                                        st.warning("Không thể kết nối đến PostgreSQL database.")
                                except Exception as e:
                                    st.warning(f"Lỗi khi lưu dữ liệu vào database: {str(e)}")
                                finally:
                                    db.close()
                            
                            # Hiển thị mẫu dữ liệu
                            st.subheader("Xem trước dữ liệu")
                            st.dataframe(pd.DataFrame(all_comments_data).head(10))
                        else:
                            st.warning("Không thu thập được bình luận nào từ các video đã chọn.")
            if db_enabled and videos:
                try:
                    # Lấy kết nối database
                    db = get_db_connector(db_config)
                    
                    if db.connect_to_database():
                        # Lưu kết quả tìm kiếm
                        if db.save_search_results(search_keyword, videos):
                            st.success(f"Đã lưu kết quả tìm kiếm vào database!")
                        else:
                            st.warning(f"Không thể lưu kết quả tìm kiếm vào database.")
                    else:
                        st.warning(f"Không thể kết nối đến database.")
                        
                    # Đóng kết nối
                    db.close()
                except Exception as e:
                    st.warning(f"Lỗi khi lưu kết quả tìm kiếm: {str(e)}")

    
    # Giai đoạn 3: Kết thúc
    elif st.session_state['stage'] == 'completed':
        st.subheader("✅ Phiên làm việc đã kết thúc")
        
        st.success("Bạn đã kết thúc phiên làm việc và trình duyệt đã được đóng.")
        
        # Hiển thị các file đã thu thập
        data_dir = Path("data/raw")
        if data_dir.exists():
            data_files = list(data_dir.glob("*.csv")) + list(data_dir.glob("*.json")) + list(data_dir.glob("*.xlsx"))
            
            if data_files:
                st.subheader("📁 File dữ liệu đã thu thập")
                
                # Sắp xếp theo thời gian giảm dần (mới nhất trước)
                recent_files = sorted(data_files, key=lambda x: x.stat().st_mtime, reverse=True)
                
                file_data = []
                for file in recent_files[:10]:  # Hiển thị 10 file gần nhất
                    file_size = file.stat().st_size / 1024  # KB
                    file_time = datetime.fromtimestamp(file.stat().st_mtime)
                    file_data.append({
                        "Tên file": file.name,
                        "Dung lượng": f"{file_size:.2f} KB",
                        "Thời gian": file_time.strftime("%d/%m/%Y %H:%M:%S")
                    })
                
                if file_data:
                    st.table(pd.DataFrame(file_data))
        
        # Nút để bắt đầu phiên mới
        if st.button("Bắt đầu phiên mới", type="primary"):
            st.session_state['stage'] = 'login'
            st.rerun()