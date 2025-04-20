import streamlit as st
import pandas as pd
from pathlib import Path
import os

def render_home_page():
    """
    Hiển thị trang chính của ứng dụng
    """
    st.header("🏠 Trang chính")
    
    # Hiển thị thông tin giới thiệu
    st.markdown("""
    ## Chào mừng đến với TikTok Comment Harvester
    
    Đây là công cụ giúp bạn thu thập và phân tích bình luận từ các video TikTok một cách dễ dàng.
    
    ### Các tính năng chính:
    
    - 🕸️ **Thu thập bình luận**: Crawl bình luận từ video TikTok với Selenium
    - 📊 **Phân tích dữ liệu**: Xem thống kê và phân tích cơ bản từ dữ liệu thu thập được
    - 📤 **Xuất dữ liệu**: Xuất dữ liệu sang nhiều định dạng (CSV, JSON, Excel)
    - ⚙️ **Tùy chỉnh**: Điều chỉnh các tham số thu thập theo nhu cầu
    
    ### Bắt đầu sử dụng:
    """)
    
    # Nút bắt đầu crawl
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("🕸️ Bắt đầu thu thập", use_container_width=True):
            st.session_state['page'] = 'Crawler'
            st.rerun()
    with col2:
        if st.button("📊 Xem dữ liệu", use_container_width=True):
            st.session_state['page'] = 'Data View'
            st.rerun()
            
    with col3:
        if st.button("⚙️ Cài đặt", use_container_width=True):
            st.session_state['page'] = 'Settings'
            st.rerun()
    
    # Hiển thị thống kê dữ liệu đã thu thập (nếu có)
    st.markdown("---")
    
    data_dir = Path("data/raw")
    if data_dir.exists():
        data_files = list(data_dir.glob("*.csv")) + list(data_dir.glob("*.json")) + list(data_dir.glob("*.xlsx"))
        
        if data_files:
            st.subheader("📁 Thống kê dữ liệu")
            
            # Thống kê cơ bản
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Số lượng file dữ liệu", len(data_files))
                
                # Dung lượng tổng cộng
                total_size = sum(f.stat().st_size for f in data_files) / (1024 * 1024)  # MB
                st.metric("Dung lượng dữ liệu", f"{total_size:.2f} MB")
                
            with col2:
                # Thời gian thu thập gần nhất
                if data_files:
                    latest_file = max(data_files, key=lambda x: x.stat().st_mtime)
                    import datetime
                    latest_time = datetime.datetime.fromtimestamp(latest_file.stat().st_mtime)
                    st.metric("Lần thu thập gần nhất", latest_time.strftime("%d/%m/%Y %H:%M:%S"))
                    st.metric("File gần nhất", latest_file.name)
            
            # Hiển thị 5 file gần nhất
            st.subheader("📋 File dữ liệu gần đây")
            
            recent_files = sorted(data_files, key=lambda x: x.stat().st_mtime, reverse=True)[:5]
            
            file_data = []
            for file in recent_files:
                file_size = file.stat().st_size / 1024  # KB
                file_time = datetime.datetime.fromtimestamp(file.stat().st_mtime)
                file_data.append({
                    "Tên file": file.name,
                    "Dung lượng": f"{file_size:.2f} KB",
                    "Thời gian": file_time.strftime("%d/%m/%Y %H:%M:%S")
                })
            
            if file_data:
                st.table(pd.DataFrame(file_data))
            
            # Nút mở thư mục dữ liệu
            if st.button("📁 Mở thư mục dữ liệu", use_container_width=False):
                try:
                    import subprocess
                    import platform
                    
                    if platform.system() == "Windows":
                        os.startfile(data_dir)
                    elif platform.system() == "Darwin":  # macOS
                        subprocess.Popen(["open", data_dir])
                    else:  # Linux
                        subprocess.Popen(["xdg-open", data_dir])
                        
                    st.success(f"Đã mở thư mục: {data_dir}")
                except Exception as e:
                    st.error(f"Không thể mở thư mục: {str(e)}")
        else:
            st.info("Chưa có dữ liệu nào được thu thập. Hãy bắt đầu thu thập bình luận từ video TikTok.")
    else:
        st.info("Chưa có dữ liệu nào được thu thập. Hãy bắt đầu thu thập bình luận từ video TikTok.")
    
    # Hiển thị hướng dẫn
    st.markdown("---")
    with st.expander("📚 Hướng dẫn sử dụng", expanded=False):
        st.markdown("""
        ### Cách sử dụng TikTok Comment Harvester
        
        1. **Thu thập bình luận**:
           - Vào trang "Crawler"
           - Nhập URL video TikTok (ví dụ: https://www.tiktok.com/@username/video/1234567890123456789)
           - Chọn số lượng bình luận tối đa cần thu thập
           - Tùy chỉnh các tham số khác nếu cần
           - Nhấn "Bắt đầu thu thập" và đợi quá trình hoàn tất
        
        2. **Xem và phân tích dữ liệu**:
           - Vào trang "Data View"
           - Chọn file dữ liệu từ danh sách
           - Xem thống kê và biểu đồ phân tích
        
        3. **Xuất dữ liệu**:
           - Từ trang "Data View"
           - Chọn định dạng xuất (CSV, Excel, JSON)
           - Nhấn nút xuất tương ứng
        
        4. **Cài đặt**:
           - Vào trang "Settings"
           - Tùy chỉnh các thiết lập của ứng dụng
        
        ### Lưu ý
        
        - TikTok có thể thay đổi cấu trúc trang web, có thể cần cập nhật selectors
        - Sử dụng ứng dụng một cách có trách nhiệm và tuân thủ điều khoản dịch vụ của TikTok
        - Không nên thu thập dữ liệu với tần suất quá cao để tránh bị chặn IP
        """)