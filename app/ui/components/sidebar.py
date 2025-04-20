import streamlit as st
from pathlib import Path
import base64

def render_sidebar():
    """
    Hiển thị sidebar cho ứng dụng
    
    Returns:
        str: Tên trang được chọn
    """
    st.sidebar.title("TikTok Comment Harvester")
    
    # Logo (nếu có)
    logo_path = Path(__file__).parent.parent.parent / "ui" / "styles" / "images" / "logo.png"
    if logo_path.exists():
        st.sidebar.image(str(logo_path), width=200)
    
    # Menu điều hướng
    page = st.sidebar.radio(
        "Chọn chức năng",
        options=["Home", "Crawler", "Data View", "Settings"],
        index=0,
        help="Chọn chức năng bạn muốn sử dụng"
    )
    
    st.sidebar.markdown("---")
    
    # Hiển thị thông tin đã thu thập (nếu có)
    data_dir = Path("data/raw")
    if data_dir.exists():
        data_files = list(data_dir.glob("*.csv")) + list(data_dir.glob("*.json")) + list(data_dir.glob("*.xlsx"))
        if data_files:
            st.sidebar.subheader("Dữ liệu đã thu thập")
            st.sidebar.text(f"Số lượng file: {len(data_files)}")
            
            if page == "Data View":
                file_options = ["Chọn file..."] + [file.name for file in data_files]
                selected_file = st.sidebar.selectbox("Chọn file dữ liệu", options=file_options)
                
                if selected_file != "Chọn file...":
                    st.session_state['selected_data_file'] = str(data_dir / selected_file)
    
    st.sidebar.markdown("---")
    
    # Phần thông tin
    with st.sidebar.expander("Thông tin", expanded=False):
        st.markdown("""
        **TikTok Comment Harvester** là công cụ giúp thu thập và phân tích bình luận từ các video TikTok.
        
        **Lưu ý quan trọng:**
        - Sử dụng ứng dụng này một cách có trách nhiệm
        - Tôn trọng quyền riêng tư và tuân thủ điều khoản dịch vụ của TikTok
        - Không sử dụng dữ liệu thu thập được vào mục đích xấu
        
        **Cách sử dụng:**
        1. Vào trang "Crawler" để thu thập bình luận
        2. Nhập URL video TikTok và cấu hình tham số
        3. Bắt đầu thu thập và đợi quá trình hoàn tất
        4. Vào trang "Data View" để xem và phân tích dữ liệu
        """)
    
    # Tác giả
    st.sidebar.markdown("---")
    st.sidebar.caption("Phát triển bởi: [Tên của bạn]")
    st.sidebar.caption("Phiên bản: 1.0.0")
    
    return page