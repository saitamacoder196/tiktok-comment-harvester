import streamlit as st
import json
from pathlib import Path
import os
import shutil

def render_settings_page():
    """
    Hiển thị trang cài đặt ứng dụng
    """
    st.header("⚙️ Cài đặt")
    
    # Đường dẫn đến file cấu hình
    config_dir = Path("app/config")
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "settings.json"
    
    # Tải cấu hình hiện tại
    config = load_config(config_file)
    
    # Tab cho các nhóm cài đặt
    tab1, tab2, tab3 = st.tabs(["Crawler", "Giao diện", "Dữ liệu"])
    
    with tab1:
        st.subheader("Cài đặt Crawler")
        
        # Cài đặt chromedriver
        st.markdown("#### ChromeDriver")
        
        chromedriver_method = st.radio(
            "Phương thức lấy ChromeDriver",
            options=["Tự động tải", "Thủ công (chỉ định đường dẫn)"],
            index=0 if config.get("auto_chromedriver", True) else 1
        )
        
        config["auto_chromedriver"] = (chromedriver_method == "Tự động tải")
        
        if not config["auto_chromedriver"]:
            config["chromedriver_path"] = st.text_input(
                "Đường dẫn đến ChromeDriver",
                value=config.get("chromedriver_path", "")
            )
        
        # Cài đặt hành vi
        st.markdown("#### Hành vi Crawler")
        
        col1, col2 = st.columns(2)
        
        with col1:
            config["default_headless"] = st.checkbox(
                "Chế độ headless mặc định",
                value=config.get("default_headless", False)
            )
            
            config["default_max_comments"] = st.number_input(
                "Số lượng bình luận mặc định",
                min_value=10,
                max_value=10000,
                value=config.get("default_max_comments", 100),
                step=10
            )
        
        with col2:
            config["default_scroll_pause"] = st.slider(
                "Thời gian chờ mặc định (giây)",
                min_value=0.5,
                max_value=5.0,
                value=config.get("default_scroll_pause", 1.5),
                step=0.1
            )
            
            config["default_timeout"] = st.slider(
                "Timeout mặc định (giây)",
                min_value=5,
                max_value=60,
                value=config.get("default_timeout", 10),
                step=1
            )
            
        # Cài đặt User-Agent
        st.markdown("#### User-Agent")
        
        use_custom_ua = st.checkbox(
            "Sử dụng User-Agent tùy chỉnh",
            value=config.get("use_custom_ua", False)
        )
        
        config["use_custom_ua"] = use_custom_ua
        
        if use_custom_ua:
            config["custom_user_agent"] = st.text_input(
                "User-Agent tùy chỉnh",
                value=config.get("custom_user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            )
    
    with tab2:
        st.subheader("Cài đặt giao diện")
        
        # Cài đặt theme
        st.markdown("#### Theme")
        
        theme = st.selectbox(
            "Theme",
            options=["Light", "Dark", "Auto"],
            index=["Light", "Dark", "Auto"].index(config.get("theme", "Light"))
        )
        
        config["theme"] = theme
        
        # Cài đặt ngôn ngữ
        st.markdown("#### Ngôn ngữ")
        
        language = st.selectbox(
            "Ngôn ngữ",
            options=["Tiếng Việt", "English"],
            index=["Tiếng Việt", "English"].index(config.get("language", "Tiếng Việt"))
        )
        
        config["language"] = language
        
        st.info("Một số cài đặt giao diện có thể cần khởi động lại ứng dụng để có hiệu lực.")
    
    with tab3:
        st.subheader("Cài đặt dữ liệu")
        
        # Định dạng xuất mặc định
        st.markdown("#### Định dạng xuất")
        
        default_export_format = st.selectbox(
            "Định dạng xuất mặc định",
            options=["CSV", "JSON", "Excel"],
            index=["CSV", "JSON", "Excel"].index(config.get("default_export_format", "CSV"))
        )
        
        config["default_export_format"] = default_export_format
        
        # Cài đặt lưu trữ
        st.markdown("#### Lưu trữ")
        
        col1, col2 = st.columns(2)
        
        with col1:
            config["auto_clean_data"] = st.checkbox(
                "Tự động làm sạch dữ liệu cũ",
                value=config.get("auto_clean_data", False)
            )
            
        with col2:
            if config["auto_clean_data"]:
                config["clean_days"] = st.number_input(
                    "Xóa dữ liệu cũ hơn (ngày)",
                    min_value=1,
                    max_value=365,
                    value=config.get("clean_days", 30),
                    step=1
                )
        
        # Nút xóa dữ liệu
        st.markdown("#### Quản lý dữ liệu")
        
        if st.button("🗑️ Xóa tất cả dữ liệu", type="primary", use_container_width=False):
            confirm = st.checkbox("Tôi hiểu rằng hành động này không thể hoàn tác và muốn xóa tất cả dữ liệu")
            
            if confirm:
                try:
                    # Xóa thư mục data
                    data_dir = Path("data")
                    if data_dir.exists():
                        for item in data_dir.glob("**/*"):
                            if item.is_file():
                                item.unlink()
                        
                        st.success("Đã xóa tất cả dữ liệu thành công!")
                    else:
                        st.info("Không có dữ liệu để xóa.")
                except Exception as e:
                    st.error(f"Lỗi khi xóa dữ liệu: {str(e)}")
    
    # Lưu cấu hình
    if st.button("💾 Lưu cài đặt", type="primary"):
        save_config(config, config_file)
        st.success("Đã lưu cài đặt thành công!")
        
    # Khôi phục cài đặt mặc định
    if st.button("🔄 Khôi phục cài đặt mặc định"):
        default_config = get_default_config()
        save_config(default_config, config_file)
        st.success("Đã khôi phục cài đặt mặc định!")
        st.info("Vui lòng làm mới trang để áp dụng cài đặt mới.")

def load_config(config_file):
    """
    Tải cấu hình từ file
    
    Args:
        config_file (Path): Đường dẫn đến file cấu hình
        
    Returns:
        dict: Cấu hình đã tải
    """
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return get_default_config()
    else:
        return get_default_config()

def save_config(config, config_file):
    """
    Lưu cấu hình vào file
    
    Args:
        config (dict): Cấu hình cần lưu
        config_file (Path): Đường dẫn đến file cấu hình
    """
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)

def get_default_config():
    """
    Trả về cấu hình mặc định
    
    Returns:
        dict: Cấu hình mặc định
    """
    return {
        "auto_chromedriver": True,
        "chromedriver_path": "",
        "default_headless": False,
        "default_max_comments": 100,
        "default_scroll_pause": 1.5,
        "default_timeout": 10,
        "use_custom_ua": False,
        "custom_user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "theme": "Light",
        "language": "Tiếng Việt",
        "default_export_format": "CSV",
        "auto_clean_data": False,
        "clean_days": 30
    }