import streamlit as st
from pathlib import Path
import sys
import os

# Đảm bảo có thể import từ thư mục gốc
root_path = Path(__file__).parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# Import các module cần thiết
from app.ui.components.sidebar import render_sidebar
from app.ui.pages.home import render_home_page
from app.ui.pages.crawler import render_crawler_page
from app.ui.pages.data_view import render_data_view_page
from app.ui.pages.settings import render_settings_page
from app.ui.pages.database_view import render_database_view_page

# Thiết lập cấu hình Streamlit
st.set_page_config(
    page_title="TikTok Comment Harvester",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
def load_css():
    css_file = Path(__file__).parent / "ui" / "styles" / "custom.css"
    if css_file.exists():
        with open(css_file) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def main():
    # Load CSS
    load_css()
    
    # Render sidebar
    selected_page = render_sidebar()
    
    # Render header
    st.title("🔍 TikTok Comment Harvester")
    
    # Render selected page
    if selected_page == "Home":
        render_home_page()
    elif selected_page == "Crawler":
        render_crawler_page()
    elif selected_page == "Data View":
        render_data_view_page()
    elif selected_page == "Database":
        render_database_view_page()
    elif selected_page == "Settings":
        render_settings_page()

if __name__ == "__main__":
    main()