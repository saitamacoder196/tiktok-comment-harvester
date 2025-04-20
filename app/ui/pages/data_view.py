import streamlit as st
import pandas as pd
import json
from pathlib import Path
import os
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
from app.data.processor import clean_data, basic_analysis, sentiment_analysis, extract_hashtags, get_popular_hashtags
from app.data.exporter import export_to_excel, export_to_csv, export_to_json

def render_data_view_page():
    """
    Hiển thị trang xem và phân tích dữ liệu
    """
    st.header("📊 Xem và phân tích dữ liệu")
    
    # Kiểm tra xem có file dữ liệu được chọn từ sidebar không
    selected_file = st.session_state.get('selected_data_file', None)
    
    # Nếu không có, hiển thị danh sách file để chọn
    if not selected_file:
        data_dir = Path("data/raw")
        if data_dir.exists():
            data_files = list(data_dir.glob("*.csv")) + list(data_dir.glob("*.json")) + list(data_dir.glob("*.xlsx"))
            
            if data_files:
                file_options = [file.name for file in data_files]
                selected_filename = st.selectbox("Chọn file dữ liệu", options=file_options)
                
                selected_file = str(data_dir / selected_filename)
            else:
                st.info("Chưa có dữ liệu nào được thu thập. Hãy vào trang Crawler để thu thập dữ liệu.")
                return
        else:
            st.info("Chưa có dữ liệu nào được thu thập. Hãy vào trang Crawler để thu thập dữ liệu.")
            return
    
    # Đọc dữ liệu từ file
    file_path = Path(selected_file)
    df = None
    
    try:
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        elif file_path.suffix.lower() == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            df = pd.DataFrame(data)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        else:
            st.error(f"Không hỗ trợ định dạng file: {file_path.suffix}")
            return
    except Exception as e:
        st.error(f"Lỗi khi đọc file dữ liệu: {str(e)}")
        return
    
    if df is None or df.empty:
        st.warning("File dữ liệu trống hoặc không đọc được.")
        return
    
    # Làm sạch dữ liệu
    df_clean = clean_data(df)
    
    # Hiển thị thông tin cơ bản
    st.subheader("📋 Thông tin dữ liệu")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Số lượng bình luận", len(df_clean))
    with col2:
        st.metric("Số người dùng khác nhau", df_clean['username'].nunique())
    with col3:
        if 'comment_length' in df_clean.columns:
            st.metric("Độ dài bình luận trung bình", f"{df_clean['comment_length'].mean():.1f} ký tự")
    
    # Xem dữ liệu thô
    with st.expander("Xem dữ liệu thô", expanded=False):
        st.dataframe(df)
    
    # Phân tích dữ liệu
    st.subheader("📊 Phân tích dữ liệu")
    
    # Phân tích cơ bản
    analysis_results = basic_analysis(df_clean)
    
    # Tab cho các loại phân tích khác nhau
    tab1, tab2, tab3, tab4 = st.tabs(["Thống kê cơ bản", "Phân tích người dùng", "Phân tích nội dung", "Xu hướng"])
    
    with tab1:
        # Phân phối độ dài bình luận
        st.subheader("Phân phối độ dài bình luận")
        st.bar_chart(analysis_results["comment_length_dist"])
        
        # Phân tích lượt thích
        if 'likes_count' in df_clean.columns:
            st.subheader("Phân phối lượt thích")
            
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.histplot(df_clean['likes_count'], kde=True, bins=20, ax=ax)
            ax.set_xlabel('Số lượt thích')
            ax.set_ylabel('Số lượng bình luận')
            st.pyplot(fig)
    
    with tab2:
        # Top người dùng tích cực nhất
        st.subheader("Top 10 người dùng tích cực nhất")
        st.bar_chart(analysis_results["top_users"])
        
        # Phân tích người dùng và lượt thích
        if 'likes_count' in df_clean.columns:
            st.subheader("Người dùng và lượt thích trung bình")
            
            user_likes = df_clean.groupby('username')['likes_count'].mean().sort_values(ascending=False).head(10)
            st.bar_chart(user_likes)
    
    with tab3:
        # Phân tích cảm xúc
        st.subheader("Phân tích cảm xúc")
        
        # Thực hiện phân tích cảm xúc
        df_sentiment = sentiment_analysis(df_clean)
        
        # Đếm số lượng mỗi loại cảm xúc
        sentiment_counts = df_sentiment['sentiment'].value_counts()
        
        # Vẽ biểu đồ tròn
        fig = px.pie(
            values=sentiment_counts.values,
            names=sentiment_counts.index,
            title="Phân bố cảm xúc trong bình luận",
            color=sentiment_counts.index,
            color_discrete_map={
                'positive': '#4CAF50',
                'neutral': '#FFC107',
                'negative': '#F44336'
            }
        )
        st.plotly_chart(fig)
        
        # Hiển thị một số ví dụ mỗi loại
        with st.expander("Ví dụ bình luận theo cảm xúc", expanded=False):
            for sentiment in ['positive', 'neutral', 'negative']:
                st.subheader(f"Bình luận {sentiment}")
                examples = df_sentiment[df_sentiment['sentiment'] == sentiment].head(3)
                for _, row in examples.iterrows():
                    st.markdown(f"**{row['username']}**: {row['comment_text']}")
                    st.markdown("---")
    
    with tab4:
        # Phân tích hashtag
        st.subheader("Phân tích hashtag")
        
        # Trích xuất hashtag
        df_hashtags = extract_hashtags(df_clean)
        
        # Lấy top hashtag
        popular_hashtags = get_popular_hashtags(df_hashtags, top_n=15)
        
        if len(popular_hashtags) > 0:
            st.bar_chart(popular_hashtags)
        else:
            st.info("Không tìm thấy hashtag nào trong bình luận.")
    
    # Xuất dữ liệu
    st.markdown("---")
    st.subheader("📤 Xuất dữ liệu")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Xuất CSV", use_container_width=True):
            # Tạo đường dẫn file xuất
            export_path = file_path.parent.parent / "processed" / f"{file_path.stem}_processed.csv"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Xuất file
            if export_to_csv(df_clean, export_path):
                st.success(f"Đã xuất dữ liệu sang: {export_path}")
            else:
                st.error("Lỗi khi xuất dữ liệu.")
    
    with col2:
        if st.button("📊 Xuất Excel", use_container_width=True):
            # Tạo đường dẫn file xuất
            export_path = file_path.parent.parent / "processed" / f"{file_path.stem}_processed.xlsx"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Xuất file
            if export_to_excel(df_clean, export_path):
                st.success(f"Đã xuất dữ liệu sang: {export_path}")
            else:
                st.error("Lỗi khi xuất dữ liệu.")
    
    with col3:
        if st.button("🔄 Xuất JSON", use_container_width=True):
            # Tạo đường dẫn file xuất
            export_path = file_path.parent.parent / "processed" / f"{file_path.stem}_processed.json"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Xuất file
            if export_to_json(df_clean, export_path):
                st.success(f"Đã xuất dữ liệu sang: {export_path}")
            else:
                st.error("Lỗi khi xuất dữ liệu.")