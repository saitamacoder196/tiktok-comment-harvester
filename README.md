# TikTok Comment Harvester

Ứng dụng thu thập và phân tích bình luận từ các video TikTok, được xây dựng với Selenium và Streamlit.

## Tính năng

- Thu thập bình luận từ video TikTok với Selenium
- Giao diện người dùng thân thiện với Streamlit
- Phân tích dữ liệu bình luận cơ bản
- Xuất dữ liệu sang nhiều định dạng (CSV, Excel)
- Tùy chỉnh các tham số thu thập

## Yêu cầu hệ thống

- Python 3.7+
- Chrome/Chromium browser
- ChromeDriver phù hợp với phiên bản Chrome

## Cài đặt

### Sử dụng Conda (khuyến nghị)

```bash
# Clone repository
git clone https://github.com/your-username/tiktok-comment-harvester.git
cd tiktok-comment-harvester

# Tạo môi trường Conda
conda env create -f environment.yml

# Kích hoạt môi trường
conda activate tiktok-harvester
```

### Sử dụng pip

```bash
# Clone repository
git clone https://github.com/your-username/tiktok-comment-harvester.git
cd tiktok-comment-harvester

# Cài đặt dependencies
pip install -r requirements.txt
```

## Tải ChromeDriver

Tải ChromeDriver tại [trang chính thức](https://chromedriver.chromium.org/downloads) phù hợp với phiên bản Chrome của bạn và đặt vào thư mục gốc của dự án hoặc đường dẫn hệ thống.

## Sử dụng

### Chạy ứng dụng Streamlit

```bash
streamlit run app/main.py
```

### Sử dụng từ dòng lệnh

```bash
python -m app.crawler.tiktok_crawler --url "https://www.tiktok.com/@username/video/videoID" --output data/raw/comments.csv
```

## Hướng dẫn sử dụng

1. Mở ứng dụng Streamlit
2. Nhập URL video TikTok 
3. Chọn các tùy chọn thu thập (số lượng bình luận, thời gian chờ, v.v.)
4. Nhấn nút "Bắt đầu thu thập"
5. Xem và phân tích dữ liệu thu thập được
6. Xuất dữ liệu sang định dạng mong muốn

## Lưu ý

- TikTok có thể thay đổi cấu trúc trang web, có thể cần cập nhật XPATH selectors
- Sử dụng ứng dụng này một cách có trách nhiệm và tuân thủ điều khoản dịch vụ của TikTok
- Không nên thu thập dữ liệu với tần suất quá cao để tránh bị chặn IP

## Đóng góp

Đóng góp luôn được chào đón! Vui lòng xem [CONTRIBUTING.md](CONTRIBUTING.md) để biết thêm chi tiết.

## Giấy phép

Dự án này được phân phối theo giấy phép MIT. Xem [LICENSE](LICENSE) để biết thêm chi tiết.