import re
import time
import csv
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from urllib.parse import quote

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

from app.utils.helpers import get_video_id_from_url, validate_tiktok_url, setup_logger
from app.crawler.selectors import COMMENT_SELECTORS

logger = setup_logger(__name__)

class TikTokCommentCrawler:
    def __init__(self, headless: bool = False, chromedriver_path: Optional[str] = None,
                 timeout: int = 10, user_agent: Optional[str] = None):
        """
        Khởi tạo trình crawl comments TikTok
        
        Args:
            headless (bool): Chạy trình duyệt ở chế độ headless hay không
            chromedriver_path (str): Đường dẫn đến chromedriver (None để tự động tải)
            timeout (int): Thời gian chờ tối đa (giây)
            user_agent (str): User agent tùy chỉnh
        """
        self.driver = None
        self.wait = None
        self.timeout = timeout
        self._setup_driver(headless, chromedriver_path, user_agent)
        
    def _setup_driver(self, headless: bool, chromedriver_path: Optional[str], user_agent: Optional[str]):
        """Thiết lập trình duyệt Selenium"""
        options = Options()
        if headless:
            options.add_argument("--headless")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--start-maximized")
        options.add_argument("--lang=vi")
        
        # User agent
        if user_agent:
            options.add_argument(f"user-agent={user_agent}")
        else:
            options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        try:
            if chromedriver_path:
                service = Service(chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=options)
            else:
                # Tự động tải chromedriver
                self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            
            self.wait = WebDriverWait(self.driver, self.timeout)
            logger.info("Đã khởi tạo trình duyệt thành công")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo trình duyệt: {e}")
            raise
        
    def extract_video_info(self, video_url: str) -> Dict[str, Any]:
        """
        Trích xuất thông tin video từ trang video TikTok
        
        Args:
            video_url (str): URL của video TikTok
            
        Returns:
            dict: Thông tin video đã trích xuất
        """
        try:
            # Mở trang video nếu chưa mở
            if self.driver.current_url != video_url:
                self.navigate_to_video(video_url)
            
            # Đợi trang tải xong
            time.sleep(2)
            
            video_info = {
                "video_id": get_video_id_from_url(video_url),
                "video_url": video_url,
                "author": "Unknown",
                "description": "",
                "tags": [],
                "post_time": "",
                "music": "",
                "related_videos": []
            }
            
            # Trích xuất tên tác giả
            try:
                author_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'DivCreatorInfoContainer')]//a")
                video_info["author"] = author_element.text
            except NoSuchElementException:
                pass
            
            # Trích xuất mô tả
            try:
                desc_element = self.driver.find_element(By.XPATH, "//div[contains(@class, 'DivDescriptionContentContainer')]")
                video_info["description"] = desc_element.text
            except NoSuchElementException:
                pass
            
            # Trích xuất hashtags
            try:
                tag_elements = self.driver.find_elements(By.XPATH, "//a[contains(@href, '/tag/')]")
                video_info["tags"] = [tag.text.replace('#', '') for tag in tag_elements]
            except NoSuchElementException:
                pass
            
            # Trích xuất thời gian đăng
            try:
                time_element = self.driver.find_element(By.XPATH, "//span[contains(@class, 'TUXText--weight-medium') and contains(text(), 'trước')]")
                video_info["post_time"] = time_element.text
            except NoSuchElementException:
                pass
            
            # Trích xuất thông tin nhạc
            try:
                music_element = self.driver.find_element(By.XPATH, "//h4[contains(@class, 'music-title')]")
                video_info["music"] = music_element.text
            except NoSuchElementException:
                pass
            
            # Trích xuất video liên quan (nếu có)
            try:
                # Tìm tab "Video có liên quan"
                related_tab = self.driver.find_element(By.XPATH, "//div[contains(text(), 'Video có liên quan')]")
                related_tab.click()
                
                # Đợi videos liên quan tải
                time.sleep(1)
                
                # Lấy danh sách videos
                related_videos = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivItemContainer')]//a")
                
                # Lấy URLs
                for video in related_videos[:5]:  # Lấy tối đa 5 video liên quan
                    try:
                        href = video.get_attribute("href")
                        if href and "video" in href:
                            video_info["related_videos"].append(href)
                    except:
                        continue
            except NoSuchElementException:
                pass
            
            return video_info
        
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất thông tin video: {e}")
            return {
                "video_id": get_video_id_from_url(video_url),
                "video_url": video_url,
                "error": str(e)
            }

    def login_to_tiktok(self, username, password, max_wait=30):
        """
        Đăng nhập vào TikTok bằng username/email và password
        
        Args:
            username (str): Username hoặc email
            password (str): Mật khẩu
            max_wait (int): Thời gian chờ tối đa (giây)
            
        Returns:
            bool: True nếu đăng nhập thành công, False nếu thất bại
        """
        try:
            # Truy cập trang đăng nhập TikTok với ngôn ngữ tiếng Anh
            self.driver.get("https://www.tiktok.com/login?lang=en")
            logger.info("Đã mở trang đăng nhập TikTok")
            
            # Đợi cho các phần tử tải xong
            time.sleep(3)
            
            # Tìm và nhấp vào phương thức đăng nhập bằng email/username
            try:
                # Đầu tiên tìm và chọn "Use phone / email / username"
                use_email_btn = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//div[@data-e2e='channel-item']//div[contains(text(), 'Use phone / email / username')]")
                ))
                use_email_btn.click()
                logger.info("Đã chọn 'Use phone / email / username'")
                
                # Sau đó tìm và chọn "Log in with email or username"
                login_with_email = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Log in with email or username')]")
                ))
                login_with_email.click()
                logger.info("Đã chọn 'Log in with email or username'")
            except Exception as e:
                logger.warning(f"Không tìm thấy nút đăng nhập với email, thử tìm trực tiếp form: {e}")
                # Có thể đã chuyển trực tiếp đến form đăng nhập
                pass
            
            # Đợi form đăng nhập xuất hiện
            time.sleep(2)
            
            # Tìm ô username và nhập username
            username_input = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@name='username' or @placeholder='Email or username']")
            ))
            username_input.clear()
            username_input.send_keys(username)
            logger.info(f"Đã nhập username: {username}")
            
            # Tìm ô password và nhập password
            password_input = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@type='password' or @placeholder='Password']")
            ))
            password_input.clear()
            password_input.send_keys(password)
            logger.info("Đã nhập password")
            
            # Chờ một chút để nút Login được kích hoạt
            time.sleep(1)
            
            # Tìm và nhấp vào nút Login
            login_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-e2e='login-button' or contains(text(), 'Log in')]")
            ))
            login_button.click()
            logger.info("Đã nhấp vào nút Login")
            
            # Đợi để xác định đăng nhập thành công hay thất bại
            start_time = time.time()
            logged_in = False
            
            while time.time() - start_time < max_wait:
                # Kiểm tra nếu đã đăng nhập thành công (thường là chuyển đến trang chủ hoặc có nút profile)
                if "/following" in self.driver.current_url or "/foryou" in self.driver.current_url:
                    logged_in = True
                    break
                    
                # Kiểm tra nếu có thông báo lỗi
                try:
                    error_msg = self.driver.find_element(By.XPATH, "//div[contains(@class, 'error-text')]")
                    if error_msg.is_displayed():
                        logger.error(f"Lỗi đăng nhập: {error_msg.text}")
                        return False
                except:
                    pass
                    
                # Kiểm tra nếu có yêu cầu captcha
                try:
                    # Kiểm tra captcha slider
                    captcha_container = self.driver.find_element(By.ID, "captcha-verify-container-main-page")
                    if captcha_container.is_displayed():
                        logger.warning("Phát hiện Captcha. Vui lòng giải quyết captcha thủ công.")
                        
                        # Hiển thị thông báo cho người dùng (nếu được gọi từ streamlit)
                        if hasattr(self, 'captcha_callback') and callable(self.captcha_callback):
                            self.captcha_callback()
                        
                        # Đợi người dùng giải captcha
                        captcha_solved = False
                        captcha_wait_start = time.time()
                        max_captcha_wait = 60  # Tối đa 60 giây để giải captcha
                        
                        while time.time() - captcha_wait_start < max_captcha_wait:
                            try:
                                # Kiểm tra xem captcha còn hiển thị không
                                captcha_container = self.driver.find_element(By.ID, "captcha-verify-container-main-page")
                                if not captcha_container.is_displayed():
                                    captcha_solved = True
                                    break
                            except NoSuchElementException:
                                captcha_solved = True
                                break
                            
                            time.sleep(3)  # Kiểm tra mỗi 3 giây
                        
                        if captcha_solved:
                            logger.info("Captcha đã được giải quyết.")
                        else:
                            logger.warning("Hết thời gian chờ giải captcha.")
                except NoSuchElementException:
                    # Không tìm thấy captcha
                    pass
                    
                time.sleep(1)
            
            if logged_in:
                logger.info("Đăng nhập thành công!")
                return True
            else:
                logger.error("Đăng nhập thất bại sau khi đợi.")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi trong quá trình đăng nhập: {e}")
            return False

    def get_cookies(self):
        """
        Lấy cookies hiện tại từ trình duyệt
        
        Returns:
            list: Danh sách cookies
        """
        if self.driver:
            return self.driver.get_cookies()
        return []

    def load_cookies(self, cookies):
        """
        Tải cookies vào trình duyệt
        
        Args:
            cookies (list): Danh sách cookies cần tải
            
        Returns:
            bool: True nếu tải thành công, False nếu thất bại
        """
        try:
            # Cần mở một trang web trước khi có thể thêm cookies
            self.driver.get("https://www.tiktok.com")
            
            # Thêm từng cookie vào trình duyệt
            for cookie in cookies:
                try:
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    logger.warning(f"Không thể thêm cookie: {e}")
                    
            # Làm mới trang để áp dụng cookies
            self.driver.refresh()
            
            logger.info("Đã tải cookies thành công")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tải cookies: {e}")
            return False

    def is_logged_in(self):
        """
        Kiểm tra xem đã đăng nhập vào TikTok chưa
        
        Returns:
            bool: True nếu đã đăng nhập, False nếu chưa
        """
        try:
            # Truy cập trang chủ TikTok
            self.driver.get("https://www.tiktok.com")
            
            # Đợi trang tải xong
            time.sleep(3)
            
            # Kiểm tra các phần tử chỉ hiển thị khi đã đăng nhập
            try:
                profile_icon = self.driver.find_element(By.XPATH, "//div[contains(@class, 'ProfileIcon')]")
                return True
            except:
                pass
                
            # Kiểm tra URL sau khi tải trang
            if "/following" in self.driver.current_url or "/foryou" in self.driver.current_url:
                return True
                
            return False
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra trạng thái đăng nhập: {e}")
            return False
        
    def navigate_to_video(self, video_url: str) -> bool:
        """
        Mở trang video TikTok
        
        Args:
            video_url (str): URL của video TikTok
            
        Returns:
            bool: True nếu mở thành công, False nếu thất bại
        """
        try:
            if not validate_tiktok_url(video_url):
                logger.error(f"URL không hợp lệ: {video_url}")
                return False
                
            logger.info(f"Đang mở trang video: {video_url}")
            self.driver.get(video_url)
            
            # Đợi cho đến khi trang tải xong, kiểm tra bằng sự xuất hiện của phần tử video
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "video")))
            
            # Đợi thêm để trang hiển thị hoàn toàn
            time.sleep(2)
            
            logger.info(f"Đã mở trang video: {video_url}")
            return True
        except TimeoutException:
            logger.error(f"Không thể tải trang video: {video_url}")
            return False
        except Exception as e:
            logger.error(f"Lỗi khi mở trang video: {e}")
            return False
    
    def search_tiktok(self, keyword: str, max_videos: int = 10):
        """
        Tìm kiếm video TikTok theo từ khóa
        
        Args:
            keyword (str): Từ khóa tìm kiếm
            max_videos (int): Số lượng video tối đa cần lấy
        
        Returns:
            list: Danh sách thông tin các video tìm thấy
        """
        try:
            # Mã hóa từ khóa tìm kiếm
            keyword_encoded = quote(keyword)
            
            # Truy cập trang tìm kiếm của TikTok
            search_url = f"https://www.tiktok.com/search?q={keyword_encoded}"
            self.driver.get(search_url)
            
            # Đợi cho các video tải
            self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[@data-e2e='search_video-item']")
            ))
            
            # Cuộn trang để tải thêm video
            videos_loaded = 0
            last_videos_count = 0
            attempts = 0
            max_attempts = 10
            
            while videos_loaded < max_videos and attempts < max_attempts:
                # Lấy danh sách các video hiện tại
                video_elements = self.driver.find_elements(By.XPATH, "//div[@data-e2e='search_video-item']")
                videos_loaded = len(video_elements)
                
                if videos_loaded == last_videos_count:
                    attempts += 1
                else:
                    attempts = 0
                    
                last_videos_count = videos_loaded
                
                # Cuộn trang để tải thêm video
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                
                if videos_loaded >= max_videos:
                    break
            
            # Lấy thông tin các video
            video_elements = self.driver.find_elements(By.XPATH, "//div[@data-e2e='search_video-item']")[:max_videos]
            videos_data = []
            
            for element in video_elements:
                try:
                    # Lấy link video
                    link_element = element.find_element(By.XPATH, ".//a")
                    video_url = link_element.get_attribute("href")
                    
                    # Lấy ID video từ URL
                    video_id = get_video_id_from_url(video_url)
                    
                    # Lấy tên tác giả
                    try:
                        author_element = element.find_element(By.XPATH, ".//a[@data-e2e='search-username']")
                        author = author_element.text
                    except NoSuchElementException:
                        author = "Unknown"
                    
                    # Lấy tiêu đề hoặc mô tả
                    try:
                        desc_element = element.find_element(By.XPATH, ".//div[@data-e2e='search-card-desc']")
                        description = desc_element.text
                    except NoSuchElementException:
                        description = ""
                    
                    # Lấy số lượt xem (nếu có)
                    try:
                        views_element = element.find_element(By.XPATH, ".//strong[contains(@class, 'video-count')]")
                        views = views_element.text
                    except NoSuchElementException:
                        views = "0"
                    
                    # Thêm vào danh sách kết quả
                    videos_data.append({
                        "video_id": video_id,
                        "video_url": video_url,
                        "author": author,
                        "description": description,
                        "views": views
                    })
                    
                except Exception as e:
                    logger.warning(f"Lỗi khi trích xuất thông tin video: {e}")
                    continue
            
            return videos_data
        
        except Exception as e:
            logger.error(f"Lỗi khi tìm kiếm video: {e}")
            return []


    def navigate_to_comments(self, video_url: str) -> bool:
        """
        Điều hướng tới trang video và hiển thị phần bình luận
        
        Args:
            video_url (str): URL của video TikTok
                
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Mở trang video
            if not self.navigate_to_video(video_url):
                logger.error(f"Không thể mở trang video: {video_url}")
                return False
            
            logger.info("Đã mở trang video, đang tìm kiếm nút bình luận...")
            
            # Đợi một chút để trang tải hoàn toàn
            time.sleep(3)
            
            # Tìm và nhấp vào icon bình luận
            try:
                comment_icon = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//span[@data-e2e='comment-icon']")
                ))
                comment_icon.click()
                logger.info("Đã nhấp vào biểu tượng bình luận")
                
                # Đợi phần bình luận hiển thị
                time.sleep(2)
                
                # Kiểm tra xem tab bình luận đã mở chưa
                self.wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//div[contains(@class, 'DivCommentListContainer')]")
                ))
                
                logger.info("Đã mở tab bình luận thành công")
                return True
                
            except Exception as e:
                logger.error(f"Lỗi khi mở tab bình luận: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi khi điều hướng đến bình luận: {e}")
            return False

    def load_all_comments(self, max_comments: int = 100, 
                        scroll_pause_time: float = 1.5,
                        progress_callback = None) -> bool:
        """
        Cuộn trang để tải tất cả comments
        
        Args:
            max_comments (int): Số lượng comments tối đa cần crawl
            scroll_pause_time (float): Thời gian dừng giữa các lần cuộn (giây)
            progress_callback (callable): Hàm callback để cập nhật tiến trình
            
        Returns:
            bool: True nếu tải comments thành công, False nếu thất bại
        """
        try:
            # Tìm phần tử comments container
            comments_section = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class, 'DivCommentListContainer')]")
            ))
            
            # Cuộn xuống để tải thêm comments
            comments_loaded = 0
            last_comments_count = 0
            attempts = 0
            max_attempts = 20  # Số lần thử tối đa khi không có thêm comments mới
            
            while comments_loaded < max_comments and attempts < max_attempts:
                # Đếm số lượng comments hiện tại
                comments = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivCommentItemWrapper')]")
                comments_loaded = len(comments)
                
                logger.info(f"Đã tải {comments_loaded}/{max_comments} comments")
                
                # Cập nhật tiến trình
                if progress_callback and callable(progress_callback):
                    progress_percent = min(100, int((comments_loaded / max_comments) * 100))
                    progress_callback(progress_percent, f"Đã tải {comments_loaded}/{max_comments} comments")
                
                if comments_loaded == last_comments_count:
                    attempts += 1
                else:
                    attempts = 0
                    
                last_comments_count = comments_loaded
                
                # Cuộn đến comment cuối cùng
                if comments_loaded > 0:
                    last_comment = comments[-1]
                    self.driver.execute_script("arguments[0].scrollIntoView();", last_comment)
                    
                # Hoặc cuộn phần tử comments container
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", comments_section)
                time.sleep(scroll_pause_time)
                
                # Xem thêm câu trả lời (replies) nếu có
                try:
                    view_more_replies = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivViewRepliesContainer')]")
                    for reply_button in view_more_replies[:5]:  # Chỉ mở 5 cái một lần để tránh quá tải
                        try:
                            self.driver.execute_script("arguments[0].click();", reply_button)
                            time.sleep(0.5)  # Chờ một chút để replies tải
                        except:
                            continue
                except:
                    pass
                
                # Kiểm tra nếu đã đạt đủ số lượng comments cần thiết
                if comments_loaded >= max_comments:
                    logger.info(f"Đã đạt đủ số lượng comments: {comments_loaded}")
                    break
                
                # Kiểm tra nếu không tải được thêm comments
                if attempts >= max_attempts:
                    logger.info(f"Không thể tải thêm comments sau {max_attempts} lần thử")
                    break
            
            # Hoàn thành tiến trình
            if progress_callback and callable(progress_callback):
                progress_callback(100, f"Đã hoàn thành việc tải {comments_loaded} comments")
                
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tải comments: {e}")
            if progress_callback and callable(progress_callback):
                progress_callback(0, f"Lỗi: {str(e)}")
            return False

        def extract_comments(self, max_comments: int = 100, include_replies: bool = True) -> List[Dict[str, Any]]:
            """
            Trích xuất thông tin từ các comments đã tải
            
            Args:
                max_comments (int): Số lượng comments tối đa cần trích xuất
                include_replies (bool): Có trích xuất cả replies hay không
                
            Returns:
                list: Danh sách các comment đã trích xuất
            """
            comments_data = []
            
            try:
                # Tìm tất cả các comment containers
                comment_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivCommentItemWrapper')]")
                
                # Giới hạn số lượng comment cần xử lý
                comments_to_process = min(len(comment_elements), max_comments)
                
                for i in range(comments_to_process):
                    try:
                        comment_element = comment_elements[i]
                        
                        # Trích xuất thông tin username
                        try:
                            username_element = comment_element.find_element(By.XPATH, ".//div[@data-e2e='comment-username-1']//p")
                            username = username_element.text
                        except NoSuchElementException:
                            username = "Unknown"
                        
                        # Trích xuất nội dung comment
                        try:
                            comment_text_element = comment_element.find_element(By.XPATH, ".//span[@data-e2e='comment-level-1']/p")
                            comment_text = comment_text_element.text
                        except NoSuchElementException:
                            comment_text = ""
                        
                        # Trích xuất số lượng likes
                        try:
                            likes_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivLikeContainer')]/span")
                            likes = likes_element.text
                        except NoSuchElementException:
                            likes = "0"
                        
                        # Trích xuất thời gian comment
                        try:
                            time_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivCommentSubContentWrapper')]/span[1]")
                            comment_time = time_element.text
                        except NoSuchElementException:
                            comment_time = "Unknown"
                        
                        # Kiểm tra số lượng replies
                        try:
                            replies_text_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivViewRepliesContainer')]/span")
                            replies_text = replies_text_element.text
                            # Trích xuất số từ chuỗi "Xem X câu trả lời"
                            replies_count = re.search(r'(\d+)', replies_text)
                            replies_count = replies_count.group(1) if replies_count else "0"
                        except NoSuchElementException:
                            replies_count = "0"
                        
                        # Tạo đối tượng comment
                        comment_data = {
                            "username": username,
                            "comment_text": comment_text,
                            "likes": likes,
                            "comment_time": comment_time,
                            "replies_count": replies_count,
                            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        # Thêm vào danh sách kết quả
                        comments_data.append(comment_data)
                        
                        # Trích xuất replies nếu có và được yêu cầu
                        if include_replies and int(replies_count) > 0:
                            try:
                                # Kiểm tra xem replies đã được mở chưa
                                replies_container = comment_element.find_elements(By.XPATH, ".//div[contains(@class, 'DivReplyContainer')]//div[contains(@class, 'DivCommentItemWrapper')]")
                                
                                # Nếu chưa mở, tìm và nhấp vào nút "Xem X câu trả lời"
                                if len(replies_container) == 0:
                                    view_replies_button = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivViewRepliesContainer')]")
                                    self.driver.execute_script("arguments[0].click();", view_replies_button)
                                    time.sleep(1)  # Đợi để replies tải
                                    
                                    # Tìm lại container sau khi đã mở
                                    replies_container = comment_element.find_elements(By.XPATH, ".//div[contains(@class, 'DivReplyContainer')]//div[contains(@class, 'DivCommentItemWrapper')]")
                                
                                # Trích xuất thông tin từ mỗi reply
                                for reply_element in replies_container:
                                    try:
                                        # Trích xuất username của reply
                                        reply_username = reply_element.find_element(By.XPATH, ".//div[@data-e2e='comment-username-2']//p").text
                                        
                                        # Trích xuất nội dung reply
                                        reply_text = reply_element.find_element(By.XPATH, ".//span[@data-e2e='comment-level-2']/p").text
                                        
                                        # Trích xuất số likes của reply
                                        try:
                                            reply_likes = reply_element.find_element(By.XPATH, ".//div[contains(@class, 'DivLikeContainer')]/span").text
                                        except:
                                            reply_likes = "0"
                                        
                                        # Trích xuất thời gian của reply
                                        try:
                                            reply_time = reply_element.find_element(By.XPATH, ".//div[contains(@class, 'DivCommentSubContentWrapper')]/span[1]").text
                                        except:
                                            reply_time = "Unknown"
                                        
                                        # Tạo đối tượng reply
                                        reply_data = {
                                            "parent_comment_username": username,
                                            "username": reply_username,
                                            "comment_text": reply_text,
                                            "likes": reply_likes,
                                            "comment_time": reply_time,
                                            "is_reply": True,
                                            "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                        }
                                        
                                        # Thêm vào danh sách kết quả
                                        comments_data.append(reply_data)
                                        
                                    except Exception as e:
                                        logger.warning(f"Lỗi khi trích xuất reply: {e}")
                                        continue
                                        
                            except Exception as e:
                                logger.warning(f"Lỗi khi trích xuất replies cho comment {i}: {e}")
                        
                    except Exception as e:
                        logger.error(f"Lỗi khi trích xuất comment thứ {i}: {e}")
                        continue
                
                logger.info(f"Đã trích xuất {len(comments_data)} comments và replies")
                return comments_data
                
            except Exception as e:
                logger.error(f"Lỗi khi trích xuất comments: {e}")
                return comments_data
    
    def load_all_comments(self, max_comments: int = 100, 
                      scroll_pause_time: float = 1.5,
                      progress_callback = None) -> bool:
        """
        Cuộn trang để tải tất cả comments
        
        Args:
            max_comments (int): Số lượng comments tối đa cần crawl
            scroll_pause_time (float): Thời gian dừng giữa các lần cuộn (giây)
            progress_callback (callable): Hàm callback để cập nhật tiến trình
            
        Returns:
            bool: True nếu tải comments thành công, False nếu thất bại
        """
        try:
            # Tìm phần tử comments container
            comments_section = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//div[contains(@class, 'DivCommentListContainer')]")
            ))
            
            # Cuộn xuống để tải thêm comments
            comments_loaded = 0
            last_comments_count = 0
            attempts = 0
            max_attempts = 20  # Số lần thử tối đa khi không có thêm comments mới
            
            while comments_loaded < max_comments and attempts < max_attempts:
                # Đếm số lượng comments hiện tại
                comments = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivCommentItemWrapper')]")
                comments_loaded = len(comments)
                
                logger.info(f"Đã tải {comments_loaded}/{max_comments} comments")
                
                # Cập nhật tiến trình
                if progress_callback and callable(progress_callback):
                    progress_percent = min(100, int((comments_loaded / max_comments) * 100))
                    progress_callback(progress_percent, f"Đã tải {comments_loaded}/{max_comments} comments")
                
                if comments_loaded == last_comments_count:
                    attempts += 1
                else:
                    attempts = 0
                    
                last_comments_count = comments_loaded
                
                # Cuộn đến comment cuối cùng
                if comments_loaded > 0:
                    last_comment = comments[-1]
                    self.driver.execute_script("arguments[0].scrollIntoView();", last_comment)
                    
                # Hoặc cuộn phần tử comments container
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", comments_section)
                time.sleep(scroll_pause_time)
                
                # Xem thêm câu trả lời (replies) nếu có
                try:
                    view_more_replies = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivViewRepliesContainer')]")
                    for reply_button in view_more_replies[:5]:  # Chỉ mở 5 cái một lần để tránh quá tải
                        try:
                            self.driver.execute_script("arguments[0].click();", reply_button)
                            time.sleep(0.5)  # Chờ một chút để replies tải
                        except:
                            continue
                except:
                    pass
                
                # Kiểm tra nếu đã đạt đủ số lượng comments cần thiết
                if comments_loaded >= max_comments:
                    logger.info(f"Đã đạt đủ số lượng comments: {comments_loaded}")
                    break
                
                # Kiểm tra nếu không tải được thêm comments
                if attempts >= max_attempts:
                    logger.info(f"Không thể tải thêm comments sau {max_attempts} lần thử")
                    break
            
            # Hoàn thành tiến trình
            if progress_callback and callable(progress_callback):
                progress_callback(100, f"Đã hoàn thành việc tải {comments_loaded} comments")
                
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tải comments: {e}")
            if progress_callback and callable(progress_callback):
                progress_callback(0, f"Lỗi: {str(e)}")
            return False

    
    def extract_comments(self, max_comments: int = 100, include_replies: bool = True) -> List[Dict[str, Any]]:
        """
        Trích xuất thông tin từ các comments đã tải
        
        Args:
            max_comments (int): Số lượng comments tối đa cần trích xuất
            include_replies (bool): Có trích xuất cả replies hay không
            
        Returns:
            list: Danh sách các comment đã trích xuất
        """
        comments_data = []
        
        try:
            # Tìm tất cả các comment containers
            comment_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'DivCommentItemWrapper')]")
            
            # Giới hạn số lượng comment cần xử lý
            comments_to_process = min(len(comment_elements), max_comments)
            
            for i in range(comments_to_process):
                try:
                    comment_element = comment_elements[i]
                    
                    # Trích xuất thông tin username
                    try:
                        username_element = comment_element.find_element(By.XPATH, ".//div[@data-e2e='comment-username-1']//p")
                        username = username_element.text
                    except NoSuchElementException:
                        username = "Unknown"
                    
                    # Trích xuất nội dung comment
                    try:
                        comment_text_element = comment_element.find_element(By.XPATH, ".//span[@data-e2e='comment-level-1']/p")
                        comment_text = comment_text_element.text
                    except NoSuchElementException:
                        comment_text = ""
                    
                    # Trích xuất số lượng likes
                    try:
                        likes_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivLikeContainer')]/span")
                        likes = likes_element.text
                    except NoSuchElementException:
                        likes = "0"
                    
                    # Trích xuất thời gian comment
                    try:
                        time_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivCommentSubContentWrapper')]/span[1]")
                        comment_time = time_element.text
                    except NoSuchElementException:
                        comment_time = "Unknown"
                    
                    # Kiểm tra số lượng replies
                    try:
                        replies_text_element = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivViewRepliesContainer')]/span")
                        replies_text = replies_text_element.text
                        # Trích xuất số từ chuỗi "Xem X câu trả lời"
                        replies_count = re.search(r'(\d+)', replies_text)
                        replies_count = replies_count.group(1) if replies_count else "0"
                    except NoSuchElementException:
                        replies_count = "0"
                    
                    # Tạo đối tượng comment
                    comment_data = {
                        "username": username,
                        "comment_text": comment_text,
                        "likes": likes,
                        "comment_time": comment_time,
                        "replies_count": replies_count,
                        "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # Thêm vào danh sách kết quả
                    comments_data.append(comment_data)
                    
                    # Trích xuất replies nếu có và được yêu cầu
                    if include_replies and int(replies_count) > 0:
                        try:
                            # Kiểm tra xem replies đã được mở chưa
                            replies_container = comment_element.find_elements(By.XPATH, ".//div[contains(@class, 'DivReplyContainer')]//div[contains(@class, 'DivCommentItemWrapper')]")
                            
                            # Nếu chưa mở, tìm và nhấp vào nút "Xem X câu trả lời"
                            if len(replies_container) == 0:
                                view_replies_button = comment_element.find_element(By.XPATH, ".//div[contains(@class, 'DivViewRepliesContainer')]")
                                self.driver.execute_script("arguments[0].click();", view_replies_button)
                                time.sleep(1)  # Đợi để replies tải
                                
                                # Tìm lại container sau khi đã mở
                                replies_container = comment_element.find_elements(By.XPATH, ".//div[contains(@class, 'DivReplyContainer')]//div[contains(@class, 'DivCommentItemWrapper')]")
                            
                            # Trích xuất thông tin từ mỗi reply
                            for reply_element in replies_container:
                                try:
                                    # Trích xuất username của reply
                                    reply_username = reply_element.find_element(By.XPATH, ".//div[@data-e2e='comment-username-2']//p").text
                                    
                                    # Trích xuất nội dung reply
                                    reply_text = reply_element.find_element(By.XPATH, ".//span[@data-e2e='comment-level-2']/p").text
                                    
                                    # Trích xuất số likes của reply
                                    try:
                                        reply_likes = reply_element.find_element(By.XPATH, ".//div[contains(@class, 'DivLikeContainer')]/span").text
                                    except:
                                        reply_likes = "0"
                                    
                                    # Trích xuất thời gian của reply
                                    try:
                                        reply_time = reply_element.find_element(By.XPATH, ".//div[contains(@class, 'DivCommentSubContentWrapper')]/span[1]").text
                                    except:
                                        reply_time = "Unknown"
                                    
                                    # Tạo đối tượng reply
                                    reply_data = {
                                        "parent_comment_username": username,
                                        "username": reply_username,
                                        "comment_text": reply_text,
                                        "likes": reply_likes,
                                        "comment_time": reply_time,
                                        "is_reply": True,
                                        "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    }
                                    
                                    # Thêm vào danh sách kết quả
                                    comments_data.append(reply_data)
                                    
                                except Exception as e:
                                    logger.warning(f"Lỗi khi trích xuất reply: {e}")
                                    continue
                                    
                        except Exception as e:
                            logger.warning(f"Lỗi khi trích xuất replies cho comment {i}: {e}")
                    
                except Exception as e:
                    logger.error(f"Lỗi khi trích xuất comment thứ {i}: {e}")
                    continue
            
            logger.info(f"Đã trích xuất {len(comments_data)} comments và replies")
            return comments_data
            
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất comments: {e}")
            return comments_data
    
    def login_to_tiktok(self, username, password, max_wait=30):
        """
        Đăng nhập vào TikTok bằng username/email và password
        
        Args:
            username (str): Username hoặc email
            password (str): Mật khẩu
            max_wait (int): Thời gian chờ tối đa (giây)
            
        Returns:
            bool: True nếu đăng nhập thành công, False nếu thất bại
        """
        try:
            # Truy cập trang đăng nhập TikTok
            self.driver.get("https://www.tiktok.com/login?lang=en")
            logger.info("Đã mở trang đăng nhập TikTok")
            
            # Đợi cho các phần tử tải xong
            time.sleep(3)
            
            # Tìm và nhấp vào phương thức đăng nhập bằng email/username
            try:
                # Đầu tiên tìm và chọn "Use phone / email / username"
                use_email_btn = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//div[@data-e2e='channel-item']//div[contains(text(), 'Use phone / email / username')]")
                ))
                use_email_btn.click()
                logger.info("Đã chọn 'Use phone / email / username'")
                
                # Sau đó tìm và chọn "Log in with email or username"
                login_with_email = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//a[contains(text(), 'Log in with email or username')]")
                ))
                login_with_email.click()
                logger.info("Đã chọn 'Log in with email or username'")
            except Exception as e:
                logger.warning(f"Không tìm thấy nút đăng nhập với email, thử tìm trực tiếp form: {e}")
                # Có thể đã chuyển trực tiếp đến form đăng nhập
                pass
            
            # Đợi form đăng nhập xuất hiện
            time.sleep(2)
            
            # Tìm ô username và nhập username
            username_input = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@name='username' or @placeholder='Email or username']")
            ))
            username_input.clear()
            username_input.send_keys(username)
            logger.info(f"Đã nhập username: {username}")
            
            # Tìm ô password và nhập password
            password_input = self.wait.until(EC.presence_of_element_located(
                (By.XPATH, "//input[@type='password' or @placeholder='Password']")
            ))
            password_input.clear()
            password_input.send_keys(password)
            logger.info("Đã nhập password")
            
            # Chờ một chút để nút Login được kích hoạt
            time.sleep(1)
            
            # Tìm và nhấp vào nút Login
            login_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-e2e='login-button' or contains(text(), 'Log in')]")
            ))
            login_button.click()
            logger.info("Đã nhấp vào nút Login")
            
            # Đợi để xác định đăng nhập thành công hay thất bại
            start_time = time.time()
            logged_in = False
            
            while time.time() - start_time < max_wait:
                # Kiểm tra nếu đã đăng nhập thành công (thường là chuyển đến trang chủ hoặc có nút profile)
                if "/following" in self.driver.current_url or "/foryou" in self.driver.current_url:
                    logged_in = True
                    break
                    
                # Kiểm tra nếu có thông báo lỗi
                try:
                    error_msg = self.driver.find_element(By.XPATH, "//div[contains(@class, 'error-text')]")
                    if error_msg.is_displayed():
                        logger.error(f"Lỗi đăng nhập: {error_msg.text}")
                        return False
                except:
                    pass
                    
                # Kiểm tra nếu có yêu cầu captcha
                try:
                    captcha_container = self.driver.find_element(By.XPATH, "//div[contains(@class, 'captcha_container')]")
                    if captcha_container.is_displayed():
                        logger.warning("Phát hiện Captcha. Vui lòng giải quyết captcha thủ công.")
                        # Đợi người dùng giải captcha
                        time.sleep(15)  # Thời gian để người dùng giải captcha
                except:
                    pass
                    
                time.sleep(1)
            
            if logged_in:
                logger.info("Đăng nhập thành công!")
                return True
            else:
                logger.error("Đăng nhập thất bại sau khi đợi.")
                return False
                
        except Exception as e:
            logger.error(f"Lỗi trong quá trình đăng nhập: {e}")
            return False
    
    def save_to_csv(self, comments_data: List[Dict[str, Any]], 
                    output_file: Union[str, Path] = "tiktok_comments.csv") -> bool:
        """
        Lưu dữ liệu comments vào file CSV
        
        Args:
            comments_data (list): Danh sách các comment
            output_file (str/Path): Tên file CSV đầu ra
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        try:
            if not comments_data:
                logger.warning("Không có dữ liệu comments để lưu")
                return False
            
            # Chuyển đổi sang Path object
            output_path = Path(output_file)
            
            # Tạo thư mục nếu chưa tồn tại
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['username', 'comment_text', 'likes', 'comment_time', 
                             'replies_count', 'crawled_at']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for comment in comments_data:
                    writer.writerow(comment)
            
            logger.info(f"Đã lưu {len(comments_data)} comments vào file {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu file CSV: {e}")
            return False
    
    def save_to_json(self, comments_data: List[Dict[str, Any]], 
                     output_file: Union[str, Path] = "tiktok_comments.json") -> bool:
        """
        Lưu dữ liệu comments vào file JSON
        
        Args:
            comments_data (list): Danh sách các comment
            output_file (str/Path): Tên file JSON đầu ra
            
        Returns:
            bool: True nếu lưu thành công, False nếu thất bại
        """
        try:
            if not comments_data:
                logger.warning("Không có dữ liệu comments để lưu")
                return False
            
            # Chuyển đổi sang Path object
            output_path = Path(output_file)
            
            # Tạo thư mục nếu chưa tồn tại
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as jsonfile:
                json.dump(comments_data, jsonfile, ensure_ascii=False, indent=4)
            
            logger.info(f"Đã lưu {len(comments_data)} comments vào file {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu file JSON: {e}")
            return False
    
    def close(self):
        """Đóng trình duyệt"""
        if self.driver:
            self.driver.quit()
            logger.info("Đã đóng trình duyệt")

def main():
    """Hàm chính khi chạy như một module độc lập"""
    parser = argparse.ArgumentParser(description='TikTok Comment Crawler')
    parser.add_argument('--url', required=True, help='URL của video TikTok')
    parser.add_argument('--output', default='tiktok_comments.csv', help='File CSV đầu ra')
    parser.add_argument('--max-comments', type=int, default=100, help='Số lượng comments tối đa cần crawl')
    parser.add_argument('--headless', action='store_true', help='Chạy Chrome ở chế độ headless')
    parser.add_argument('--chromedriver-path', help='Đường dẫn đến chromedriver')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Định dạng đầu ra')
    
    args = parser.parse_args()
    
    # Thiết lập logging
    setup_logger("tiktok_crawler")
    
    # Khởi tạo crawler
    crawler = TikTokCommentCrawler(headless=args.headless, chromedriver_path=args.chromedriver_path)
    
    try:
        # Mở trang video
        if crawler.navigate_to_video(args.url):
            # Cho phép trang tải xong
            time.sleep(5)
            
            # Tải tất cả comments
            crawler.load_all_comments(max_comments=args.max_comments)
            
            # Trích xuất comments
            comments_data = crawler.extract_comments(max_comments=args.max_comments)
            
            # Lưu comments vào file theo định dạng yêu cầu
            if args.format == 'csv':
                crawler.save_to_csv(comments_data, output_file=args.output)
            else:  # json
                output_file = args.output.replace('.csv', '.json') if args.output.endswith('.csv') else args.output
                crawler.save_to_json(comments_data, output_file=output_file)
    finally:
        crawler.close()

if __name__ == "__main__":
    main()