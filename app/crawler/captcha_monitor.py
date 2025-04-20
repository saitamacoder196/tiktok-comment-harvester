import threading
import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

class CaptchaMonitor:
    """
    Giám sát và phát hiện captcha TikTok trong một luồng riêng biệt
    """
    def __init__(self, driver, callback=None):
        """
        Khởi tạo monitor
        
        Args:
            driver: WebDriver của Selenium
            callback: Hàm callback được gọi khi phát hiện captcha
        """
        self.driver = driver
        self.callback = callback
        self.is_running = False
        self.is_paused = False
        self.monitor_thread = None
        self.captcha_solved = threading.Event()
        
    def start(self):
        """Bắt đầu giám sát captcha"""
        if self.monitor_thread is not None and self.monitor_thread.is_alive():
            return
            
        self.is_running = True
        self.is_paused = False
        self.captcha_solved.clear()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
    def stop(self):
        """Dừng giám sát captcha"""
        self.is_running = False
        if self.monitor_thread is not None:
            self.monitor_thread.join(timeout=1.0)
        self.monitor_thread = None
        
    def pause(self):
        """Tạm dừng giám sát"""
        self.is_paused = True
        
    def resume(self):
        """Tiếp tục giám sát sau khi tạm dừng"""
        self.is_paused = False
        
    def mark_solved(self):
        """Đánh dấu captcha đã được giải"""
        self.captcha_solved.set()
        
    def wait_for_solution(self, timeout=300):
        """
        Đợi cho đến khi captcha được giải
        
        Args:
            timeout: Thời gian tối đa đợi (giây)
            
        Returns:
            bool: True nếu captcha đã được giải, False nếu hết thời gian
        """
        return self.captcha_solved.wait(timeout)
    
    def _monitor_loop(self):
        """Vòng lặp giám sát captcha"""
        check_interval = 1.0  # Kiểm tra mỗi 1 giây
        
        while self.is_running:
            if not self.is_paused:
                try:
                    # Kiểm tra captcha container
                    captcha_element = self.driver.find_element(By.ID, "captcha-verify-container-main-page")
                    
                    if captcha_element.is_displayed():
                        # Phát hiện captcha
                        self.is_paused = True
                        self.captcha_solved.clear()
                        
                        # Gọi callback nếu có
                        if self.callback and callable(self.callback):
                            self.callback(captcha_element)
                            
                        # Đợi cho đến khi captcha được giải
                        while self.is_running and captcha_element.is_displayed():
                            try:
                                time.sleep(1.0)
                                captcha_element = self.driver.find_element(By.ID, "captcha-verify-container-main-page")
                            except NoSuchElementException:
                                # Captcha đã biến mất, có thể đã được giải
                                break
                                
                        # Captcha đã được giải
                        self.captcha_solved.set()
                        self.is_paused = False
                except NoSuchElementException:
                    # Không tìm thấy captcha, tiếp tục kiểm tra
                    pass
                except Exception as e:
                    print(f"Lỗi khi kiểm tra captcha: {e}")
                    
            time.sleep(check_interval)