'''import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def find_well_url_by_api(api: str, headless: bool = True, timeout: int = 20):
    """
    用 Selenium 打开 drillingedge 搜索页，找到 href 里包含 api 的第一个链接。
    成功返回完整 well page url；失败返回 None
    """
    api = api.strip()
    url = f"https://www.drillingedge.com/search?q={api}"

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

    try:
        driver.get(url)

        # 等待页面渲染出至少一个包含 api 的链接
        wait = WebDriverWait(driver, timeout)
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, f"//a[contains(@href, '{api}')]")
            )
        )

        links = driver.find_elements(By.XPATH, f"//a[contains(@href, '{api}')]")
        for a in links:
            href = a.get_attribute("href") or ""
            if api in href and "/wells/" in href:
                return href

        # 兜底：返回第一个包含 api 的 href
        if links:
            return links[0].get_attribute("href")

        return None

    except Exception:
        return None

    finally:
        driver.quit()'''

# drillingedge_fallback.py
import requests
from bs4 import BeautifulSoup

BASE = "https://www.drillingedge.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def find_well_url_by_api(api: str, timeout: int = 20) -> str | None:
    """
    通过 drillingedge 搜索页：/search?q=<API>
    找到包含 /wells/ 且 href 里包含 api 的链接，返回第一个匹配的 well page URL
    """
    search_url = f"{BASE}/search?q={api}"
    r = requests.get(search_url, headers=HEADERS, timeout=timeout)
    if r.status_code != 200:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # 最稳：找 href 同时满足 "/wells/" + api
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/wells/" in href and api in href:
            if href.startswith("http"):
                return href
            return BASE + href

    return None