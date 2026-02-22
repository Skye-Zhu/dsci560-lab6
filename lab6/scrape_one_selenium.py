import re
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

def clean(s):
    if not s:
        return "N/A"
    return re.sub(r"\s+", " ", s).strip()

def find_value_by_label(page_text, label):
    m = re.search(rf"{label}\s*\n\s*([^\n]+)", page_text, re.IGNORECASE)
    return clean(m.group(1)) if m else "N/A"

def scrape(api):
    url = f"https://www.drillingedge.com/search?q={api}"

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1400,900")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )

    try:
        driver.get(url)
        time.sleep(3)  
        links = driver.find_elements(By.TAG_NAME, "a")
        target = None
        for a in links:
            href = a.get_attribute("href") or ""
            if "/wells/" in href and href != "https://www.drillingedge.com/wells":
                target = href
                break

        if not target:
            with open("debug_search_rendered.txt", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            return None

        driver.get(target)
        time.sleep(3)

        page_text = driver.find_element(By.TAG_NAME, "body").text

        return {
            "well_url": target,
            "well_status": find_value_by_label(page_text, "Well Status"),
            "well_type": find_value_by_label(page_text, "Well Type"),
            "closest_city": find_value_by_label(page_text, "Closest City"),
            "barrels_oil": find_value_by_label(page_text, "Barrels of Oil Produced"),
            "barrels_gas": find_value_by_label(page_text, "MCF of Gas Produced"),
        }

    finally:
        driver.quit()

if __name__ == "__main__":
    api = "33-053-02102"
    result = scrape(api)
    print(result)