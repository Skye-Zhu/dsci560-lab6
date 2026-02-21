import re

def parse_api(text):
    # 支持：API #33-053-02102 / API# 33-053-02102 / API 33-053-02102
    m = re.search(r'API\s*#?\s*(\d{2}-\d{3}-\d{5})', text, re.IGNORECASE)
    if m:
        return m.group(1)

    # 兜底：OCR 可能丢掉连字符，允许 10 位数字（3305302102）
    m2 = re.search(r'API\s*#?\s*(\d{10})', text, re.IGNORECASE)
    if m2:
        s = m2.group(1)
        return f"{s[0:2]}-{s[2:5]}-{s[5:10]}"

    return "N/A"

def parse_operator(text):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    def bad(op: str) -> bool:
        # 太像井名/编号/表头
        if re.search(r"(well name|well file|address|city|zip|county|township|range|section)", op, re.IGNORECASE):
            return True
        if "#" in op:  # 很多井名里会有 #31-10 这种
            return True
        return False

    # 1) NAME OF OPERATOR 下一行
    for i, ln in enumerate(lines):
        if re.search(r"NAME OF OPERATOR", ln, re.IGNORECASE):
            if i + 1 < len(lines):
                op = lines[i + 1].strip()
                if op and not bad(op):
                    return op

    # 2) Operator : XXX
    for ln in lines:
        m = re.search(r"\bOperator\s*:\s*(.+)$", ln, re.IGNORECASE)
        if m:
            op = m.group(1).strip()
            if op and not bad(op):
                return op

    # 3) 兜底：找最像“公司名”的行（Inc/LLC/Corp/Company/LP…）
    suffix_pat = re.compile(r"\b(Inc\.?|LLC|L\.L\.C\.|Corp\.?|Corporation|Company|Co\.|LP|L\.P\.|Ltd\.?)\b", re.IGNORECASE)
    for ln in lines:
        if suffix_pat.search(ln) and not bad(ln):
            # 防止抓到整段地址：如果太长就不要
            if 3 <= len(ln) <= 60:
                return ln

    return "N/A"

'''def parse_county_state(text, permit_no=None):
    lines = text.splitlines()

    if permit_no and permit_no != "N/A":
        for line in lines:
            if permit_no in line and "|" in line:
                m = re.search(rf"\b{re.escape(permit_no)}\b.*?([A-Za-z]{{3,}})\s*\|\s*([A-Z]{{2}})\b", line)
                if m:
                    return m.group(1), m.group(2)


    m2 = re.search(r"\b\d{5}\b.*?([A-Za-z]{3,})\s*\|\s*([A-Z]{2})\b", text)
    if m2:
        return m2.group(1), m2.group(2)

    return "N/A", "N/A"'''
def parse_county_state(text, permit_no=None):
    # 优先抓 "County : XXX"
    m = re.search(r"County\s*:\s*([A-Za-z]+)", text, re.IGNORECASE)
    if m:
        county = m.group(1).strip()
        # 这批几乎都是 ND（North Dakota）；若你后面有别州我们再扩展
        return county, "ND"

    # 其次：你之前稳定的 permit_no 行规则（County | ST）
    if permit_no and permit_no != "N/A":
        for line in text.splitlines():
            if permit_no in line and "|" in line:
                pattern = r"\b" + re.escape(permit_no) + r"\b.*?([A-Za-z]{3,})\s*\|\s*([A-Z]{2})\b"
                m2 = re.search(pattern, line)
                if m2:
                    return m2.group(1), m2.group(2)

    # 兜底：全文找 “CountyName ND”
    m3 = re.search(r"\b([A-Za-z]{3,})\s+ND\b", text)
    if m3:
        return m3.group(1), "ND"

    return "N/A", "N/A"


import re

def parse_well_name_number(text):
    lines = [ln.rstrip() for ln in text.splitlines()]
    # 保留原顺序，去掉完全空行
    lines = [ln.strip() for ln in lines if ln.strip()]

    # 设备/电话等噪声强排除
    noise_keywords = [
        "pump unit", "lufkin", "ajax", "engine", "eng", "api gravity",
        "telephone", "phone", "fax", "email", "address", "city", "zip",
        "witness", "signature", "production", "authorization to purchase",
    ]
    def is_noise(line: str) -> bool:
        low = line.lower()
        if any(k in low for k in noise_keywords):
            return True
        # 电话格式
        if re.search(r"\(\d{3}\)\s*\d{3}[-\s]\d{4}", line):
            return True
        if re.search(r"\b\d{3}[-\s]\d{3}[-\s]\d{4}\b", line):
            return True
        return False

    def looks_like_name(line: str) -> bool:
        # 井名一般：有字母 + 有数字，长度适中
        if is_noise(line):
            return False
        if not re.search(r"[A-Za-z]", line):
            return False
        if not re.search(r"\d", line):
            return False
        if len(line) < 6 or len(line) > 80:
            return False
        return True

    def split_name_number(line: str):
        m = re.search(r"^(.*?)(\b\d{1,4}[-/]\d{1,4}[A-Za-z]?\b|\b\d{1,4}[A-Za-z]?\b)$", line)
        if m:
            name = m.group(1).strip(" -|")
            num = m.group(2).strip()
            return (name if name else line), num
        return line, "N/A"

    # Strategy 1: 抓信件里的 "RE:" 段落（你这个 W11920 就是这样）
    for i, ln in enumerate(lines):
        if re.fullmatch(r"RE:?", ln, flags=re.IGNORECASE):
            # RE: 下一行通常就是井名
            if i + 1 < len(lines):
                cand = lines[i + 1].strip()
                if looks_like_name(cand):
                    name, num = split_name_number(cand)
                    return name, num, cand

    # Strategy 2: 抓包含 "Well Name and Number" 的位置，取后面几行
    for i, ln in enumerate(lines):
        if re.search(r"Well\s+Name\s+and\s+Number", ln, re.IGNORECASE):
            for j in range(i + 1, min(i + 12, len(lines))):
                cand = lines[j]
                if looks_like_name(cand):
                    name, num = split_name_number(cand)
                    return name, num, cand

    # Strategy 3: API 附近（兜底）
    api_pat = re.compile(r"\b\d{2}-\d{3}-\d{5}\b")
    api_idx = None
    for i, ln in enumerate(lines):
        if api_pat.search(ln) or re.search(r"API\s*#?\s*\d", ln, re.IGNORECASE):
            api_idx = i
            break

    if api_idx is not None:
        start = max(0, api_idx - 50)
        end = min(len(lines), api_idx + 50)
        candidates = []
        for j in range(start, end):
            cand = lines[j]
            if looks_like_name(cand):
                score = 0
                if re.search(r"\d{1,4}[-/]\d{1,4}[A-Za-z]?", cand):
                    score += 3
                score += min(10, len(cand) // 10)
                candidates.append((score, cand))
        if candidates:
            candidates.sort(reverse=True, key=lambda x: x[0])
            best = candidates[0][1]
            name, num = split_name_number(best)
            return name, num, best

    return "N/A", "N/A", "N/A"

def parse_well_info(text):
    data = {}

    # Permit number（先抓 5 位数）
    permit_match = re.search(r"Well\s*File\s*(No\.?|Number)\s*[:#]?\s*(\d{5})", text, re.IGNORECASE)
    data["permit_no"] = permit_match.group(2) if permit_match else "N/A"


    # County + State（调用外部函数）
    data["operator"] = parse_operator(text)
    county, state = parse_county_state(text, data["permit_no"])
    data["api"] = parse_api(text)
    well_name, well_number, raw = parse_well_name_number(text)
    data["well_name"] = well_name
    data["well_number"] = well_number
    data["well_name_raw"] = raw
    data["county"] = county
    data["state"] = state

    return data