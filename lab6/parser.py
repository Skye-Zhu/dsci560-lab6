import re

def parse_county_state(text, permit_no=None):
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

    return "N/A", "N/A"


def parse_well_info(text):
    data = {}

    # Permit number（先抓 5 位数）
    permit_match = re.search(r'\b\d{5}\b', text)
    data["permit_no"] = permit_match.group(0) if permit_match else "N/A"

    # Operator
    op_match = re.search(r'NAME OF OPERATOR\s*\n(.+)', text, re.IGNORECASE)
    if op_match:
        data["operator"] = op_match.group(1).strip()
    else:
        # 备用（如果你想更通用，后面我们会做更稳的）
        op_match = re.search(r'PANTERRA PETROLEUM', text)
        data["operator"] = op_match.group(0) if op_match else "N/A"

    # County + State（调用外部函数）
    county, state = parse_county_state(text, data["permit_no"])
    data["county"] = county
    data["state"] = state

    return data