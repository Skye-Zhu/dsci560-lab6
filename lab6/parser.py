import re

def parse_well_info(text):
    data = {}

    # Permit number
    permit_match = re.search(r'\b\d{5}\b', text)
    data["permit_no"] = permit_match.group(0) if permit_match else "N/A"

    op_match = re.search(r'NAME OF OPERATOR\s*\n(.+)', text, re.IGNORECASE)

    if op_match:
        data["operator"] = op_match.group(1).strip()
    else:
        op_match = re.search(r'PANTERRA PETROLEUM', text)
        data["operator"] = op_match.group(0) if op_match else "N/A"

    # County + State
    county_state = re.search(r'(\w+)\s+ND', text)
    if county_state:
        data["county"] = county_state.group(1)
        data["state"] = "ND"
    else:
        data["county"] = "N/A"
        data["state"] = "N/A"

    return data