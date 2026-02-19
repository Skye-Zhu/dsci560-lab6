from parser import parse_well_info

with open("output/W11745.pdf.txt", "r", encoding="utf-8") as f:
    text = f.read()

result = parse_well_info(text)
print(result)
