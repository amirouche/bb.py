"""List top-level functions in bb.py below 80% coverage, least covered first."""
import xml.etree.ElementTree as ET

THRESHOLD = 80

tree = ET.parse("coverage.xml")
lines_elem = tree.find('.//class[@name="bb.py"]/lines')

covered = {int(l.get("number")) for l in lines_elem if int(l.get("hits")) > 0}
missed = {int(l.get("number")) for l in lines_elem if int(l.get("hits")) == 0}

with open("bb.py") as f:
    bb = f.readlines()

funcs = []
i = 0
while i < len(bb):
    line = bb[i]
    stripped = line.strip()
    indent = len(line) - len(line.lstrip())
    if stripped.startswith("def ") and "(" in stripped and indent == 0:
        name = stripped.split("(")[0].replace("def ", "")
        fc = fm = 0
        j = i + 1
        while j < len(bb):
            next_line = bb[j]
            next_stripped = next_line.strip()
            next_indent = len(next_line) - len(next_line.lstrip())
            # Stop at the next top-level def/class
            if next_indent == 0 and next_stripped and not next_stripped.startswith("#") and not next_stripped.startswith('"""') and not next_stripped.startswith("'"):
                break
            n = j + 1  # 1-indexed line number
            if n in covered:
                fc += 1
            elif n in missed:
                fm += 1
            j += 1
        total = fc + fm
        pct = fc / total * 100 if total else 0
        if pct < THRESHOLD:
            funcs.append((name, fc, fm, total, pct))
    i += 1

funcs.sort(key=lambda x: (x[4], -x[3]))

total_covered = len(covered)
total_all = len(covered) + len(missed)
print(f"Coverage: {total_covered}/{total_all} lines ({total_covered / total_all * 100:.0f}%)")
print()
print(f"{'Function':<45} {'Covered':>7} {'Missed':>7} {'Total':>7} {'%':>5}")
print("-" * 75)
for name, fc, fm, total, pct in funcs:
    print(f"{name:<45} {fc:>7} {fm:>7} {total:>7} {pct:>4.0f}%")
