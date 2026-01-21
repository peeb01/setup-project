import re
import statistics

log_file = "ollama.log"

times = []

with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        m = re.search(r"\|\s*([\d\.]+)s\s*\|", line)
        if m:
            times.append(float(m.group(1)))

if not times:
    print("not found log in this time")
    exit()

times.sort()

avg = statistics.mean(times)
p95 = times[int(len(times) * 0.95) - 1]

print(f"num request     : {len(times)}")
print(f"avg             : {avg:.3f} s")
print(f"min             : {min(times):.3f} s")
print(f"max             : {max(times):.3f} s")
print(f"p95             : {p95:.3f} s")
