mkdir -p cos-downloads
python3 << 'PYEOF'
import os
from qcloud_cos import CosConfig, CosService

secret_id  = os.environ['TENCENT_SECRET_ID']
secret_key = os.environ['TENCENT_SECRET_KEY']
bucket     = os.environ['TENCENT_COS_BUCKET']
region     = os.environ.get('TENCENT_COS_REGION', 'ap-guangzhou')

config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key)
client = CosService(CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key))

file_count = int(os.environ.get('FILE_COUNT', '0'))
downloaded = []

for i in range(file_count):
key = os.environ.get(f'FILE_{i}', '')
if not key:
continue
fname = os.path.basename(key.rstrip('/'))
local = f'cos-downloads/{fname}'
try:
client.get_object(Bucket=bucket, Key=key, DestFilePath=local)
size = os.path.getsize(local)
print(f"  ✅ {fname} ({size} bytes)")
downloaded.append(fname)
except Exception as e:
print(f"  ❌ {key}: {e}")

print(f"\n下载完成: {len(downloaded)}/{file_count} 个文件")
with open(os.environ['GITHUB_ENV'], 'a') as f:
f.write(f"DOWNLOADED={len(downloaded)}\n")
f.write(f"NAMES={','.join(downloaded)}\n")
PYEOF
env:
TENCENT_SECRET_ID: ${{ secrets.TENCENT_SECRET_ID }}
TENCENT_SECRET_KEY: ${{ secrets.TENCENT_SECRET_KEY }}
TENCENT_COS_BUCKET: ${{ secrets.TENCENT_COS_BUCKET }}
TENCENT_COS_REGION: ${{ secrets.TENCENT_COS_REGION }}
FILE_COUNT: ${{ env.FILE_COUNT }}

# ── 解析数据并更新仪表盘 ──────────────────
- name: 解析并更新仪表盘
run: |
python3 << 'PYEOF'
import os, json, re, glob
from datetime import datetime
from collections import defaultdict
import pandas as pd
import openpyxl

print("=" * 50)
print("🦐 天猫BI数据解析")
print(datetime.now().strftime("⏰ %Y-%m-%d %H:%M:%S"))
print("=" * 50)

downloaded = os.environ.get('DOWNLOADED', '0')
if downloaded == '0':
print("⚠️ 没有新文件，跳过")
open('.skip', 'w').close()
exit(0)

files = sorted(glob.glob('cos-downloads/*'))
print(f"📁 处理 {len(files)} 个文件")
for f in files:
print(f"   {os.path.basename(f)}")

def pnum(s):
if s is None or s == '' or s == '-': return 0.0
s = str(s).strip().replace(',','').replace('%','').replace('¥','').replace('元','')
try: return float(s)
except: return 0.0

def norm_date(s):
s = str(s).strip()
for pat in [r'(\d{4})/(\d{1,2})/(\d{1,2})', r'(\d{4})-(\d{1,2})-(\d{1,2})']:
m = re.match(pat, s)
if m: return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
return s

def detect_sku(fname):
f = os.path.basename(fname).upper()
if 'PET500' in f or '500' in f: return 'PET500'
if 'PET600' in f or '600' in f: return 'PET600'
if 'RX400' in f or '400' in f: return 'RX400_Pro'
if 'U8' in f or '1500' in f: return 'U8'
if 'PROH' in f: return 'RX600_PROH'
if 'RX600P' in f or 'P600' in f: return 'RX600P'
if 'RX600' in f or 'PRO' in f: return 'RX600_PRO'
return 'PET500'

# SKU key 映射
SKU_JS_KEYS = {
'PET500': 'PET500', 'PET600': 'PET600',
'RX400_Pro': 'RX400_Pro', 'U8': 'U8',
'RX600_PRO': 'RX600_PRO', 'RX600P': 'RX600P',
'RX600_PROH': 'RX600_PROH',
}

all_data = defaultdict(lambda: defaultdict(dict))
all_dates = set()

for fpath in files:
fname = os.path.basename(fpath)
sku = detect_sku(fname)
print(f"\n📊 [{sku}] {fname}")

try:
if fname.endswith('.xlsx') or fname.endswith('.xls'):
wb = openpyxl.load_workbook(fpath, data_only=True)
for sheet_name in wb.sheetnames:
ws = wb[sheet_name]
rows = list(ws.iter_rows(values_only=True))
if not rows: continue

header_idx = None
for i, row in enumerate(rows):
if row and any('日期' in str(c) for c in row if c):
header_idx = i
break
if header_idx is None: continue

headers = [str(c).strip() if c else '' for c in rows[header_idx]]

# 判断数据类型
ct = 'sales'
if any('花费' in h or '广告' in h or '成交' in h for h in headers): ct = 'ads'
elif any('退款' in h or '售后' in h for h in headers): ct = 'refund'

print(f"  [{sheet_name}] {ct}, {len(rows)-header_idx-1}行")
for row in rows[header_idx+1:]:
if not row: continue
rd = dict(zip(headers, row))
dc = next((h for h in headers if '日期' in h), None)
if not dc: continue
dt = norm_date(rd.get(dc, ''))
if not dt or dt == 'nan': continue
all_data[sku][ct][dt] = rd
all_dates.add(dt)

elif fname.endswith('.csv'):
df = pd.read_csv(fpath, encoding='utf-8')
headers = list(df.columns)
dc = next((h for h in headers if '日期' in h), None)
if not dc: continue
df['dt'] = df[dc].apply(norm_date)
sku = detect_sku(fname)
for _, row in df.iterrows():
dt = row['dt']
if dt and dt != 'nan':
all_data[sku]['sales'][dt] = row.to_dict()
all_dates.add(dt)
except Exception as e:
print(f"  ❌ 解析错误: {e}")
import traceback; traceback.print_exc()

if not all_dates:
print("⚠️ 未解析到任何日期数据")
open('.skip', 'w').close()
exit(0)

dates_sorted = sorted(d for d in all_dates if d and d != 'nan')
recent_14 = dates_sorted[-14:] if len(dates_sorted) > 14 else dates_sorted
print(f"\n📅 范围: {dates_sorted[0]} ~ {dates_sorted[-1]} | 取最近14天: {recent_14[0]} ~ {recent_14[-1]}")

# 读取并替换 index.html
with open('index.html', 'r', encoding='utf-8') as f:
html = f.read()

total_updated = 0
for sku_name, js_key in SKU_JS_KEYS.items():
sku_info = all_data.get(sku_name, {})

gmv, net, adSpend, adRev = [], [], [], []
dir_, indir, roi, refund = [], [], [], []
paidTraf, advTraf = [0.0]*len(recent_14), [0.0]*len(recent_14)
revisit, inner = [0.0]*len(recent_14), [0.0]*len(recent_14)

for dt in recent_14:
sd = sku_info.get('sales', {}).get(dt, {})
ad = sku_info.get('ads', {}).get(dt, {})
rd = sku_info.get('refund', {}).get(dt, {})

g = pnum(sd.get('支付金额', sd.get('销售额', 0))) / 10000
r = pnum(sd.get('退款额', 0)) / 10000
gmv.append(round(g, 2))
net.append(round(g - r, 2))

cost = pnum(ad.get('花费', 0)) / 10000
dgmv = pnum(ad.get('直接成交金额', 0)) / 10000
igmv = pnum(ad.get('间接成交金额', 0)) / 10000
tgmv = pnum(ad.get('总成交金额', 0)) / 10000
adSpend.append(round(cost, 2))
adRev.append(round(tgmv, 2))
dir_.append(round(dgmv, 2))
indir.append(round(igmv, 2))
roi.append(round(tgmv / cost, 2) if cost > 0 else 0)

rr_raw = str(rd.get('退款率', '0')).replace('%','')
try: rr = float(rr_raw) if float(rr_raw) <= 1 else float(rr_raw)/100
except: rr = 0
refund.append(round(rr * 100, 2))

new_block = f"""{js_key}:{{
name:'{js_key}',
gmv:    {json.dumps(gmv)},
net:    {json.dumps(net)},
adSpend:{json.dumps(adSpend)},
adRev:  {json.dumps(adRev)},
dir:    {json.dumps(dir_)},
indir:  {json.dumps(indir)},
roi:    {json.dumps(roi)},
refund: {json.dumps(refund)},
paidTraf:{json.dumps(paidTraf)},
advTraf: {json.dumps(advTraf)},
revisit: {json.dumps(revisit)},
inner:  {json.dumps(inner)}}
}}"""

pat = rf'({js_key}:\{{.*?\n  \}}),'
if re.search(pat, html, flags=re.DOTALL):
html = re.sub(pat, new_block + ',', html, flags=re.DOTALL)
valid_roi = [x for x in roi if x > 0]
print(f"✅ {js_key}: GMV={sum(gmv):.1f}万 | ROI均值={sum(valid_roi)/len(valid_roi):.2f}" if valid_roi else f"✅ {js_key}: 无广告数据")
total_updated += 1
else:
print(f"⚠️ {js_key} 未在HTML中找到")

# 更新DATES
dates_str = json.dumps(recent_14)
m = re.search(r"const DATES=\[.*?\]", html)
if m:
html = html.replace(m.group(0), f"const DATES={dates_str}")
print(f"✅ DATES: {recent_14[0]} ~ {recent_14[-1]}")

with open('index.html', 'w', encoding='utf-8') as f:
f.write(html)

print(f"\n🎉 更新完成! 共更新 {total_updated} 个SKU的数据")
print(f"   仪表盘: https://wg887k4tf4-lang.github.io/tmall-bi-dashboard/")
PYEOF

# ── Git 提交推送 ──────────────────────────
- name: 提交并推送
