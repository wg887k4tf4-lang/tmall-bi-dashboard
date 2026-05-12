#!/usr/bin/env python3
"""天猫BI仪表盘数据更新脚本 - 从COS拉取数据并更新index.html"""

import os, json, re, glob
from datetime import datetime
from collections import defaultdict
import pandas as pd
import openpyxl
from qcloud_cos import CosConfig, CosS3Client

print("=" * 50)
print("🦐 天猫BI数据解析")
print(datetime.now().strftime("⏰ %Y-%m-%d %H:%M:%S"))
print("=" * 50)

# ── COS 连接 ──────────────────────────────
secret_id  = os.environ['TENCENT_SECRET_ID']
secret_key = os.environ['TENCENT_SECRET_KEY']
bucket     = os.environ['TENCENT_COS_BUCKET']
region     = os.environ.get('TENCENT_COS_REGION', 'ap-beijing')

config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key)
client = CosS3Client(config)

# ── 列出并下载COS文件 ──────────────────────
os.makedirs('cos-downloads', exist_ok=True)

try:
    resp = client.list_objects(Bucket=bucket, Prefix='data/', MaxKeys=200)
    files = [f for f in resp.get('Contents', []) if not f['Key'].endswith('/')]
except Exception as e:
    print(f"⚠️ 列出COS文件失败: {e}")
    files = []

if not files:
    print("⚠️ Bucket为空或无数据文件，跳过更新")
    open('.skip', 'w').close()
    exit(0)

print(f"📦 找到 {len(files)} 个文件:")
downloaded = []
for f in files:
    key = f['Key']
    # 保留子目录结构作为本地路径
    local = os.path.join('cos-downloads', key.replace('data/', '', 1))
    os.makedirs(os.path.dirname(local), exist_ok=True)
    try:
        client.download_file(Bucket=bucket, Key=key, DestFilePath=local)
        size = os.path.getsize(local)
        print(f"  ✅ {key} ({size} bytes)")
        downloaded.append(local)
    except Exception as e:
        print(f"  ❌ {key}: {e}")

print(f"\n下载完成: {len(downloaded)} 个文件")

# ── 工具函数 ──────────────────────────────
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

def detect_sku(filepath):
    """从文件路径推断SKU"""
    f = filepath.upper()
    if 'PET500' in f: return 'PET500'
    if 'PET600' in f: return 'PET600'
    if 'RX400' in f: return 'RX400_Pro'
    if 'U8' in f: return 'U8'
    if 'PROH' in f: return 'RX600_PROH'
    if 'RX600P' in f: return 'RX600P'
    if 'RX600' in f: return 'RX600_PRO'
    return 'PET500'  # 默认

# SKU key 映射
SKU_JS_KEYS = {
    'PET500': 'PET500', 'PET600': 'PET600',
    'RX400_Pro': 'RX400_Pro', 'U8': 'U8',
    'RX600_PRO': 'RX600_PRO', 'RX600P': 'RX600P',
    'RX600_PROH': 'RX600_PROH',
}

# ── 解析所有下载文件 ──────────────────────
all_data = defaultdict(lambda: defaultdict(dict))
all_dates = set()

for fpath in downloaded:
    fname = os.path.basename(fpath)
    sku = detect_sku(fpath)
    print(f"\n📊 [{sku}] {fname}")

    try:
        if fname.endswith(('.xlsx', '.xls')):
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
                if any('花费' in h or '广告' in h or '投放' in h for h in headers): ct = 'ads'
                elif any('退款' in h or '售后' in h for h in headers): ct = 'refund'
                elif any('流量' in h or '访客' in h for h in headers): ct = 'traffic'

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
            for enc in ['utf-8', 'gbk', 'utf-8-sig']:
                try:
                    df = pd.read_csv(fpath, encoding=enc)
                    break
                except: continue
            headers = list(df.columns)
            dc = next((h for h in headers if '日期' in h), None)
            if not dc: continue
            df['dt'] = df[dc].apply(norm_date)
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

# ── 更新 index.html ──────────────────────
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
        try:
            rr = float(rr_raw) if float(rr_raw) <= 1 else float(rr_raw)/100
        except:
            rr = 0
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
        if valid_roi:
            print(f"✅ {js_key}: GMV={sum(gmv):.1f}万 | ROI均值={sum(valid_roi)/len(valid_roi):.2f}")
        else:
            print(f"✅ {js_key}: GMV={sum(gmv):.1f}万 | 无广告数据")
        total_updated += 1
    else:
        print(f"⚠️ {js_key} 未在HTML中找到匹配")

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
