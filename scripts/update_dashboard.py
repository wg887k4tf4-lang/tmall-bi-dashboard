#!/usr/bin/env python3
"""天猫BI仪表盘数据更新脚本 - 从COS拉取数据并生成data.json"""

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
    all_files = []
    marker = ''
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': 'data/', 'MaxKeys': 1000}
        if marker:
            kwargs['Marker'] = marker
        resp = client.list_objects(**kwargs)
        batch = [f for f in resp.get('Contents', []) if not f['Key'].endswith('/')]
        all_files.extend(batch)
        if resp.get('IsTruncated') == 'true':
            marker = resp.get('NextMarker', '')
            if not marker and batch:
                marker = batch[-1]['Key']
        else:
            break
    files = all_files
except Exception as e:
    print(f"⚠️ 列出COS文件失败: {e}")
    files = []

if not files:
    print("⚠️ Bucket为空或无数据文件,跳过更新")
    open('.skip', 'w').close()
    exit(0)

print(f"📦 找到 {len(files)} 个文件:")
downloaded = []
for f in files:
    key = f['Key']
    if key.endswith('.gitkeep'):
        continue
    local = os.path.join('cos-downloads', key.replace('data/', '', 1))
    os.makedirs(os.path.dirname(local), exist_ok=True)
    try:
        client.download_file(Bucket=bucket, Key=key, DestFilePath=local)
        size = os.path.getsize(local)
        if size == 0:
            print(f"  ⏭️ {key} (空文件,跳过)")
            os.remove(local)
            continue
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
    """兼容字符串、datetime对象、带时间的日期格式"""
    from datetime import datetime as dt
    # datetime对象直接格式化
    if isinstance(s, dt):
        return s.strftime('%Y-%m-%d')
    s = str(s).strip()
    # 带时间: 2026-04-28 00:00:00
    m = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 斜线格式
    m = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return s

SKU_JS_KEYS = {
    'PET500': 'PET500', 'PET600': 'PET600',
    'RX400_Pro': 'RX400_Pro', 'U8': 'U8',
    'RX600_PRO': 'RX600_PRO', 'RX600P': 'RX600P',
    'RX600_PROH': 'RX600_PROH',
    '7232Pro': '7232Pro',
}

def detect_sku(filepath):
    parts = filepath.replace('\\', '/').split('/')
    for part in parts:
        m = re.match(r'([A-Za-z0-9]+(?:_Pro|_PRO|_PROH|P)?)_\d{10,}', part)
        if m:
            key = m.group(1)
            for known in SKU_JS_KEYS:
                if key.upper().replace('-', '_') == known.upper():
                    return known
    f = filepath.upper()
    if 'RX600_PROH' in f or 'RX600PROH' in f: return 'RX600_PROH'
    if 'RX600P' in f: return 'RX600P'
    if 'RX600_PRO' in f or 'RX600PRO' in f: return 'RX600_PRO'
    if 'RX400' in f: return 'RX400_Pro'
    if 'PET600' in f: return 'PET600'
    if 'PET500' in f: return 'PET500'
    if 'U8' in f: return 'U8'
    if '7232PRO' in f or '7232PRO' in f or '东芝' in f: return '7232Pro'
    return None

# ── 解析所有下载文件 ──────────────────────
all_data = defaultdict(lambda: defaultdict(dict))
all_dates = set()

for fpath in downloaded:
    fname = os.path.basename(fpath)
    sku = detect_sku(fpath)
    if sku is None:
        print(f"\n⏭️ {fname} - 无法识别SKU,跳过")
        continue
    if sku not in SKU_JS_KEYS:
        print(f"\n⏭️ {fname} - SKU '{sku}' 不在已知列表中,跳过")
        continue
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

                ct = 'sales'
                # 先看表头判断
                if any('花费' in h or '广告' in h or '投放' in h for h in headers): ct = 'ads'
                elif any('退款' in h or '售后' in h for h in headers): ct = 'refund'
                elif any('流量' in h or '访客' in h for h in headers): ct = 'traffic'
                # 表头无法判断时，看文件名（兼容"PET500站内.xlsx"格式）
                if ct == 'sales':
                    fname_up = fname.upper()
                    if '站内' in fname_up or '投放' in fname_up or '推广' in fname_up: ct = 'ads'
                    elif '流量' in fname_up or '访客' in fname_up: ct = 'traffic'
                    elif '退款' in fname_up: ct = 'refund'

                print(f"  [{sheet_name}] {ct}, {len(rows)-header_idx-1}行")
                for row in rows[header_idx+1:]:
                    if not row: continue
                    rd = dict(zip(headers, row))
                    dc = next((h for h in headers if '日期' in h), None)
                    if not dc: continue
                    dt = norm_date(rd.get(dc, ''))
                    if not dt or dt == 'nan': continue
                    # 广告数据需要按日期聚合（多行=多个计划/关键词）
                    if ct == 'ads':
                        if dt not in all_data[sku][ct]:
                            # 初始化聚合字典
                            all_data[sku][ct][dt] = {}
                        existing = all_data[sku][ct][dt]
                        for h in headers:
                            val = pnum(rd.get(h, 0))
                            if h in existing:
                                # 数值列累加，非数值列保留第一个值
                                try:
                                    existing[h] = round(existing[h] + val, 2)
                                except (TypeError, ValueError):
                                    pass
                            else:
                                existing[h] = val
                    else:
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
print(f"\n📅 范围: {dates_sorted[0]} ~ {dates_sorted[-1]} | 取最近14天: {recent_14[0]} ~ {recent_14[-1] if recent_14 else 'None'}")

# ── 生成 data.json ───────────────────────
skus_output = {}

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
            rv = float(rr_raw)
            rr = rv / 100 if rv > 1 else rv
        except:
            rr = 0
        refund.append(round(rr * 100, 2))

    # DEBUG: 检查广告数据
    ad_dates = list(sku_info.get('ads', {}).keys())
    if ad_dates:
        first_ad = sku_info['ads'][ad_dates[0]]
        print(f"  🔍 广告数据: {len(ad_dates)}天, 首日花费={first_ad.get('花费', 'N/A')}")
    else:
        print(f"  ⚠️ 无广告日期数据!")

    valid_roi = [x for x in roi if x > 0]
    if gmv and sum(gmv) > 0:
        print(f"✅ {js_key}: GMV={sum(gmv):.1f}万 | ROI均值={sum(valid_roi)/len(valid_roi):.2f}" if valid_roi else f"✅ {js_key}: GMV={sum(gmv):.1f}万 | 无广告数据")
    elif gmv:
        print(f"⚠️ {js_key}: GMV={sum(gmv):.1f}万 | ROI全为0(检查广告数据)")

    skus_output[js_key] = {
        "name": js_key,
        "gmv": gmv, "net": net, "adSpend": adSpend, "adRev": adRev,
        "dir": dir_, "indir": indir, "roi": roi, "refund": refund,
        "paidTraf": paidTraf, "advTraf": advTraf, "revisit": revisit, "inner": inner
    }

# 写入 data.json
output = {"skus": skus_output}
with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\n🎉 更新完成! 生成 data.json (最近{len(recent_14)}天, {len(skus_output)}个SKU)")
print(f"   仪表盘: https://wg887k4tf4-lang.github.io/tmall-bi-dashboard/")
