#!/usr/bin/env python3
"""天猫BI仪表盘数据更新脚本 - 从COS拉取数据并生成data.json"""
# v3.0 - 彻底重写，修复广告数据解析

import os, json, re, glob
from datetime import datetime, timedelta
from collections import defaultdict
import openpyxl
from qcloud_cos import CosConfig, CosS3Client

print("="*50)
print("🦐 天猫BI数据解析")
print(datetime.now().strftime("⏰ %Y-%m-%d %H:%M:%S"))
print("="*50)

# ── COS 连接 ──────────────────────────────
secret_id  = os.environ['TENCENT_SECRET_ID']
secret_key = os.environ['TENCENT_SECRET_KEY']
bucket     = os.environ['TENCENT_COS_BUCKET']
region     = os.environ.get('TENCENT_COS_REGION', 'ap-beijing')

config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key)
client = CosS3Client(config)

# ── SKU 映射 ──────────────────────────────
SKU_JS_KEYS = {
    'PET500':      'PET500',
    'PET600':      'PET600',
    'RX400_Pro':  'RX400_Pro',
    'U8':          'U8',
    'RX600_PRO':   'RX600_PRO',
    'RX600P':      'RX600P',
    'RX600_PROH':  'RX600_PROH',
    '东芝微烤-7232Pro': '7232Pro',
}

def norm_date(s):
    """兼容字符串、datetime对象、Excel数字日期"""
    from datetime import datetime as dt, timedelta
    if isinstance(s, dt):
        return s.strftime('%Y-%m-%d')
    s = str(s).strip()
    # Excel数字日期
    try:
        n = float(s)
        if 40000 < n < 50000:
            base = dt(1899, 12, 30)
            return (base + timedelta(days=n)).strftime('%Y-%m-%d')
    except ValueError:
        pass
    # 标准格式: 2026-04-28
    m = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 斜线格式
    m = re.match(r'(\d{4})/(\d{1,2})/(\d{1,2})', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return None

def pnum(s):
    """转数字，兜底0"""
    try:
        return float(str(s).replace(',', ''))
    except:
        return 0.0

# ── 下载 & 解析 ──────────────────────────
os.makedirs('cos-downloads', exist_ok=True)
all_data = {}   # {sku: {sales:{dt:{...}}, ads:{dt:{...}}, refund:{dt:{...}}, traffic:{dt:{...}}}}
all_dates = set()

for sku_name, js_key in SKU_JS_KEYS.items():
    all_data[sku_name] = {'sales':{}, 'ads':{}, 'refund':{}, 'traffic':{}}
    prefix = f'data/{sku_name}_{js_key}/'
    
    try:
        resp = client.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=100)
        files = [f['Key'] for f in resp.get('Contents', []) if not f['Key'].endswith('/')]
        print(f"\n📦 [{sku_name}] {len(files)}个文件")
        
        for key in files:
            fname = os.path.basename(key)
            lpath = f'cos-downloads/{fname}'
            
            # 下载
            try:
                r = client.get_object(Bucket=bucket, Key=key)
                with open(lpath, 'wb') as f:
                    f.write(r['Body'].read())
            except Exception as e:
                print(f"  ❌ 下载失败 {fname}: {e}")
                continue
            
            # 判断数据类型
            if '销售' in fname or '销售' in key:
                ct = 'sales'
            elif '站内' in fname or '推广' in fname or '投放' in fname:
                ct = 'ads'
            elif '退款' in fname or '售后' in fname:
                ct = 'refund'
            elif '流量' in fname or '访客' in fname:
                ct = 'traffic'
            else:
                ct = 'sales'  # 默认
            
            print(f"  📊 {fname} → {ct}")
            
            # 解析Excel
            if fname.endswith(('.xlsx', '.xls')):
                try:
                    wb = openpyxl.load_workbook(lpath, data_only=True)
                    for sheet_name in wb.sheetnames:
                        ws = wb[sheet_name]
                        rows = list(ws.iter_rows(values_only=True))
                        
                        # 找表头行
                        header_idx = None
                        for i, row in enumerate(rows):
                            if row and any('日期' in str(c) for c in row if c):
                                header_idx = i
                                break
                        if header_idx is None:
                            continue
                        
                        headers = [str(c).strip() if c else '' for c in rows[header_idx]]
                        
                        for row in rows[header_idx+1:]:
                            if not row or not any(row):
                                continue
                            rd = dict(zip(headers, row))
                            
                            # 找日期列
                            dc = next((h for h in headers if '日期' in h), None)
                            if not dc:
                                continue
                            dt = norm_date(rd.get(dc, ''))
                            if not dt:
                                continue
                            
                            # 存入all_data
                            if ct == 'ads':
                                if dt not in all_data[sku_name][ct]:
                                    all_data[sku_name][ct][dt] = {}
                                existing = all_data[sku_name][ct][dt]
                                for h in headers:
                                    val = pnum(rd.get(h, 0))
                                    if h in existing:
                                        try:
                                            existing[h] = round(existing[h] + val, 2)
                                        except:
                                            pass
                                    else:
                                        existing[h] = val
                            else:
                                all_data[sku_name][ct][dt] = rd
                            
                            all_dates.add(dt)
                except Exception as e:
                    print(f"  ❌ 解析错误 {fname}: {e}")
                    
            elif fname.endswith('.csv'):
                try:
                    import csv
                    for enc in ['utf-8', 'gbk', 'utf-8-sig']:
                        try:
                            with open(lpath, 'r', encoding=enc) as f:
                                reader = csv.DictReader(f)
                                headers = reader.fieldnames
                                dc = next((h for h in headers if '日期' in h), None)
                                if not dc:
                                    continue
                                for row in reader:
                                    dt = norm_date(row.get(dc, ''))
                                    if dt and dt != 'nan':
                                        all_data[sku_name]['sales'][dt] = row
                                        all_dates.add(dt)
                            break
                        except:
                            continue
                except Exception as e:
                    print(f"  ❌ CSV解析错误 {fname}: {e}")
    except Exception as e:
        print(f"❌ [{sku_name}] 处理失败: {e}")

# ── 整理日期 ──────────────────────────────
dates_sorted = sorted(d for d in all_dates if d and d != 'nan' and len(d)==10 and '-' in d)
recent_14 = dates_sorted[-14:] if len(dates_sorted) > 14 else dates_sorted
print(f"\n📅 范围: {dates_sorted[0]} ~ {dates_sorted[-1]} | 使用 {len(recent_14)} 天")

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
        
        # 销售数据
        g = pnum(sd.get('支付金额', sd.get('销售额', 0))) / 10000
        r = pnum(sd.get('退款额', 0)) / 10000
        gmv.append(round(g, 2))
        net.append(round(g - r, 2))
        
        # 广告数据
        cost = pnum(ad.get('花费', 0)) / 10000
        dgmv = pnum(ad.get('直接成交金额', 0)) / 10000
        igmv = pnum(ad.get('间接成交金额', 0)) / 10000
        tgmv = pnum(ad.get('总成交金额', 0)) / 10000
        adSpend.append(round(cost, 2))
        adRev.append(round(tgmv, 2))
        dir_.append(round(dgmv, 2))
        indir.append(round(igmv, 2))
        roi.append(round(tgmv / cost, 2) if cost > 0 else 0)
        
        # 退款数据
        rr_raw = str(rd.get('退款率', '0')).replace('%','')
        try:
            rv = float(rr_raw)
            rr = rv / 100 if rv > 1 else rv
        except:
            rr = 0
        refund.append(round(rr * 100, 2))
        
        # 流量数据（简化）
        traffic = sku_info.get('traffic', {}).get(dt, {})
        # 这里先留0，后面再细化
    
    # 检查数据是否有效
    has_gmv = any(x > 0 for x in gmv)
    has_ad = any(x > 0 for x in adSpend)
    
    if has_gmv:
        skus_output[js_key] = {
            'name': sku_name,
            'gmv': gmv,
            'net': net,
            'adSpend': adSpend,
            'adRev': adRev,
            'dir': dir_,
            'indir': indir,
            'roi': roi,
            'refund': refund,
            'paidTraf': paidTraf,
            'advTraf': advTraf,
            'revisit': revisit,
            'inner': inner,
        }
        print(f"✅ {js_key}: GMV={sum(gmv):.1f}万 | 广告花费={sum(adSpend):.1f}万 {'✅' if has_ad else '⚠️ 无广告数据'}")
    else:
        print(f"⚠️ {js_key}: 无有效销售数据")

# ── 写入 data.json ───────────────────────
output = {
    "dates": recent_14,
    "skus": skus_output,
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
}

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

print(f"\n🎉 更新完成! 生成 data.json ({len(recent_14)}天, {len(skus_output)}个SKU)")
print(f"   仪表盘: https://wg887k4tf4-lang.github.io/tmall-bi-dashboard/")
