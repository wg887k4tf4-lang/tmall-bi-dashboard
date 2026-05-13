#!/usr/bin/env python3
"""天猫BI仪表盘数据更新脚本 v10.0 - 双格式输出"""
import os, json, re, csv
from datetime import datetime
from collections import defaultdict
import openpyxl
from qcloud_cos import CosConfig, CosS3Client

print("="*50)
print("🦐 天猫BI数据解析 v10.0")
print(datetime.now().strftime("⏰ %Y-%m-%d %H:%M:%S"))
print("="*50)

# ── COS 连接 ────────────────────────
secret_id  = os.environ['TENCENT_SECRET_ID']
secret_key = os.environ['TENCENT_SECRET_KEY']
bucket     = os.environ['TENCENT_COS_BUCKET']
region     = os.environ.get('TENCENT_COS_REGION', 'ap-beijing')

config = CosConfig(Region=region, SecretId=secret_id, SecretKey=secret_key)
client = CosS3Client(config)

# ── SKU 映射 ────────────────────────
SKU_MAP = {
    'PET500_873480929689':      'PET500',
    'PET600_1001231224168':    'PET600',
    'RX400_Pro_704193543906':  'RX400_Pro',
    'U8_1032758801866':        'U8',
    'RX600_PRO_801617527631':  'RX600_PRO',
    'RX600P_800794914500':     'RX600P',
    'RX600_PROH_802250146018': 'RX600_PROH',
    '7232Pro_898077474925':   '7232Pro',
}

def norm_date(s):
    """兼容字符串、datetime对象、Excel数字日期"""
    from datetime import datetime as dt, timedelta
    if isinstance(s, dt):
        return s.strftime('%Y-%m-%d')
    s = str(s).strip()
    try:
        n = float(s)
        if 40000 < n < 50000:
            base = dt(1899, 12, 30)
            return (base + timedelta(days=n)).strftime('%Y-%m-%d')
    except ValueError:
        pass
    m = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # 尝试 YYYYMMDD 格式
    m = re.match(r'(\d{4})(\d{2})(\d{2})', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def pnum(s):
    """转数字，兜底0"""
    try:
        return float(str(s).replace(',', ''))
    except:
        return 0.0

def parse_rr(val):
    """解析退款率（兼容百分比和小数）"""
    try:
        v = float(str(val).replace('%',''))
        return v/100 if v>1 else v
    except:
        return 0

# ── 下载 & 解析 ────────────────────────
os.makedirs('cos-downloads', exist_ok=True)
all_data = {}   # {sku: {'sales':{dt:{全部字段}}, 'ads':{dt:{}}, 'refund':{dt:{}}, 'traffic':{dt:{}}}}
all_dates = set()

for sku_name, js_key in SKU_MAP.items():
    all_data[sku_name] = {'sales':{}, 'ads':{}, 'refund':{}, 'traffic':{}}
    prefix = f'data/{sku_name}/'
    
    try:
        resp = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/', MaxKeys=100)
        subdirs = [p['Prefix'] for p in resp.get('CommonPrefixes', [])]
        if not subdirs:
            subdirs = [prefix]
        
        print(f"\n📦 [{sku_name}] {len(subdirs)}个子文件夹")
        
        for sub_prefix in subdirs:
            sub_name = sub_prefix.rstrip('/').split('/')[-1]
            resp2 = client.list_objects(Bucket=bucket, Prefix=sub_prefix, MaxKeys=100)
            files = [f['Key'] for f in resp2.get('Contents', []) if not f['Key'].endswith('/')]
            
            for key in files:
                fname = os.path.basename(key)
                if fname == '.gitkeep':
                    continue
                
                lpath = f'cos-downloads/{sku_name}/{sub_name}/{fname}'
                os.makedirs(os.path.dirname(lpath), exist_ok=True)
                
                try:
                    r = client.get_object(Bucket=bucket, Key=key)
                    data = b''
                    chunk = r['Body'].read(8192)
                    while chunk:
                        data += chunk
                        chunk = r['Body'].read(8192)
                    with open(lpath, 'wb') as f:
                        f.write(data)
                except Exception as e:
                    print(f"  ❌ 下载失败 {fname}: {e}")
                    continue
                
                # 判断数据类型
                fname_up = fname.upper()
                sub_name_up = sub_name.upper()
                if '退款' in fname_up or '退款' in sub_name_up:
                    ct = 'refund'
                elif '流量' in fname_up or '流量' in sub_name_up:
                    ct = 'traffic'
                elif '投放' in fname_up or '推广' in fname_up:
                    ct = 'ads'
                else:
                    ct = 'sales'
                
                print(f"  📊 {sub_name}/{fname} → {ct}")
                
                if fname.endswith(('.xlsx', '.xls')):
                    try:
                        wb = openpyxl.load_workbook(lpath, data_only=True)
                        ws = wb.active
                        rows = list(ws.iter_rows(values_only=True))
                        
                        header_idx = None
                        for i, row in enumerate(rows):
                            if row and any('日期' in str(c) for c in row if c):
                                header_idx = i
                                break
                        
                        if header_idx is None:
                            print(f"      ⚠️ 没找到表头")
                            continue
                        
                        headers = [str(c).strip() if c else '' for c in rows[header_idx]]
                        
                        for row in rows[header_idx+1:]:
                            if not row or not any(row):
                                continue
                            rd = dict(zip(headers, row))
                            dc = next((h for h in headers if '日期' in h or '时间' in h), None)
                            if not dc:
                                continue
                            dt_raw = str(rd.get(dc, ''))
                            dt = norm_date(dt_raw)
                            if not dt:
                                continue
                            
                            if ct == 'ads':
                                if dt not in all_data[sku_name][ct]:
                                    all_data[sku_name][ct][dt] = {'花费':0,'直接成交金额':0,'间接成交金额':0,'总成交金额':0}
                                all_data[sku_name][ct][dt]['花费'] += pnum(rd.get('花费',0))
                                all_data[sku_name][ct][dt]['直接成交金额'] += pnum(rd.get('直接成交金额',0))
                                all_data[sku_name][ct][dt]['间接成交金额'] += pnum(rd.get('间接成交金额',0))
                                all_data[sku_name][ct][dt]['总成交金额'] += pnum(rd.get('总成交金额',0))
                            elif ct == 'traffic':
                                if dt not in all_data[sku_name][ct]:
                                    all_data[sku_name][ct][dt] = {'访客数':0,'支付买家数':0,'支付金额':0}
                                all_data[sku_name][ct][dt]['访客数'] += pnum(rd.get('访客数',0))
                                all_data[sku_name][ct][dt]['支付买家数'] += pnum(rd.get('支付买家数',0))
                                all_data[sku_name][ct][dt]['支付金额'] += pnum(rd.get('支付金额',0))
                            else:
                                # 销售/退款数据：存全部原始字段
                                if dt not in all_data[sku_name][ct]:
                                    all_data[sku_name][ct][dt] = {}
                                all_data[sku_name][ct][dt].update(rd)
                                all_dates.add(dt)
                    except Exception as e:
                        print(f"  ❌ 解析错误 {fname}: {e}")
                elif fname.endswith('.csv'):
                    try:
                        for enc in ['utf-8-sig','gbk','utf-8']:
                            try:
                                with open(lpath,'r',encoding=enc) as f:
                                    reader = csv.DictReader(f)
                                    dc = next((h for h in reader.fieldnames if '日期' in h),None)
                                    if not dc:
                                        continue
                                    for row in reader:
                                        dt = norm_date(row.get(dc,''))
                                        if dt and dt!='nan':
                                            if ct=='refund':
                                                if dt not in all_data[sku_name]['refund']:
                                                    all_data[sku_name]['refund'][dt] = {}
                                                # 3种退款率：存全部原始字段
                                                all_data[sku_name]['refund'][dt].update(row)
                                            else:
                                                if dt not in all_data[sku_name]['sales']:
                                                    all_data[sku_name]['sales'][dt] = {}
                                                all_data[sku_name]['sales'][dt].update(row)
                                            all_dates.add(dt)
                                    print(f'      ✅ CSV解析成功 ({enc})')
                                break
                            except:
                                continue
                    except Exception as e:
                        print(f"  ❌ CSV解析错误 {fname}: {e}")
    except Exception as e:
        print(f"❌ [{sku_name}] 处理失败: {e}")

# ── 整理日期 ─────────────────────────
dates_sorted = sorted(d for d in all_dates if d and d!='nan' and len(d)==10 and '-' in d)
max_days = 365  # 最多保留365天（累积数据）
recent_data = dates_sorted[-max_days:] if len(dates_sorted)>max_days else dates_sorted
print(f"\n📅 范围: {dates_sorted[0] if dates_sorted else '无'} ~ {dates_sorted[-1] if dates_sorted else '无'} | 使用 {len(recent_data)} 天（最多{max_days}天）")

# ── 生成 双格式 ───────────────────────
# 1. data.json → 旧格式（数组），给 index.html 用
# 2. data_full.json → 新格式（全量对象），给以后扩展用

# ── 格式1: 旧格式（数组）────────────────
old_format = {
    "dates": recent_data,
    "skus": {},
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
}

# ── 格式2: 新格式（全量对象）────────────
new_format = {
    "dates": recent_data,
    "skus": {},
    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")
}

for sku_name, js_key in SKU_MAP.items():
    sku_info = all_data.get(sku_name, {})
    
    # 检查是否有销售数据
    has_sales = any(dt in sku_info.get('sales', {}) for dt in recent_data)
    
    if not has_sales:
        print(f"⚠️ {js_key}: 无有效销售数据")
        continue
    
    # ── 格式1: 构建数组 ────────────────
    gmv,net,adSpend,adRev = [],[],[],[]
    dir_,indir,roi = [],[],[]
    refund_total_list,refund_after_list,refund_before_list = [],[],[]
    paidTraf,advTraf = [0.0]*len(recent_data),[0.0]*len(recent_data)
    revisit,inner = [0.0]*len(recent_data),[0.0]*len(recent_data)
    
    for idx,dt in enumerate(recent_data):
        sd = sku_info.get('sales',{}).get(dt,{})
        ad = sku_info.get('ads',{}).get(dt,{})
        rd = sku_info.get('refund',{}).get(dt,{})
        td = sku_info.get('traffic',{}).get(dt,{})
        
        # 销售数据
        g = pnum(sd.get('支付金额', sd.get('销售额', sd.get('下单金额',0))))
        r = pnum(sd.get('退款额', sd.get('退款金额',0)))
        if g > 0.01:
            gmv.append(round(g/10000,2))
            net.append(round((g-r)/10000,2))
        else:
            gmv.append(0.0)
            net.append(0.0)
        
        # 广告数据
        cost = pnum(ad.get('花费',0))/10000
        dgmv = pnum(ad.get('直接成交金额',0))/10000
        igmv = pnum(ad.get('间接成交金额',0))/10000
        tgmv = pnum(ad.get('总成交金额',0))/10000
        adSpend.append(round(cost,2))
        adRev.append(round(tgmv,2))
        dir_.append(round(dgmv,2))
        indir.append(round(igmv,2))
        roi.append(round(tgmv/cost,2) if cost>0 else 0)
        
        # 退款数据（3种退款率）
        rr_total = parse_rr(rd.get('退款率', rd.get('total', 0)))
        rr_after = parse_rr(rd.get('退款率（发货后）', rd.get('after', 0)))
        rr_before = parse_rr(rd.get('退货率（发货前）', rd.get('退货率', rd.get('before', 0))))
        
        refund_total_list.append(round(rr_total*100,2))
        refund_after_list.append(round(rr_after*100,2))
        refund_before_list.append(round(rr_before*100,2))
        
        # 流量数据
        visitors = pnum(td.get('访客数',0))
        if visitors > 0:
            paidTraf[idx] = visitors
    
    old_format['skus'][js_key] = {
        'name': sku_name,
        'gmv': gmv,
        'net': net,
        'adSpend': adSpend,
        'adRev': adRev,
        'dir': dir_,
        'indir': indir,
        'roi': roi,
        'refund': refund_total_list,  # 兼容 index.html 的 refund 字段
        'refund_after': refund_after_list,
        'refund_before': refund_before_list,
        'paidTraf': paidTraf,
        'advTraf': advTraf,
        'revisit': revisit,
        'inner': inner,
    }
    
    # ── 格式2: 构建全量对象 ─────────────
    sku_output = {
        'name': sku_name,
        'sales': {},
        'ads': {},
        'refund': {},
        'traffic': {}
    }
    
    for dt in recent_data:
        if 'sales' in sku_info and dt in sku_info['sales']:
            sku_output['sales'][dt] = sku_info['sales'][dt]
        if 'ads' in sku_info and dt in sku_info['ads']:
            sku_output['ads'][dt] = sku_info['ads'][dt]
        if 'refund' in sku_info and dt in sku_info['refund']:
            sku_output['refund'][dt] = sku_info['refund'][dt]
        if 'traffic' in sku_info and dt in sku_info['traffic']:
            sku_output['traffic'][dt] = sku_info['traffic'][dt]
    
    new_format['skus'][js_key] = sku_output
    
    # 计算汇总
    total_gmv = 0
    total_ads = 0
    for dt in recent_data:
        sd = sku_output['sales'].get(dt, {})
        g = pnum(sd.get('支付金额', sd.get('销售额', sd.get('下单金额',0))))
        if g > 0.01:
            total_gmv += g
        ad = sku_output['ads'].get(dt, {})
        cost = pnum(ad.get('花费',0))
        if cost > 0:
            total_ads += cost
    
    print(f"✅ {js_key}: GMV={total_gmv/10000:.1f}万 | 广告花费={total_ads/10000:.1f}万")

# ── 写入文件 ─────────────────────────
# 1. data.json（旧格式）
with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(old_format, f, ensure_ascii=False, indent=2)

# 2. data_full.json（新格式，需转换datetime为字符串）
def make_json_serializable(obj):
    """递归转换datetime为字符串"""
    if hasattr(obj, 'strftime'):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(v) for v in obj]
    else:
        return obj

new_format_serializable = make_json_serializable(new_format)

with open('data_full.json', 'w', encoding='utf-8') as f:
    json.dump(new_format_serializable, f, ensure_ascii=False, indent=2)

print(f"\n🎉 更新完成! 生成双格式:")
print(f"   📊 data.json ({len(recent_data)}天, {len(old_format['skus'])}个SKU) → 给 index.html 用")
print(f"   📦 data_full.json ({len(recent_data)}天, {len(new_format['skus'])}个SKU) → 全量数据，给以后扩展用")
print(f"   仪表盘: https://wg887k4tf4-lang.github.io/tmall-bi-dashboard/")
print(f"   本地: http://localhost:8899/")
