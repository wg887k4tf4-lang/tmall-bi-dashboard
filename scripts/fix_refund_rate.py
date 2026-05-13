#!/usr/bin/env python3
"""修复退款率数据"""
import json, csv, re
from datetime import datetime as dt

def norm_date(s):
    s = str(s).strip()
    # YYYYMMDD格式
    m = re.match(r'(\d{4})(\d{2})(\d{2})', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # YYYY-MM-DD格式
    m2 = re.match(r'(\d{4})-(\d{1,2})-(\d{1,2})', s)
    if m2:
        return f"{m2.group(1)}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None

# 读取现有的data.json
with open('../data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"当前日期范围: {data['dates'][0]} ~ {data['dates'][-1]}")

# 修正每个SKU的退款率
for sku, info in data['skus'].items():
    print(f"\n处理 {sku}...")
    
    # 找对应的CSV文件
    sku_dir = f"/Users/greg/.qclaw/workspace/tmall-bi-dashboard/cos-downloads/{info['name']}/商品退款分析/"
    print(f"  目录: {sku_dir}")
    
    import os
    if not os.path.exists(sku_dir):
        print(f"  ❌ 目录不存在")
        continue
    
    csv_files = [f for f in os.listdir(sku_dir) if f.endswith('.csv')]
    if not csv_files:
        print(f"  ❌ 没有CSV文件")
        continue
    
    print(f"  找到 {len(csv_files)} 个CSV文件")
    
    # 读取退款数据
    refund_dict = {}
    for csv_file in csv_files:
        try:
            with open(os.path.join(sku_dir, csv_file), 'r', encoding='gbk') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    dt_raw = row.get('日期', '')
                    # 处理 20260428-20260428 格式
                    dt_raw = str(dt_raw).split('-')[0]
                    dt = norm_date(dt_raw)
                    if not dt:
                        continue
                    
                    rr_raw = row.get('退款率', '0')
                    # 去掉百分号
                    rr_str = rr_raw.replace('%', '')
                    try:
                        rr = float(rr_str)
                        # 如果大于1，说明是百分比（如52.35），要除以100
                        if rr > 1:
                            rr = rr / 100
                    except:
                        rr = 0
                    
                    refund_dict[dt] = rr
    
        except Exception as e:
            print(f"  ❌ 解析 {csv_file} 失败: {e}")
    
    print(f"  读取到 {len(refund_dict)} 天的退款数据")
    
    # 更新到data.json
    fixed_refund = []
    for dt in data['dates']:
        if dt in refund_dict:
            fixed_refund.append(round(refund_dict[dt] * 100, 2))
        else:
            fixed_refund.append(0.0)
    
    info['refund'] = fixed_refund
    print(f"  修正后前3天退款率: {fixed_refund[:3]}")

# 写回data.json
with open('../data.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("\n✅ 退款率修复完成!")
