#!/usr/bin/env python3
"""最简单测试 - 只下载一个文件"""
import os
from qcloud_cos import CosConfig, CosS3Client

config = CosConfig(Region='ap-beijing', 
    SecretId='AKIDjS3X7a2QgomdMExXgt0LYbS6ZqumU8aq',
    SecretKey='63wd7dGnCRVOQMKASbaQIzVqzzoSuCLQ')
client = CosS3Client(config)

# 下载一个文件
key = 'data/PET500_873480929689/商品销售分析/PET500销售.xlsx'
print(f'下载: {key}')

r = client.get_object(Bucket='tmall-bi-data-v1-1430009310', Key=key)
data = r['Body'].read()
print(f'下载成功: {len(data)} bytes')

# 保存到文件
os.makedirs('cos-downloads', exist_ok=True)
lpath = 'cos-downloads/PET500销售.xlsx'
with open(lpath, 'wb') as f:
    f.write(data)
print(f'保存成功: {lpath}')

# 试着打开
import openpyxl
wb = openpyxl.load_workbook(lpath, data_only=True)
print(f'Excel打开成功: {wb.sheetnames}')
