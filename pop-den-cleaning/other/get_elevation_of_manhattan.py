import rasterio
import numpy as np
import pandas as pd

manhattan_lon_range = (-74.03, -73.91)  # 曼哈顿地理坐标范围的经度范围
manhattan_lat_range = (40.88, 40.70)  # 曼哈顿地理坐标范围的纬度范围

# 打开GeoTIFF文件
with rasterio.open('exportImage.tif') as src:
    # 转换经纬度范围为像素坐标
    top_left = src.index(manhattan_lon_range[0], manhattan_lat_range[1])
    bottom_right = src.index(manhattan_lon_range[1], manhattan_lat_range[0])

    # 计算窗口
    window = rasterio.windows.Window.from_slices(
        (top_left[0], bottom_right[0]+1),
        (top_left[1], bottom_right[1]+1)
    )
    
    # 读取窗口数据
    raster = src.read(1, window=window)

    # 获取无数据值并替换为NaN
    no_data = src.nodatavals[0]
    raster[raster == no_data] = np.nan

    # 获取窗口的变换参数
    transform = src.window_transform(window)

    # 使用numpy创建坐标网格
    rows, cols = raster.shape
    row, col = np.meshgrid(np.arange(rows), np.arange(cols), indexing='ij')

    # 转换为地理坐标
    xs, ys = rasterio.transform.xy(transform, row, col)

    # 创建DataFrame
    df = pd.DataFrame({
        'Longitude': np.array(xs).flatten(),
        'Latitude': np.array(ys).flatten(),
        'PopulationDensity': raster.flatten()
    })

    # 移除NaN值
    df = df.dropna(subset=['PopulationDensity'])

# 保存到CSV文件
df.to_csv('elevation_manhattan.csv', index=False)