import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import io

# df = pd.read_csv('crimes.csv')
df = pd.read_csv('citibike.csv')

def verify_interarrival_with_limit(df, target_type, ratio=None):
    # 1. 数据处理, citibike是type
    subset = df[df['type'] == target_type].copy().sort_values(by='eventTime')
    if len(subset) < 3: return
    # crimes是Type, EventTime
    inter_arrival_times = subset['eventTime'].diff().dropna()
    # inter_arrival_times = inter_arrival_times[inter_arrival_times > 0]

    # 2. 统计计算 (必须使用所有数据，不能只用截断后的数据，否则参数会偏)
    mean_interval = inter_arrival_times.mean()
    ks_stat, p_value = stats.kstest(inter_arrival_times, 'expon', args=(0, mean_interval))

    print(f"平均间隔: {mean_interval:.2f} 秒")
    print(f"P-value: {p_value:.4f} (基于全部数据计算)")

    # 3. 确定可视化范围
    display_limit = None
    if ratio is None:
        display_limit = np.percentile(inter_arrival_times, 95)
        print(f"未指定显示上限，自动设置为 95% 分位数: {display_limit:.2f} 秒")
    else:
        display_limit = np.percentile(inter_arrival_times, ratio)

    # 4. 可视化
    plt.figure(figsize=(10, 6))

    # 关键点：自定义 bins (分桶)
    # 我们根据 0 到 display_limit 生成细密的桶，这样即使最大值很大，图也不会糊
    bins = np.linspace(0, display_limit, 30)

    # 画直方图
    # range参数限制了只统计这个范围内的数据用于绘图
    plt.hist(inter_arrival_times, bins=bins, density=True, alpha=0.6, color='skyblue', label='Actual Data (Zoomed)')

    # 画理论曲线
    # 只在 0 到 display_limit 范围内生成 X 轴坐标
    x = np.linspace(0, display_limit, 200)
    pdf = stats.expon.pdf(x, scale=mean_interval)
    plt.plot(x, pdf, 'r-', lw=2.5, label=f'Theoretical Exp (Mean={mean_interval:.0f})')

    plt.xlim(0, display_limit) # 强制截断 X 轴
    plt.xlabel('Time between events (seconds)')
    plt.ylabel('Probability Density')
    plt.title(f'Inter-arrival Time: First {display_limit} seconds\n(Type: {target_type}, p={p_value:.3f})')
    plt.legend()
    plt.grid(alpha=0.3)
    plt.show()


# verify_interarrival_with_limit(df, 'ROBBERY', ratio=99.9)
# verify_interarrival_with_limit(df, 'BATTERY', ratio=99.9)
# verify_interarrival_with_limit(df, 'MOTOR_VEHICLE_THEFT', ratio=99.9)

verify_interarrival_with_limit(df, 'A', ratio=99.5)