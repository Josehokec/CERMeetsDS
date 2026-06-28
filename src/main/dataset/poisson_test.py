import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import io


df = pd.read_csv('crimes.csv')


# ==========================================
# 2. 数据预处理
# ==========================================
def verify_poisson_distribution(df, target_type, time_interval='1D'):
    """
    df: 数据框
    target_type: 要验证的事件类型 (如 'THEFT')
    time_interval: 统计的时间窗口 (如 '1D' 代表一天, '1H' 代表一小时)
    """

    print(f"--- 正在分析事件类型: {target_type} ---")

    # 筛选特定类型的数据
    subset = df[df['Type'] == target_type].copy()

    if len(subset) < 5:
        print("数据量过少，无法进行有效的统计检验。")
        return

    # 将时间戳转换为 datetime 对象
    subset['datetime'] = pd.to_datetime(subset['EventTime'], unit='s')

    # 按时间窗口重采样，统计每个时间段内的事件发生次数 (k)
    # 例如：统计每天发生了多少次 THEFT
    event_counts = subset.set_index('datetime').resample(time_interval).size()

    # 此时 event_counts 的值就是 k (0次, 1次, 2次...)
    # 我们统计每个 k 出现的频率 (即：发生0次的天数有多少，发生1次的天数有多少...)
    observed_freq = event_counts.value_counts().sort_index()

    # 补全中间缺失的计数（例如有发生1次和3次的情况，但没有2次，需要补0）
    max_k = observed_freq.index.max()
    all_k = np.arange(0, max_k + 1)
    observed_counts = np.array([observed_freq.get(k, 0) for k in all_k])

    # ==========================================
    # 3. 计算泊松分布参数 Lambda (λ)
    # ==========================================
    # λ = E(X) = 平均每个时间段发生的次数
    lambda_val = event_counts.mean()
    print(f"计算得到的 Lambda (平均发生率): {lambda_val:.4f} 次/{time_interval}")

    # ==========================================
    # 4. 生成理论期望频数
    # ==========================================
    total_observations = observed_counts.sum()

    # 使用泊松公式计算理论概率 P(k) = (e^-λ * λ^k) / k!
    expected_probs = stats.poisson.pmf(all_k, lambda_val)

    # 计算期望频数 = 总观察数 * 理论概率
    expected_counts = expected_probs * total_observations

    # 归一化调整 (因为实际观测是有限的，理论是无限的，需确保总和一致)
    expected_counts = expected_counts * (observed_counts.sum() / expected_counts.sum())

    # ==========================================
    # 5. 卡方拟合优度检验 (Chi-Square Test)
    # ==========================================
    # 注意：为了统计有效性，通常要求每个桶的期望频数不小于5。
    # 这里为了演示简化处理，直接进行计算。
    chi2_stat, p_value = stats.chisquare(f_obs=observed_counts, f_exp=expected_counts)

    print(f"卡方统计量: {chi2_stat:.4f}")
    print(f"P-value (P值): {p_value:.4f}")

    print("\n>>> 结论判定:")
    if p_value > 0.05:
        print(f"P值 > 0.05。结果显著：我们【无法拒绝】原假设。")
        print(f"这意味着 '{target_type}' 的发生频率【符合】泊松分布。")
    else:
        print(f"P值 < 0.05。结果显著：我们【拒绝】原假设。")
        print(f"这意味着 '{target_type}' 的发生频率与泊松分布存在显著差异（可能不符合）。")

    # ==========================================
    # 6. 可视化对比 (Observed vs Expected)
    # ==========================================
    plt.figure(figsize=(10, 6))
    plt.bar(all_k - 0.15, observed_counts, width=0.3, label='Observed (Actual)', alpha=0.7)
    plt.bar(all_k + 0.15, expected_counts, width=0.3, label='Expected (Poisson)', alpha=0.7)
    plt.xlabel(f'Number of Events per {time_interval}')
    plt.ylabel('Frequency')
    plt.title(f'Poisson Distribution Fit for {target_type} (p={p_value:.3f})')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.show()

# BATTERY ROBBERY
verify_poisson_distribution(df, target_type='ROBBERY', time_interval='1D')