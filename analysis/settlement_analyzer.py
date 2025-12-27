#!/usr/bin/env python3
"""
Settlement Time Analyzer
分析長期收集的 funding settlement 統計數據，找出真正的結算時間模式

使用方式：
    python3 analysis/settlement_analyzer.py [--stats-file PATH] [--output-dir PATH]

輸入：
    - analysis/logs/settlement_stats.csv（由 funding_monitor.py 自動產生）

輸出：
    - 統計分析報告
    - 真正結算時間的分佈圖
    - 各 symbol 的凍結模式分析
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# 解析命令列參數
parser = argparse.ArgumentParser(description="Settlement Time Analyzer")
parser.add_argument('--stats-file', type=str, default='analysis/logs/settlement_stats.csv',
                    help='settlement_stats.csv 檔案路徑')
parser.add_argument('--output-dir', type=str, default='analysis/plots/settlement_analysis',
                    help='輸出目錄')
parser.add_argument('--min-samples', type=int, default=3,
                    help='每個 symbol 至少需要多少筆數據才進行分析')
args = parser.parse_args()


def load_data(stats_file: str) -> pd.DataFrame:
    """載入結算統計數據"""
    path = Path(stats_file)
    if not path.exists():
        print(f"❌ 找不到檔案：{path}")
        print("   請先執行 funding_monitor.py 收集數據")
        return None
    
    # 先讀取 header 確定欄位數量
    import csv
    with open(path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
    
    # 使用 names 和 usecols 來確保正確讀取（處理欄位數量不一致的情況）
    df = pd.read_csv(path, names=headers, skiprows=1, usecols=range(len(headers)))
    
    print(f"✅ 載入 {len(df)} 筆結算記錄")
    print(f"   欄位：{list(df.columns[:10])}...")
    
    # 自動偵測 CSV 格式
    # 格式 1：funding_monitor.py 產生的 settlement_stats.csv
    if 'settlement_time_utc' in df.columns:
        df['settlement_time_utc'] = pd.to_datetime(df['settlement_time_utc'], utc=True)
        df_with_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
        print(f"   格式：settlement_stats.csv")
        print(f"   其中 {len(df_with_freeze)} 筆有凍結事件記錄")
        df['_format'] = 'stats'
    
    # 格式 2：settlement_sim_results.csv（模擬交易結果）
    elif 'timestamp_utc' in df.columns and 'max_latency_ms_2s' in df.columns:
        print(f"   格式：settlement_sim_results.csv（模擬交易結果）")
        # 轉換欄位名稱以相容分析
        df['settlement_time_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
        df['funding_time_ms'] = df['funding_time'].astype(int) * 1000 if 'funding_time' in df.columns else 0
        
        # 使用 max_latency_ms_2s 作為凍結持續時間的代理指標
        df['freeze_duration_ms'] = df['max_latency_ms_2s']
        
        # 計算 freeze_start_rel_ms：使用 entry_latency_ms 作為開始時間的估計
        # 如果 entry_latency_ms 小於某個閾值，可能就是凍結開始
        if 'entry_latency_ms' in df.columns:
            df['freeze_start_rel_ms'] = df['entry_latency_ms']
        else:
            df['freeze_start_rel_ms'] = 0
        
        df['freeze_end_rel_ms'] = df['freeze_start_rel_ms'] + df['freeze_duration_ms']
        
        # 標記有效的凍結事件（latency > 閾值）
        freeze_threshold = 50  # ms
        df.loc[df['max_latency_ms_2s'] < freeze_threshold, 'freeze_start_rel_ms'] = np.nan
        df.loc[df['max_latency_ms_2s'] < freeze_threshold, 'freeze_duration_ms'] = np.nan
        
        df_with_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
        print(f"   其中 {len(df_with_freeze)} 筆有顯著延遲記錄 (>{freeze_threshold}ms)")
        df['_format'] = 'sim'
    
    else:
        print(f"❌ 無法識別的 CSV 格式")
        print(f"   可用欄位：{list(df.columns)}")
        return None
    
    return df


def analyze_settlement_timing(df: pd.DataFrame) -> dict:
    """分析真正的結算時間模式"""
    results = {}
    
    # 檢查是模擬數據還是 funding_monitor 數據
    is_sim_format = df['_format'].iloc[0] == 'sim' if '_format' in df.columns else False
    
    # 只分析有凍結數據的記錄
    df_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
    
    if len(df_freeze) == 0:
        print("⚠️ 沒有足夠的凍結事件數據進行分析")
        return results
    
    print("\n" + "="*60)
    print("📊 整體結算時間分析")
    print("="*60)
    
    # 如果是模擬數據，使用 max_latency 相關欄位
    if is_sim_format:
        print("（使用模擬交易數據格式）")
        
        # 使用 max_latency_ms_2s 作為主要指標
        latency_col = 'max_latency_ms_2s' if 'max_latency_ms_2s' in df_freeze.columns else 'freeze_duration_ms'
        max_latencies = df_freeze[latency_col]
        
        results['overall'] = {
            'sample_count': len(df_freeze),
            'max_latency_mean_ms': max_latencies.mean(),
            'max_latency_median_ms': max_latencies.median(),
            'max_latency_std_ms': max_latencies.std(),
            'max_latency_min_ms': max_latencies.min(),
            'max_latency_max_ms': max_latencies.max(),
            'freeze_duration_mean_ms': max_latencies.mean(),
            'freeze_duration_median_ms': max_latencies.median(),
            'freeze_duration_max_ms': max_latencies.max(),
        }
        
        # 如果有 max_latency_ms_post 也分析
        if 'max_latency_ms_post' in df_freeze.columns:
            post_latencies = df_freeze['max_latency_ms_post']
            results['overall']['post_latency_mean_ms'] = post_latencies.mean()
            results['overall']['post_latency_max_ms'] = post_latencies.max()
        
        # 分析 entry_latency（進場延遲）
        if 'entry_latency_ms' in df_freeze.columns:
            entry_lat = df_freeze['entry_latency_ms']
            results['overall']['entry_latency_mean_ms'] = entry_lat.mean()
            results['overall']['entry_latency_max_ms'] = entry_lat.max()
        
        print(f"\n樣本數：{results['overall']['sample_count']} 筆")
        print(f"\n🎯 結算後 2 秒內最大延遲：")
        print(f"   平均值：{results['overall']['max_latency_mean_ms']:.1f} ms")
        print(f"   中位數：{results['overall']['max_latency_median_ms']:.1f} ms")
        print(f"   標準差：{results['overall']['max_latency_std_ms']:.1f} ms")
        print(f"   範圍：{results['overall']['max_latency_min_ms']:.1f} ~ {results['overall']['max_latency_max_ms']:.1f} ms")
        
        if 'post_latency_mean_ms' in results['overall']:
            print(f"\n⏱️ 結算後整體最大延遲：")
            print(f"   平均值：{results['overall']['post_latency_mean_ms']:.1f} ms")
            print(f"   最大值：{results['overall']['post_latency_max_ms']:.1f} ms")
        
        if 'entry_latency_mean_ms' in results['overall']:
            print(f"\n📍 進場時延遲：")
            print(f"   平均值：{results['overall']['entry_latency_mean_ms']:.1f} ms")
            print(f"   最大值：{results['overall']['entry_latency_max_ms']:.1f} ms")
        
        # 分析價格波動
        if 'amp_2s_pct' in df_freeze.columns:
            amp_2s = df_freeze['amp_2s_pct']
            results['overall']['amp_2s_mean_pct'] = amp_2s.mean()
            results['overall']['amp_2s_max_pct'] = amp_2s.max()
            print(f"\n📈 結算後 2 秒內價格振幅：")
            print(f"   平均值：{amp_2s.mean():.3f}%")
            print(f"   最大值：{amp_2s.max():.3f}%")
        
    else:
        # 原始 settlement_stats.csv 格式
        freeze_starts = df_freeze['freeze_start_rel_ms']
        freeze_durations = df_freeze['freeze_duration_ms']
        
        results['overall'] = {
            'sample_count': len(df_freeze),
            'freeze_start_mean_ms': freeze_starts.mean(),
            'freeze_start_median_ms': freeze_starts.median(),
            'freeze_start_std_ms': freeze_starts.std(),
            'freeze_start_min_ms': freeze_starts.min(),
            'freeze_start_max_ms': freeze_starts.max(),
            'freeze_duration_mean_ms': freeze_durations.mean(),
            'freeze_duration_median_ms': freeze_durations.median(),
            'freeze_duration_max_ms': freeze_durations.max(),
        }
        
        print(f"\n樣本數：{results['overall']['sample_count']} 筆")
        print(f"\n🎯 真正結算時間（相對於預期結算時刻）：")
        print(f"   平均值：{results['overall']['freeze_start_mean_ms']:+.1f} ms")
        print(f"   中位數：{results['overall']['freeze_start_median_ms']:+.1f} ms")
        print(f"   標準差：{results['overall']['freeze_start_std_ms']:.1f} ms")
        print(f"   範圍：{results['overall']['freeze_start_min_ms']:+.1f} ~ {results['overall']['freeze_start_max_ms']:+.1f} ms")
        
        print(f"\n⏱️ 凍結持續時間：")
        print(f"   平均值：{results['overall']['freeze_duration_mean_ms']:.1f} ms")
        print(f"   中位數：{results['overall']['freeze_duration_median_ms']:.1f} ms")
        print(f"   最大值：{results['overall']['freeze_duration_max_ms']:.1f} ms")
        
        # 判斷結算時間模式
        mean_offset = results['overall']['freeze_start_mean_ms']
        if mean_offset < -100:
            pattern = "結算通常在預期時間之前發生"
        elif mean_offset > 100:
            pattern = "結算通常在預期時間之後發生"
        else:
            pattern = "結算時間大致符合預期"
        
        print(f"\n📍 結論：{pattern}")
    
    return results


def analyze_by_symbol(df: pd.DataFrame, min_samples: int = 3) -> dict:
    """依 symbol 分析"""
    results = {}
    
    is_sim_format = df['_format'].iloc[0] == 'sim' if '_format' in df.columns else False
    df_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
    
    print("\n" + "="*60)
    print("📊 各 Symbol 結算時間分析")
    print("="*60)
    
    for symbol in df_freeze['symbol'].unique():
        sym_df = df_freeze[df_freeze['symbol'] == symbol]
        
        if len(sym_df) < min_samples:
            print(f"\n⚠️ {symbol}：樣本數不足（{len(sym_df)}/{min_samples}），跳過")
            continue
        
        if is_sim_format:
            latency_col = 'max_latency_ms_2s' if 'max_latency_ms_2s' in sym_df.columns else 'freeze_duration_ms'
            max_latencies = sym_df[latency_col]
            
            results[symbol] = {
                'sample_count': len(sym_df),
                'max_latency_mean_ms': max_latencies.mean(),
                'max_latency_median_ms': max_latencies.median(),
                'max_latency_std_ms': max_latencies.std(),
                'max_latency_max_ms': max_latencies.max(),
                'freeze_duration_mean_ms': max_latencies.mean(),
                'freeze_duration_max_ms': max_latencies.max(),
            }
            
            # 加入價格振幅
            if 'amp_2s_pct' in sym_df.columns:
                results[symbol]['amp_2s_mean_pct'] = sym_df['amp_2s_pct'].mean()
                results[symbol]['amp_2s_max_pct'] = sym_df['amp_2s_pct'].max()
            
            # 加入勝率統計
            if 'exit_type' in sym_df.columns:
                tp_count = (sym_df['exit_type'] == 'TAKE_PROFIT').sum()
                sl_count = (sym_df['exit_type'] == 'STOP_LOSS').sum()
                total_exits = tp_count + sl_count
                win_rate = tp_count / total_exits * 100 if total_exits > 0 else 0
                results[symbol]['win_rate_pct'] = win_rate
                results[symbol]['total_trades'] = total_exits
            
            print(f"\n{'='*40}")
            print(f"📌 {symbol} ({len(sym_df)} 筆)")
            print(f"   最大延遲 (2s): {max_latencies.mean():.1f} ± {max_latencies.std():.1f} ms（最大 {max_latencies.max():.1f} ms）")
            if 'amp_2s_mean_pct' in results[symbol]:
                print(f"   價格振幅 (2s): {results[symbol]['amp_2s_mean_pct']:.3f}%（最大 {results[symbol]['amp_2s_max_pct']:.3f}%）")
            if 'win_rate_pct' in results[symbol]:
                print(f"   勝率: {results[symbol]['win_rate_pct']:.1f}% ({results[symbol]['total_trades']} 筆交易)")
        else:
            freeze_starts = sym_df['freeze_start_rel_ms']
            freeze_durations = sym_df['freeze_duration_ms']
            
            results[symbol] = {
                'sample_count': len(sym_df),
                'freeze_start_mean_ms': freeze_starts.mean(),
                'freeze_start_median_ms': freeze_starts.median(),
                'freeze_start_std_ms': freeze_starts.std(),
                'freeze_duration_mean_ms': freeze_durations.mean(),
                'freeze_duration_max_ms': freeze_durations.max(),
            }
            
            print(f"\n{'='*40}")
            print(f"📌 {symbol} ({len(sym_df)} 筆)")
            print(f"   結算偏移：{freeze_starts.mean():+.1f} ± {freeze_starts.std():.1f} ms")
            print(f"   凍結時間：{freeze_durations.mean():.1f} ms（最大 {freeze_durations.max():.1f} ms）")
    
    return results


def analyze_by_hour(df: pd.DataFrame) -> dict:
    """依結算時段分析（00:00, 08:00, 16:00 UTC）"""
    results = {}
    
    df_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
    df_freeze['hour_utc'] = df_freeze['settlement_time_utc'].dt.hour
    
    print("\n" + "="*60)
    print("📊 依結算時段分析 (UTC)")
    print("="*60)
    
    for hour in sorted(df_freeze['hour_utc'].unique()):
        hour_df = df_freeze[df_freeze['hour_utc'] == hour]
        
        if len(hour_df) < 2:
            continue
        
        freeze_starts = hour_df['freeze_start_rel_ms']
        freeze_durations = hour_df['freeze_duration_ms']
        
        results[hour] = {
            'sample_count': len(hour_df),
            'freeze_start_mean_ms': freeze_starts.mean(),
            'freeze_start_std_ms': freeze_starts.std(),
            'freeze_duration_mean_ms': freeze_durations.mean(),
        }
        
        print(f"\n{hour:02d}:00 UTC ({len(hour_df)} 筆)：")
        print(f"   結算偏移：{freeze_starts.mean():+.1f} ± {freeze_starts.std():.1f} ms")
        print(f"   凍結時間：{freeze_durations.mean():.1f} ms")
    
    return results


def plot_settlement_distribution(df: pd.DataFrame, output_dir: Path):
    """繪製結算時間分佈圖"""
    is_sim_format = df['_format'].iloc[0] == 'sim' if '_format' in df.columns else False
    df_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
    
    if len(df_freeze) < 2:
        print("⚠️ 數據不足，無法繪製分佈圖")
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    if is_sim_format:
        fig.suptitle('Settlement Latency Analysis (Simulation Data)', fontsize=14, fontweight='bold')
        latency_col = 'max_latency_ms_2s' if 'max_latency_ms_2s' in df_freeze.columns else 'freeze_duration_ms'
        
        # 1. 最大延遲分佈
        ax1 = axes[0, 0]
        max_latencies = df_freeze[latency_col]
        ax1.hist(max_latencies, bins=30, edgecolor='black', alpha=0.7)
        ax1.axvline(max_latencies.mean(), color='red', linestyle='--', linewidth=2, 
                    label=f'Mean: {max_latencies.mean():.1f}ms')
        ax1.axvline(max_latencies.median(), color='green', linestyle='--', linewidth=2,
                    label=f'Median: {max_latencies.median():.1f}ms')
        ax1.set_xlabel('Max Latency in 2s Window (ms)')
        ax1.set_ylabel('Count')
        ax1.set_title('Maximum Latency Distribution')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. 價格振幅分佈
        ax2 = axes[0, 1]
        if 'amp_2s_pct' in df_freeze.columns:
            amp = df_freeze['amp_2s_pct']
            ax2.hist(amp, bins=30, edgecolor='black', alpha=0.7, color='orange')
            ax2.axvline(amp.mean(), color='red', linestyle='--', linewidth=2,
                        label=f'Mean: {amp.mean():.3f}%')
            ax2.set_xlabel('Price Amplitude in 2s (%)')
            ax2.set_ylabel('Count')
            ax2.set_title('Price Amplitude Distribution')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        else:
            ax2.text(0.5, 0.5, 'No amplitude data', ha='center', va='center')
        
        # 3. 時間序列：延遲趨勢
        ax3 = axes[1, 0]
        ax3.scatter(df_freeze['settlement_time_utc'], df_freeze[latency_col], 
                    s=30, alpha=0.7)
        ax3.set_xlabel('Settlement Time (UTC)')
        ax3.set_ylabel('Max Latency (ms)')
        ax3.set_title('Latency Over Time')
        ax3.grid(True, alpha=0.3)
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 4. 依 Symbol 的箱形圖
        ax4 = axes[1, 1]
        symbols = df_freeze['symbol'].unique()
        if len(symbols) > 1:
            data_by_symbol = [df_freeze[df_freeze['symbol'] == s][latency_col].values 
                              for s in symbols if len(df_freeze[df_freeze['symbol'] == s]) >= 2]
            labels = [s for s in symbols if len(df_freeze[df_freeze['symbol'] == s]) >= 2]
            if data_by_symbol:
                bp = ax4.boxplot(data_by_symbol, labels=labels, patch_artist=True)
                ax4.set_ylabel('Max Latency (ms)')
                ax4.set_title('Latency by Symbol')
                ax4.grid(True, alpha=0.3)
                plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
        else:
            ax4.text(0.5, 0.5, 'Single symbol only', ha='center', va='center', fontsize=12)
            ax4.set_title('Latency by Symbol')
    
    else:
        fig.suptitle('Settlement Time Analysis', fontsize=14, fontweight='bold')
        
        # 1. 結算偏移分佈直方圖
        ax1 = axes[0, 0]
        freeze_starts = df_freeze['freeze_start_rel_ms']
        ax1.hist(freeze_starts, bins=30, edgecolor='black', alpha=0.7)
        ax1.axvline(freeze_starts.mean(), color='red', linestyle='--', linewidth=2, 
                    label=f'Mean: {freeze_starts.mean():+.1f}ms')
        ax1.axvline(freeze_starts.median(), color='green', linestyle='--', linewidth=2,
                    label=f'Median: {freeze_starts.median():+.1f}ms')
        ax1.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.5, label='Expected (0ms)')
        ax1.set_xlabel('Freeze Start Relative to Expected Settlement (ms)')
        ax1.set_ylabel('Count')
        ax1.set_title('Settlement Timing Distribution')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. 凍結持續時間分佈
        ax2 = axes[0, 1]
        freeze_durations = df_freeze['freeze_duration_ms']
        ax2.hist(freeze_durations, bins=30, edgecolor='black', alpha=0.7, color='orange')
        ax2.axvline(freeze_durations.mean(), color='red', linestyle='--', linewidth=2,
                    label=f'Mean: {freeze_durations.mean():.1f}ms')
        ax2.set_xlabel('Freeze Duration (ms)')
        ax2.set_ylabel('Count')
        ax2.set_title('Freeze Duration Distribution')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # 3. 時間序列：結算偏移趨勢
        ax3 = axes[1, 0]
        ax3.scatter(df_freeze['settlement_time_utc'], df_freeze['freeze_start_rel_ms'], 
                    s=30, alpha=0.7)
        ax3.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
        ax3.set_xlabel('Settlement Time (UTC)')
        ax3.set_ylabel('Freeze Start Offset (ms)')
        ax3.set_title('Settlement Timing Over Time')
        ax3.grid(True, alpha=0.3)
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 4. 依 Symbol 的箱形圖
        ax4 = axes[1, 1]
        symbols = df_freeze['symbol'].unique()
        if len(symbols) > 1:
            data_by_symbol = [df_freeze[df_freeze['symbol'] == s]['freeze_start_rel_ms'].values 
                              for s in symbols if len(df_freeze[df_freeze['symbol'] == s]) >= 2]
            labels = [s for s in symbols if len(df_freeze[df_freeze['symbol'] == s]) >= 2]
            if data_by_symbol:
                bp = ax4.boxplot(data_by_symbol, labels=labels, patch_artist=True)
                ax4.axhline(0, color='black', linestyle='-', linewidth=1, alpha=0.5)
                ax4.set_ylabel('Freeze Start Offset (ms)')
                ax4.set_title('Settlement Timing by Symbol')
                ax4.grid(True, alpha=0.3)
                plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
        else:
            ax4.text(0.5, 0.5, 'Single symbol only', ha='center', va='center', fontsize=12)
            ax4.set_title('Settlement Timing by Symbol')
    
    plt.tight_layout()
    output_path = output_dir / 'settlement_distribution.png'
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"\n📈 已輸出分佈圖：{output_path}")


def plot_detailed_timing(df: pd.DataFrame, output_dir: Path):
    """繪製詳細的結算時間分析圖"""
    is_sim_format = df['_format'].iloc[0] == 'sim' if '_format' in df.columns else False
    df_freeze = df[df['freeze_start_rel_ms'].notna()].copy()
    
    if len(df_freeze) < 2:
        return
    
    if is_sim_format:
        # 模擬數據：繪製延遲 vs 價格振幅的關係圖
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Detailed Latency Analysis (Simulation Data)', fontsize=14, fontweight='bold')
        
        latency_col = 'max_latency_ms_2s' if 'max_latency_ms_2s' in df_freeze.columns else 'freeze_duration_ms'
        
        # 1. 延遲 vs 價格振幅
        ax1 = axes[0, 0]
        if 'amp_2s_pct' in df_freeze.columns:
            scatter = ax1.scatter(df_freeze[latency_col], df_freeze['amp_2s_pct'], 
                                  c=range(len(df_freeze)), cmap='viridis', s=50, alpha=0.7)
            ax1.set_xlabel('Max Latency (ms)')
            ax1.set_ylabel('Price Amplitude (%)')
            ax1.set_title('Latency vs Price Amplitude')
            ax1.grid(True, alpha=0.3)
            plt.colorbar(scatter, ax=ax1, label='Event #')
        
        # 2. 延遲 vs Funding Rate
        ax2 = axes[0, 1]
        if 'lastFundingRate' in df_freeze.columns:
            fr = df_freeze['lastFundingRate'].astype(float) * 100  # 轉為百分比
            ax2.scatter(df_freeze[latency_col], fr, s=50, alpha=0.7, c='orange')
            ax2.set_xlabel('Max Latency (ms)')
            ax2.set_ylabel('Funding Rate (%)')
            ax2.set_title('Latency vs Funding Rate')
            ax2.grid(True, alpha=0.3)
        
        # 3. 每小時延遲分佈
        ax3 = axes[1, 0]
        df_freeze['hour_utc'] = df_freeze['settlement_time_utc'].dt.hour
        hour_groups = df_freeze.groupby('hour_utc')[latency_col]
        hours = sorted(df_freeze['hour_utc'].unique())
        data_by_hour = [hour_groups.get_group(h).values for h in hours if h in hour_groups.groups]
        if data_by_hour:
            ax3.boxplot(data_by_hour, labels=[f"{h:02d}:00" for h in hours])
            ax3.set_xlabel('Hour (UTC)')
            ax3.set_ylabel('Max Latency (ms)')
            ax3.set_title('Latency by Hour')
            ax3.grid(True, alpha=0.3)
        
        # 4. 勝敗與延遲的關係
        ax4 = axes[1, 1]
        if 'exit_type' in df_freeze.columns:
            win_df = df_freeze[df_freeze['exit_type'] == 'TAKE_PROFIT']
            loss_df = df_freeze[df_freeze['exit_type'] == 'STOP_LOSS']
            if len(win_df) > 0 and len(loss_df) > 0:
                data = [win_df[latency_col].values, loss_df[latency_col].values]
                bp = ax4.boxplot(data, labels=['Win (TP)', 'Loss (SL)'], patch_artist=True)
                bp['boxes'][0].set_facecolor('green')
                bp['boxes'][1].set_facecolor('red')
                ax4.set_ylabel('Max Latency (ms)')
                ax4.set_title('Latency: Win vs Loss')
                ax4.grid(True, alpha=0.3)
        
        f_out = output_dir / 'settlement_timing_detail.png'
        plt.tight_layout()
        plt.savefig(f_out, dpi=150)
        plt.close()
        print(f"📈 已輸出詳細分析圖：{f_out}")
        
    else:
        # 原始格式：繪製凍結時間線
        fig, axes = plt.subplots(2, 1, figsize=(16, 10))
        fig.suptitle('Detailed Settlement Timing Analysis', fontsize=14, fontweight='bold')
        
        # 1. 結算前後的時間線
        ax1 = axes[0]
        for i, (_, row) in enumerate(df_freeze.iterrows()):
            symbol = row['symbol']
            start = row['freeze_start_rel_ms']
            end = row['freeze_end_rel_ms']
            
            # 繪製凍結區間
            ax1.barh(i, end - start, left=start, height=0.6, alpha=0.7,
                    label=symbol if symbol not in [l.get_label() for l in ax1.patches] else "")
        
            # 標記開始點
            ax1.scatter([start], [i], color='green', s=50, zorder=5)
            # 標記結束點
            ax1.scatter([end], [i], color='red', s=50, zorder=5)
        
        ax1.axvline(0, color='black', linestyle='--', linewidth=2, label='Expected Settlement')
        ax1.set_xlabel('Time Relative to Expected Settlement (ms)')
        ax1.set_ylabel('Settlement Event #')
        ax1.set_title('Freeze Events Timeline (Green=Start, Red=End)')
        ax1.grid(True, alpha=0.3)
        # 簡化圖例
        handles, labels = ax1.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax1.legend(by_label.values(), by_label.keys(), loc='upper right')
        
        # 2. 凍結開始時間 vs 持續時間的散點圖
        ax2 = axes[1]
        scatter = ax2.scatter(df_freeze['freeze_start_rel_ms'], df_freeze['freeze_duration_ms'],
                              c=range(len(df_freeze)), cmap='viridis', s=80, alpha=0.7)
        ax2.axvline(0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax2.set_xlabel('Freeze Start Time (ms relative to expected settlement)')
        ax2.set_ylabel('Freeze Duration (ms)')
        ax2.set_title('Freeze Start Time vs Duration')
        ax2.grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=ax2, label='Event #')
        
        plt.tight_layout()
        output_path = output_dir / 'settlement_timing_detail.png'
        plt.savefig(output_path, dpi=150)
        plt.close()
        print(f"📈 已輸出詳細分析圖：{output_path}")


def generate_summary_report(df: pd.DataFrame, overall_results: dict, 
                           symbol_results: dict, hour_results: dict,
                           output_dir: Path):
    """生成文字報告"""
    is_sim_format = df['_format'].iloc[0] == 'sim' if '_format' in df.columns else False
    report_path = output_dir / 'settlement_analysis_report.txt'
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("="*70 + "\n")
        if is_sim_format:
            f.write("         Binance Settlement Simulation Analysis Report\n")
        else:
            f.write("         Binance Funding Settlement Time Analysis Report\n")
        f.write(f"         Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write("="*70 + "\n\n")
        
        f.write(f"數據總筆數：{len(df)}\n")
        
        # 根據格式顯示不同的資訊
        if is_sim_format:
            if 'exit_type' in df.columns:
                wins = len(df[df['exit_type'] == 'TAKE_PROFIT'])
                losses = len(df[df['exit_type'] == 'STOP_LOSS'])
                win_rate = wins / len(df) * 100 if len(df) > 0 else 0
                f.write(f"勝利次數：{wins}\n")
                f.write(f"失敗次數：{losses}\n")
                f.write(f"勝率：{win_rate:.1f}%\n")
        else:
            f.write(f"有凍結事件的記錄：{len(df[df['freeze_start_rel_ms'].notna()])}\n")
        
        f.write(f"分析期間：{df['settlement_time_utc'].min()} ~ {df['settlement_time_utc'].max()}\n")
        
        if overall_results:
            f.write("\n" + "-"*50 + "\n")
            if is_sim_format:
                f.write("整體延遲分析\n")
            else:
                f.write("整體結算時間分析\n")
            f.write("-"*50 + "\n\n")
            
            overall = overall_results.get('overall', {})
            f.write(f"樣本數：{overall.get('sample_count', 0)}\n\n")
            
            if is_sim_format:
                # 模擬數據的延遲資訊
                f.write("最大延遲 (結算後2秒內)：\n")
                f.write(f"  平均值：{overall.get('freeze_start_mean_ms', 0):.1f} ms\n")
                f.write(f"  中位數：{overall.get('freeze_start_median_ms', 0):.1f} ms\n")
                f.write(f"  標準差：{overall.get('freeze_start_std_ms', 0):.1f} ms\n")
                f.write(f"  範圍：{overall.get('freeze_start_min_ms', 0):.1f} ~ {overall.get('freeze_start_max_ms', 0):.1f} ms\n\n")
                
                if 'freeze_duration_mean_ms' in overall:
                    f.write("入場延遲：\n")
                    f.write(f"  平均值：{overall.get('freeze_duration_mean_ms', 0):.1f} ms\n")
            else:
                f.write("真正結算時間（相對於預期結算時刻）：\n")
                f.write(f"  平均值：{overall.get('freeze_start_mean_ms', 0):+.1f} ms\n")
                f.write(f"  中位數：{overall.get('freeze_start_median_ms', 0):+.1f} ms\n")
                f.write(f"  標準差：{overall.get('freeze_start_std_ms', 0):.1f} ms\n")
                f.write(f"  範圍：{overall.get('freeze_start_min_ms', 0):+.1f} ~ {overall.get('freeze_start_max_ms', 0):+.1f} ms\n\n")
                
                f.write("凍結持續時間：\n")
                f.write(f"  平均值：{overall.get('freeze_duration_mean_ms', 0):.1f} ms\n")
                f.write(f"  中位數：{overall.get('freeze_duration_median_ms', 0):.1f} ms\n")
                f.write(f"  最大值：{overall.get('freeze_duration_max_ms', 0):.1f} ms\n")
            
            # 結論
            f.write("\n" + "="*50 + "\n")
            f.write("🎯 關鍵發現\n")
            f.write("="*50 + "\n\n")
            
            if is_sim_format:
                avg_latency = overall.get('freeze_start_mean_ms', 0)
                if avg_latency < 100:
                    f.write(f"平均延遲 {avg_latency:.0f}ms 表現良好\n")
                elif avg_latency < 300:
                    f.write(f"平均延遲 {avg_latency:.0f}ms 可接受\n")
                else:
                    f.write(f"平均延遲 {avg_latency:.0f}ms 較高，建議檢查網路連線\n")
            else:
                mean_offset = overall.get('freeze_start_mean_ms', 0)
                if abs(mean_offset) < 50:
                    f.write("結算時間大致符合預期，偏差在 ±50ms 內\n")
                elif mean_offset < -50:
                    f.write(f"結算通常在預期時間之前 {abs(mean_offset):.0f}ms 發生\n")
                    f.write(f"建議：將交易觸發時間提前約 {abs(mean_offset):.0f}ms\n")
                else:
                    f.write(f"結算通常在預期時間之後 {mean_offset:.0f}ms 發生\n")
                    f.write(f"建議：將交易觸發時間延後約 {mean_offset:.0f}ms\n")
                
                freeze_dur = overall.get('freeze_duration_mean_ms', 0)
                f.write(f"\n凍結窗口約 {freeze_dur:.0f}ms，在此期間價格可能無法正常更新\n")
        
        if symbol_results:
            f.write("\n" + "-"*50 + "\n")
            f.write("各 Symbol 分析\n")
            f.write("-"*50 + "\n\n")
            
            for symbol, data in symbol_results.items():
                f.write(f"\n{symbol}：\n")
                f.write(f"  樣本數：{data['sample_count']}\n")
                if is_sim_format:
                    # sim 格式使用 max_latency_* 欄位
                    latency_mean = data.get('max_latency_mean_ms', data.get('freeze_start_mean_ms', 0))
                    latency_std = data.get('max_latency_std_ms', data.get('freeze_start_std_ms', 0))
                    f.write(f"  延遲：{latency_mean:.1f} ± {latency_std:.1f} ms\n")
                    if 'win_rate' in data:
                        f.write(f"  勝率：{data['win_rate']:.1f}%\n")
                else:
                    f.write(f"  結算偏移：{data['freeze_start_mean_ms']:+.1f} ± {data['freeze_start_std_ms']:.1f} ms\n")
                    f.write(f"  凍結時間：{data['freeze_duration_mean_ms']:.1f} ms\n")
        
        if hour_results:
            f.write("\n" + "-"*50 + "\n")
            f.write("依時段分析 (UTC)\n")
            f.write("-"*50 + "\n\n")
            
            for hour, data in sorted(hour_results.items()):
                if is_sim_format:
                    f.write(f"{hour:02d}:00 UTC：延遲 {data['freeze_start_mean_ms']:.1f} ± {data['freeze_start_std_ms']:.1f} ms ")
                    f.write(f"({data['sample_count']} 筆)\n")
                else:
                    f.write(f"{hour:02d}:00 UTC：偏移 {data['freeze_start_mean_ms']:+.1f} ± {data['freeze_start_std_ms']:.1f} ms, ")
                    f.write(f"凍結 {data['freeze_duration_mean_ms']:.1f} ms ({data['sample_count']} 筆)\n")
    
    print(f"📄 已輸出分析報告：{report_path}")


def main():
    print("="*60)
    print("🔍 Binance Funding Settlement Time Analyzer")
    print("="*60)
    
    # 載入數據
    df = load_data(args.stats_file)
    if df is None or len(df) == 0:
        return
    
    # 建立輸出目錄
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 執行分析
    overall_results = analyze_settlement_timing(df)
    symbol_results = analyze_by_symbol(df, args.min_samples)
    hour_results = analyze_by_hour(df)
    
    # 繪製圖表
    print("\n📊 生成分析圖表...")
    plot_settlement_distribution(df, output_dir)
    plot_detailed_timing(df, output_dir)
    
    # 生成報告
    generate_summary_report(df, overall_results, symbol_results, hour_results, output_dir)
    
    print("\n" + "="*60)
    print("✅ 分析完成！")
    print(f"   輸出目錄：{output_dir}")
    print("="*60)


if __name__ == "__main__":
    main()
