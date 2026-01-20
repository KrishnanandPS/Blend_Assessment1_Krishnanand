import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

print("="*60)
print("LOADING CLEANED DATA")
print("="*60)

# Load the cleaned parquet file (super fast!)
df = pd.read_parquet('cleaned_trips.parquet')
print(f"✓ Loaded {len(df):,} rows × {len(df.columns)} columns")

print("\n" + "="*60)
print("COMPUTING KEY PERFORMANCE INDICATORS")
print("="*60)

# Compute all KPIs
kpis = {
    'Total Trips': len(df),
    'Total Revenue': df['total_amount'].sum(),
    'Average Trip Distance': df['trip_distance'].mean(),
    'Average Fare': df['fare_amount'].mean(),
    'Average Tip Percentage': df['tip_percentage'].mean(),
    'Average Trip Duration': df['trip_duration'].mean(),
    'Revenue per Mile': df['total_amount'].sum() / df['trip_distance'].sum(),
    'Peak Hour Trips': df[df['is_peak']].shape[0],
    'Peak Hour Percentage': (df[df['is_peak']].shape[0] / len(df)) * 100,
    'Weekend Trips': df[df['is_weekend']].shape[0],
    'Weekend Percentage': (df[df['is_weekend']].shape[0] / len(df)) * 100,
    'Average Passengers': df['passenger_count'].mean()
}

# Print formatted KPIs
print("\n📊 JANUARY 2015 NYC TAXI INSIGHTS")
print("-" * 60)

for key, value in kpis.items():
    if 'Total Revenue' in key or 'Revenue per' in key or 'Fare' in key:
        print(f"{key:.<40} ${value:,.2f}")
    elif 'Total' in key or 'Peak Hour Trips' in key or 'Weekend Trips' in key:
        print(f"{key:.<40} {value:,}")
    elif 'Percentage' in key:
        print(f"{key:.<40} {value:.2f}%")
    elif 'Distance' in key or 'Duration' in key or 'Passengers' in key:
        print(f"{key:.<40} {value:.2f}")
    else:
        print(f"{key:.<40} {value:,.2f}")

print("\n" + "="*60)
print("CREATING VISUALIZATIONS")
print("="*60)

# Set style
sns.set_style('whitegrid')
plt.rcParams['figure.figsize'] = (20, 12)

# Create comprehensive dashboard
fig, axes = plt.subplots(3, 3, figsize=(20, 14))
fig.suptitle('NYC Taxi Analytics Dashboard - January 2015', fontsize=20, fontweight='bold', y=0.995)

# 1. Hourly Demand
hourly_trips = df.groupby('hour').size()
axes[0, 0].bar(hourly_trips.index, hourly_trips.values, color='steelblue', edgecolor='black')
axes[0, 0].set_title('Trip Demand by Hour', fontsize=14, fontweight='bold')
axes[0, 0].set_xlabel('Hour of Day')
axes[0, 0].set_ylabel('Number of Trips')
axes[0, 0].grid(axis='y', alpha=0.3)

# 2. Daily Revenue Trend
daily_revenue = df.groupby('day')['total_amount'].sum() / 1_000_000
axes[0, 1].plot(daily_revenue.index, daily_revenue.values, marker='o', linewidth=2, color='green', markersize=8)
axes[0, 1].set_title('Daily Revenue Trend', fontsize=14, fontweight='bold')
axes[0, 1].set_xlabel('Day of Month')
axes[0, 1].set_ylabel('Revenue ($ Millions)')
axes[0, 1].grid(True, alpha=0.3)

# 3. Fare Distribution
axes[0, 2].hist(df['fare_amount'], bins=50, range=(0, 50), edgecolor='black', color='coral', alpha=0.7)
axes[0, 2].set_title('Fare Amount Distribution', fontsize=14, fontweight='bold')
axes[0, 2].set_xlabel('Fare Amount ($)')
axes[0, 2].set_ylabel('Frequency')
axes[0, 2].axvline(df['fare_amount'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: ${df["fare_amount"].mean():.2f}')
axes[0, 2].legend()

# 4. Distance Distribution
axes[1, 0].hist(df['trip_distance'], bins=50, range=(0, 20), edgecolor='black', color='orange', alpha=0.7)
axes[1, 0].set_title('Trip Distance Distribution', fontsize=14, fontweight='bold')
axes[1, 0].set_xlabel('Distance (miles)')
axes[1, 0].set_ylabel('Frequency')
axes[1, 0].axvline(df['trip_distance'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {df["trip_distance"].mean():.2f} mi')
axes[1, 0].legend()

# 5. Tip Percentage by Hour
tip_by_hour = df.groupby('hour')['tip_percentage'].mean()
axes[1, 1].plot(tip_by_hour.index, tip_by_hour.values, marker='o', linewidth=2, color='purple', markersize=8)
axes[1, 1].set_title('Average Tip % by Hour', fontsize=14, fontweight='bold')
axes[1, 1].set_xlabel('Hour of Day')
axes[1, 1].set_ylabel('Tip Percentage (%)')
axes[1, 1].grid(True, alpha=0.3)

# 6. Revenue by Day of Week
day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
revenue_by_dow = df.groupby('day_of_week')['total_amount'].sum() / 1_000_000
axes[1, 2].bar(range(7), revenue_by_dow.values, color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#F7DC6F', '#BB8FCE'], edgecolor='black')
axes[1, 2].set_xticks(range(7))
axes[1, 2].set_xticklabels(day_names, rotation=45, ha='right')
axes[1, 2].set_title('Revenue by Day of Week', fontsize=14, fontweight='bold')
axes[1, 2].set_ylabel('Revenue ($ Millions)')
axes[1, 2].grid(axis='y', alpha=0.3)

# 7. Passenger Count Distribution
passenger_counts = df['passenger_count'].value_counts().sort_index()
axes[2, 0].bar(passenger_counts.index, passenger_counts.values, color='teal', edgecolor='black')
axes[2, 0].set_title('Passenger Count Distribution', fontsize=14, fontweight='bold')
axes[2, 0].set_xlabel('Number of Passengers')
axes[2, 0].set_ylabel('Number of Trips')
axes[2, 0].grid(axis='y', alpha=0.3)

# 8. Peak vs Off-Peak Comparison
peak_data = [df[~df['is_peak']].shape[0], df[df['is_peak']].shape[0]]
peak_labels = ['Off-Peak', 'Peak Hours\n(7-9 AM, 5-7 PM)']
colors_peak = ['lightblue', 'darkred']
axes[2, 1].bar(peak_labels, peak_data, color=colors_peak, edgecolor='black')
axes[2, 1].set_title('Peak vs Off-Peak Trips', fontsize=14, fontweight='bold')
axes[2, 1].set_ylabel('Number of Trips')
axes[2, 1].grid(axis='y', alpha=0.3)
for i, v in enumerate(peak_data):
    axes[2, 1].text(i, v, f'{v:,}', ha='center', va='bottom', fontweight='bold')

# 9. Demand Heatmap (Day of Week vs Hour)
pivot_table = df.groupby(['day_of_week', 'hour']).size().reset_index(name='trips')
pivot = pivot_table.pivot(index='day_of_week', columns='hour', values='trips')
sns.heatmap(pivot, cmap='YlOrRd', ax=axes[2, 2], cbar_kws={'label': 'Trip Count'}, fmt='g')
axes[2, 2].set_title('Demand Heatmap: Day vs Hour', fontsize=14, fontweight='bold')
axes[2, 2].set_yticklabels(day_names, rotation=0)
axes[2, 2].set_xlabel('Hour of Day')
axes[2, 2].set_ylabel('Day of Week')

plt.tight_layout()
plt.savefig('comprehensive_dashboard.png', dpi=150, bbox_inches='tight')
print("✓ Saved: comprehensive_dashboard.png")

# Also create individual plots for easier viewing
print("\nCreating individual high-resolution plots...")

# Individual Plot: Hourly Demand
fig, ax = plt.subplots(figsize=(12, 6))
hourly_trips.plot(kind='bar', ax=ax, color='steelblue', edgecolor='black')
ax.set_title('NYC Taxi Trip Demand by Hour - January 2015', fontsize=16, fontweight='bold')
ax.set_xlabel('Hour of Day', fontsize=12)
ax.set_ylabel('Number of Trips', fontsize=12)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('hourly_demand.png', dpi=150, bbox_inches='tight')
print("✓ Saved: hourly_demand.png")
plt.close()

# Individual Plot: Demand Heatmap
fig, ax = plt.subplots(figsize=(14, 6))
sns.heatmap(pivot, cmap='YlOrRd', ax=ax, cbar_kws={'label': 'Trip Count'}, fmt='g', annot=False)
ax.set_title('Trip Demand Heatmap: Day of Week vs Hour - January 2015', fontsize=16, fontweight='bold')
ax.set_yticklabels(day_names, rotation=0)
ax.set_xlabel('Hour of Day', fontsize=12)
ax.set_ylabel('Day of Week', fontsize=12)
plt.tight_layout()
plt.savefig('demand_heatmap.png', dpi=150, bbox_inches='tight')
print("✓ Saved: demand_heatmap.png")
plt.close()

print("\n" + "="*60)
print("KPI ANALYSIS COMPLETE!")
print("="*60)
print("\n✅ Generated Files:")
print("  📊 comprehensive_dashboard.png (9-panel dashboard)")
print("  📊 hourly_demand.png")
print("  📊 demand_heatmap.png")
print("\n📌 Key Insights:")
print(f"  • Busiest hour: {hourly_trips.idxmax()}:00 with {hourly_trips.max():,} trips")
print(f"  • Average fare: ${df['fare_amount'].mean():.2f}")
print(f"  • Peak hours represent {(df[df['is_peak']].shape[0] / len(df)) * 100:.1f}% of trips")
print(f"  • Total revenue: ${df['total_amount'].sum():,.2f}")
