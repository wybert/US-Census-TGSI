import numpy as np
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import os

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

# 1) 读几何 + 指标，并过滤低覆盖
# tracts = gpd.read_parquet(
#     os.path.join(config["workspace"], "data/census_tracts_merged_shifted_geo.parquet")
# )
# 如果你有“已过滤”的表，直接 merge 那个；否则在这里过滤
# g = tracts[tracts["mask_low_coverage"] == 0]
g = tracts.copy()
g["log2CR"] = g["log2CR"].where(g["mask_low_coverage"] == 0, np.nan)
# plt.figure()
# g.hist(column="log2CR",bins = 60)
# plt.tight_layout()
# plt.savefig("log2CR_histogram.png")

# plt.figure()
# g.hist(column="CR",bins = 60)
# plt.tight_layout()
# plt.savefig("CR_histogram.png")

plt.figure()
g.plot(column="log2CR",
        cmap="coolwarm",
        vmin=-19,
        vmax=19,
        legend=True,
        legend_kwds={'shrink': 0.6},
        missing_kwds={"color": "#808080", "label": "Masked (<20 tweets)"}
                          )
plt.title("Log2(CR) by Census Tract")
plt.tight_layout()
plt.savefig(os.path.join(config["outputs_dir"], "validation/log2CR_by_census_tract.png"), dpi=300)

# plt.figure()
# g.plot(column="CR",
#         cmap="managua_r",
#         vmin=-19,
#         vmax=19,
#         legend=True,
#         legend_kwds={'shrink': 0.6},
#         )
# plt.savefig("CR_by_census_tract.png", dpi=300)

# # 2) 投影到 NAD83 / Conus Albers（EPSG:5070）
# g = g.to_crs(5070)  # 更适合全国小比例尺专题图

# # 3) 计算对称色域：用 2% 和 98% 分位数，取绝对值的较大者
# q_lo, q_hi = np.quantile(g["log2CR"].dropna(), [0.02, 0.98])
# m = float(max(abs(q_lo), abs(q_hi)))  # 让色带以 0 为中心对称
# cmap = "RdBu_r"  # ColorBrewer 发散型配色的常见选择（红=高、蓝=低）  :contentReference[oaicite:5]{index=5}

# # 如果你的 GeoPandas 支持 norm 参数，优先用 TwoSlopeNorm 固定中点=0
# norm = colors.TwoSlopeNorm(vmin=-m, vcenter=0.0, vmax=m)  # 0 对应 1×代表性

# # 4) 绘制 log2(CR)
# fig, ax = plt.subplots(figsize=(11, 7), constrained_layout=True)
# g.plot(column="log2CR", cmap=cmap, norm=norm, linewidth=0, ax=ax)
# ax.set_axis_off()

# # 自己构造 colorbar，明确标签
# sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm); sm._A = []
# cbar = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.02)
# cbar.set_label("log2(CR)  (−1 = 0.5×, 0 = 1×, +1 = 2×)")
# plt.tight_layout()
# fig.savefig("log2CR_albers.png", dpi=300, bbox_inches="tight")
# plt.close(fig)

# # 5) 如需画“CR”本身，不建议直接画；改为画 log2(CR) 即可。
# # 若一定要画 CR，可先转换：g["CR_display"] = np.clip(np.log2(g["CR"]), -m, m)
# # 再复用上面的画法（column="CR_display"）。
