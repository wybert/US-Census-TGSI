import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import json
import os

# Load configuration
with open('setting.json') as f:
    config = json.load(f)

g = gpd.read_parquet(
    os.path.join(config["workspace"], "data/census_tracts_merged_shifted_geo.parquet")
).to_crs(5070)

mask = g[g["mask_low_coverage"] == 1]
ok   = g[g["mask_low_coverage"] == 0].copy()

# 7 档 log2 阈值（上界列表）
bins = [-2, -1, -0.5, 0.5, 1, 2]
labels = ["≤0.25×","0.25–0.5×","0.5–0.71×","0.71–1.41×","1.41–2×","2–4×",">4×"]

fig, ax = plt.subplots(figsize=(11,7), constrained_layout=True)
mask.plot(color="#D9D9D9", linewidth=0, ax=ax)

ok.plot(column="log2CR",
        cmap="Spectral_r", scheme="UserDefined",
        classification_kwds={"bins": bins},
        linewidth=0, ax=ax, legend=True,
        legend_kwds={
            "title": "log\u2082(CR)\n(−1=0.5×, 0=1×, +1=2×)",
            "labels": labels,
            "frameon": False, "loc": "upper right", "bbox_to_anchor": (1.02, 1.0)
        })

ax.set_axis_off()

# 保留第一个图例，再加第二个"Masked"图例
leg1 = ax.get_legend()  # 已由上面的 plot() 创建
masked_patch = Patch(facecolor="#D9D9D9", edgecolor="none", label="Masked / No data")
leg2 = ax.legend(handles=[masked_patch], loc="lower left", frameon=False)
ax.add_artist(leg1)     # 把原图例加回去

fig.savefig(os.path.join(config["outputs_dir"], "validation/log2CR_userdefined_7class.png"), dpi=300, bbox_inches="tight")
