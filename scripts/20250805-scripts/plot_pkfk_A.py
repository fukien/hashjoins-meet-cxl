import collections
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.gridspec import GridSpec
import numpy as np
import os
import pandas as pd
import seaborn as sns
import sys

LOG_DIR = f"../../logs/20250805-logs"
FIG_DIR = "../../figs/20250805-figs/"


def round6decimals(value):
	return round(float(value), 6)


def trick_get_runtime_from_file(filepath):
	trick_scale = 1.125
	with open(filepath, "r") as f:
		while True:
			line = f.readline()
			if not line:
				break
			if line.startswith("num_trials"):
				line = line.strip().split("\t")
				memcpy_time = round6decimals(line[-4].split()[-1])
				buildpart_time = round6decimals(line[-3].split()[-1])
				probejoin_time = round6decimals(line[-2].split()[-1])
				if line[6].split()[-1] == "uniform_pkfk_A":
					memcpy_time_A = memcpy_time
					buildpart_time_A = buildpart_time
					probejoin_time_A = probejoin_time
				if line[6].split()[-1] == "uniform_pkfk_B":
					memcpy_time_B = memcpy_time
					buildpart_time_B = buildpart_time
					probejoin_time_B = probejoin_time
				if line[6].split()[-1] == "uniform_pkfk_C":
					memcpy_time_C = memcpy_time
					buildpart_time_C = buildpart_time
					probejoin_time_C = probejoin_time


	return [memcpy_time_A, buildpart_time_A, probejoin_time_A], \
		[memcpy_time_B, buildpart_time_B, probejoin_time_B], \
		[memcpy_time_C, buildpart_time_C, probejoin_time_C]


if __name__ == "__main__":
	if not os.path.exists(FIG_DIR):
		os.makedirs(FIG_DIR)




	default_fontsize = 14

	legend_bars = []
	legend_names = [
		"In-CXL Exec",
		"Full Move to DRAM + DRAM Exec",
		"Full Move to Interleaved + Interleaved Exec",
		"Partial Move to DRAM + DRAM & CXL Exec",
	]

	prefix_list = [
		"aux_dr4_ds4", 
		"aux_dr2_ds2",
		"aux_dr6_ds6",
		"hc",
	]
	prefix_num = len(prefix_list)
	prefix2color = {
		"aux_dr4_ds4": "darkseagreen",
		"aux_dr2_ds2": "slateblue",
		"aux_dr6_ds6": "indianred",
		"hc": "darkkhaki",
	}


	fig = plt.figure(figsize=(6, 3.3))

	x_axis_label_list = ["PHJ", "NPHJ"]
	x_axis = np.arange(len(x_axis_label_list))

	x_data_width = 0.86
	bar_width = x_data_width/prefix_num * 3/4
	x_starting_idx_list = [ 
		-x_data_width/2 + \
		(i+1/2) * x_data_width/prefix_num \
		for i in range(prefix_num) 
	]


	for jdx, prefix in enumerate(prefix_list):
		memcpy_time_list = []
		buildpart_time_list = []
		probejoin_time_list = []
		for kdx, suffix in enumerate(
			[
				"phj_rdx_bc",
				"nphj_sc",
			]
		):

			filepath = f"{LOG_DIR}/{prefix}_{suffix}.log"
			time_list_of_list = trick_get_runtime_from_file(filepath)
			memcpy_time_list.append(time_list_of_list[0][0])
			buildpart_time_list.append(time_list_of_list[0][1])
			probejoin_time_list.append(time_list_of_list[0][2])

		memcpy_time_list = np.array(memcpy_time_list)
		buildpart_time_list = np.array(buildpart_time_list)
		probejoin_time_list = np.array(probejoin_time_list)

		ax = plt.gca()
		memcpy_bar = ax.bar(
			x_axis+x_starting_idx_list[jdx],
			memcpy_time_list,
			width=bar_width, 
			edgecolor="black", 
			color=prefix2color[prefix_list[jdx]],
			label=legend_names[jdx]
		)
		ax.bar(
			x_axis+x_starting_idx_list[jdx],
			buildpart_time_list,
			bottom=memcpy_time_list,
			width=bar_width, 
			edgecolor="black", 
			color=prefix2color[prefix_list[jdx]],
			label=legend_names[jdx],
			hatch="//",
		)
		ax.bar(
			x_axis+x_starting_idx_list[jdx],
			probejoin_time_list,
			bottom=memcpy_time_list+buildpart_time_list,
			width=bar_width, 
			edgecolor="black", 
			color=prefix2color[prefix_list[jdx]],
			label=legend_names[jdx],
			hatch="\\\\",
		)

		legend_bars.append(memcpy_bar)

	# title_y_coord = -0.42
	# ax.set_title(
	# 	r"(a) 16M$\bowtie$256M", 
	# 	fontsize=default_fontsize+2, 
	# 	fontweight="bold", 
	# 	y=title_y_coord
	# )
	# ax.set_ylabel("Elapsed Time (s)", fontsize=default_fontsize)

	ax.grid()
	# ax.axes.get_xaxis().set_visible(False)
	ax.set_xticks(np.arange(0, len(x_axis), 1))
	ax.set_xticklabels(
		x_axis_label_list, 
		fontsize=default_fontsize
	)

	ax.set_ylabel(
		"Elapsed Time (s)", fontsize=default_fontsize
	)


	fig.legend(
		[bar[0] for bar in legend_bars],  # just one patch per bar group
		legend_names, 
		loc="lower center", 
		bbox_to_anchor=(0.5, -0.18),  # center it, and move down
		fancybox=True, shadow=False, frameon=False,
		ncol=1, fontsize=default_fontsize,
		columnspacing=2.5
	)
	fig.subplots_adjust(bottom=0.3)  # make space for legend below


	plt.savefig(
		f"{FIG_DIR}/pkfk_a.eps", 
		bbox_inches="tight", 
		format="eps"
	)
	plt.savefig(
		f"{FIG_DIR}/pkfk_a.png", 
		bbox_inches="tight", 
		format="png"
	)
	plt.close()
