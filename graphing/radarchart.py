# Libraries
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pandas as pd
from math import pi
"""
Help:
    Note that you have to modify the python script itself (a global boolean flag
    `DoGlobal`) to generate two different graphs (one for global data structure
    and another one for per-file data structure).  The script will show a
    preview of the radar chart then save the chart together with legends to a
    pdf file.
"""
# This controls we generate global data structure or per-file data structrue
DoGlobal=True
# Set data
df = pd.DataFrame({
'group': ['Global\nHash Table','HashFS','Level\nHashing','Extent\nTrees','Radix\nTrees'],
'Cache Misses': [5, 4, 3, 1, 2],
'\nMemory\nAccesses': [1, 2, 3, 5, 4],
'Index Size': [4, 5, 3, 1, 2],
'Complexity': [2, 1, 3, 5, 4],
#'Contention\nOpportunity\n': [2, 1, 3, 5, 4] 
})
colors = ['#93c47d', '#a64d79', '#f6b26b', '#73a2ec', '#e06666']



# ------- PART 1: Create background

# number of variable
categories=list(df)[1:]
N = len(categories)

# What will be the angle of each axis in the plot? (we divide the plot / number of variable)
angles = [n / float(N) * 2 * pi + pi/4 for n in range(N)]
angles += angles[:1]

# Initialise the spider plot
ax = plt.subplot(111, polar=True)

# If you want the first axis to be on top:
ax.set_theta_offset(pi / 2)
ax.set_theta_direction(-1)

ax.set_xticks(angles[:-1])
ax.set_xticklabels(categories)
plt.gcf().canvas.draw()
#plt.xticks(angles[:-1], categories)

# Draw ylabels
ax.set_rlabel_position(0)
plt.yticks([],[])
#plt.yticks([10,20,30], ["10","20","30"], color="grey", size=7)
#plt.ylim(0,40)


legend_labels = df.get('group')

# ------- PART 2: Add plots

# Plot each individual = each line of the data
# I don't do a loop, because plotting more than 3 groups makes the chart unreadable

if DoGlobal:
    # Ind1
    values=df.loc[0].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=3, linestyle='solid', label=legend_labels[0],
            color=colors[0])
    ax.fill(angles, values, colors[0], alpha=0.5)

    # Ind2
    values=df.loc[1].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=3, linestyle='solid', label=legend_labels[1],
            color=colors[1])
    ax.fill(angles, values, colors[1], alpha=0.5)
else:
    # Ind3
    #values=df.loc[2].drop('group').values.flatten().tolist()
    #values += values[:1]
    #ax.plot(angles, values, linewidth=3, linestyle='solid', label=legend_labels[2],
    #        color=colors[2])
    #ax.fill(angles, values, colors[2], alpha=0.5)

    # Ind4
    values=df.loc[3].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=3, linestyle='solid', label=legend_labels[3],
            color=colors[3])
    ax.fill(angles, values, colors[3], alpha=0.5)

    # Ind5
    values=df.loc[4].drop('group').values.flatten().tolist()
    values += values[:1]
    ax.plot(angles, values, linewidth=3, linestyle='solid', label=legend_labels[4],
            color=colors[4])
    ax.fill(angles, values, colors[4], alpha=0.5)

# Add legend
legend_elements = [Line2D([0],[0], color='w', marker='s', label=lab,
    markerfacecolor=color, markersize=26)
    for color, lab in zip(colors, legend_labels)]
if DoGlobal:
    legend = plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
            handles=legend_elements[0:2], ncol=2, fontsize=24)
else:
    legend = plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2),
            handles=legend_elements[3:], ncol=3, fontsize=24)

# Draw one axe per variable + add labels
pltlocs, pltlabels = plt.xticks()
labels = []
for loc, label, angle in zip(pltlocs, pltlabels, angles):
    x,y = label.get_position()
    lab = ax.text(x,y, label.get_text(),transform=label.get_transform(),
            ha=label.get_ha(), va=label.get_va(), fontsize=24)
    labelangle = (90-angle/pi*180)%360
    if labelangle > 180 and labelangle < 360:
        labelangle = labelangle+90
    else:
        labelangle = labelangle-90
    print("text: {} angel: {}, degree: {}".format(label.get_text(), angle, labelangle))
    lab.set_rotation(labelangle)
    labels.append(lab)
ax.set_xticklabels([])

if DoGlobal:
    savefilename='global.pdf'
else:
    savefilename='perfile.pdf'
plt.savefig(savefilename, bbox_extra_artists=[legend]+labels, bbox_inches='tight')
ax.spines['polar'].set_linewidth(2)
ax.grid(linewidth=2)
plt.show()
