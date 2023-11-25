import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.dates as mdates

FONTSIZE = 12
FIGSIZE = (10, 8)
LINEWIDTH = 2
DATE_FORMAT = mdates.DateFormatter('%H:%M')

def plot_series(ax, x, y, label, ylabel, color):
    ax.plot(x, y, color=color, linewidth=2, label=label)
    ax.set_ylabel(ylabel,fontsize=FONTSIZE)
    ax.tick_params(axis='both', which='major', labelsize=FONTSIZE)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(nbins=5))
    ax.xaxis.set_major_formatter(DATE_FORMAT)

def plot_chain(df, elev_select, outputname):
    """
    Make plot of chain data and save
    :param df: Dataframe
    :param elev_select: List of PRNs to plot
    :param outputname: output file name
    """
    plot_data = {
        'Elv': 'Elevation [deg]',
        'S4': 'S4 index',
        '60SecSigma': 'sigma [rad]',
        'TEC0': 'TECu',
        'L1 CNo': 'L1/L2 CNo'
    }

    for prn in elev_select:
        ind_select = df['PRN'] == prn

        plt.figure(figsize=FIGSIZE)
        for i, (col, ylabel) in enumerate(plot_data.items(), start=1):

            ax = plt.subplot(5, 1, i)
            if col == 'L1 CNo':
                plot_series(ax, df.index[ind_select], df[col][ind_select], "L1_cno", ylabel,'k')
                plot_series(ax, df.index[ind_select], df['L2 CNo'][ind_select], "L2_cno", ylabel,'r')
                ax.legend()
            else:
                plot_series(ax, df.index[ind_select], df[col][ind_select], col, ylabel, 'k')
            
            if i == 1:
                ax.set_title(f"{outputname}_PRN{prn}", fontsize=FONTSIZE+5)

        plt.savefig(f"{outputname}_PRN{prn}.png")
        plt.close()
