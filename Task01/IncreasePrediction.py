from pyecharts.charts import Line
import pandas as pd
from pyecharts import options as opts


data=pd.read_csv("Data_Spark.csv")
data['实际值']=data['实际值'].round(3)
data['预测值']=data['预测值'].round(3)
list_date=data['日期'].tolist()
list_shijizhi=data['实际值'].tolist()
list_yucezhi=data['预测值'].tolist()


(
    Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
    .add_xaxis(xaxis_data=list_date)
    .add_yaxis(
        series_name="实际值",
        y_axis=list_shijizhi,
        label_opts=opts.LabelOpts(is_show=False),
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="最大值",symbol_size=[150,75]),
                opts.MarkPointItem(type_="min", name="最小值",symbol_size=[150,75]),
                opts.MarkLineItem(type_="average", name="平均值",symbol='arrow',symbol_size=[120,60])
            ]
        ),
    )
    .add_yaxis(
        series_name="预测值",
        y_axis=list_yucezhi,
        label_opts=opts.LabelOpts(is_show=False),
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="最大值",symbol_size=[150,75]),
                opts.MarkPointItem(type_="min", name="最小值",symbol_size=[150,75]),
                opts.MarkLineItem(type_="average", name="平均值",symbol='arrow',symbol_size=[120,60])
            ]
        ),
            # linestyle_opts=

    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="股票涨跌幅预测结果对比折线图", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(name="日期",type_="category", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(name="涨跌幅数值")

    )
    # .set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    .render("LineForSpark.html")
)
