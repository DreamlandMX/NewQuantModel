import { jsx as _jsx } from "react/jsx-runtime";
import ReactECharts from "echarts-for-react";
import { formatSignedPercent } from "../lib/formatters";
export function ForecastDistribution({ items }) {
    const topItems = items.slice(0, 12);
    return (_jsx(ReactECharts, { style: { height: 280 }, option: {
            backgroundColor: "transparent",
            tooltip: {
                trigger: "axis",
                valueFormatter: (value) => formatSignedPercent(value, 2)
            },
            grid: { left: 24, right: 16, top: 24, bottom: 24 },
            xAxis: {
                type: "category",
                axisLabel: {
                    color: "#90A4AE",
                    interval: 0,
                    fontSize: 11,
                    formatter: (value) => value.replace(":", "\n")
                },
                data: topItems.map((item) => `${item.symbol}:${item.horizon}`)
            },
            yAxis: {
                type: "value",
                axisLabel: { color: "#90A4AE", formatter: (value) => `${(value * 100).toFixed(1)}%` },
                splitLine: { lineStyle: { color: "rgba(144,164,174,0.15)" } }
            },
            series: [
                {
                    name: "Expected Return",
                    type: "bar",
                    itemStyle: { color: "#33d17a" },
                    data: topItems.map((item) => item.expectedReturn)
                },
                {
                    name: "Median",
                    type: "line",
                    smooth: true,
                    lineStyle: { color: "#4fc3f7" },
                    data: topItems.map((item) => item.q50)
                }
            ]
        } }));
}
